//! 根据固定 Git 基线列出全部旧测试；未映射或部分承接的测试不能通过清理门禁。

use anyhow::{bail, ensure, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Component, Path};
use std::process::Command;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Manifest {
    schema: u32,
    baseline_commit: String,
    mappings: Vec<Mapping>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Mapping {
    legacy: String,
    coverage: Coverage,
    rust: Vec<TestRef>,
    note: String,
}

#[derive(Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum Coverage {
    Complete,
    Partial,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct TestRef {
    file: String,
    test: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Report {
    schema: u32,
    baseline_commit: String,
    source_tests: usize,
    complete: usize,
    partial: usize,
    pending: Vec<String>,
    mappings: Vec<Mapping>,
    mapping_complete: bool,
}

pub fn audit(root: &Path, output: &Path, require_complete: bool) -> Result<()> {
    let manifest: Manifest = serde_json::from_slice(&fs::read(
        root.join("tests/fixtures/rust-migration-map.json"),
    )?)?;
    ensure!(manifest.schema == 1, "未知迁移映射 schema");
    ensure!(
        manifest.baseline_commit.len() == 40
            && manifest
                .baseline_commit
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit()),
        "基线必须为完整 commit SHA"
    );
    let listing = Command::new("git")
        .current_dir(root)
        .args([
            "ls-tree",
            "-rz",
            "--name-only",
            &manifest.baseline_commit,
            "--",
            "tests",
        ])
        .output()
        .context("无法读取历史测试基线")?;
    ensure!(
        listing.status.success(),
        "Git 基线不可用，不能把空清单当作已完成"
    );
    let paths = String::from_utf8(listing.stdout)?;
    let legacy: BTreeSet<String> = paths
        .split('\0')
        .filter(|path| is_legacy_test(path))
        .map(str::to_owned)
        .collect();
    let report = validate(root, manifest, &legacy)?;
    let output = root.join(output);
    fs::create_dir_all(&output)?;
    fs::write(
        output.join("report.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    eprintln!(
        "历史测试文件 {}：完整映射 {}，部分映射 {}，未映射 {}。映射检查不等于测试通过。",
        report.source_tests,
        report.complete,
        report.partial,
        report.pending.len()
    );
    if require_complete && !report.mapping_complete {
        bail!(
            "旧测试尚未完整承接，禁止清理；详见 {}",
            output.join("report.json").display()
        );
    }
    Ok(())
}

fn is_legacy_test(path: &str) -> bool {
    let name = path.rsplit('/').next().unwrap_or("");
    let script = [
        ".ts", ".tsx", ".js", ".mjs", ".cjs", ".py", ".sh", ".ps1", ".swift",
    ]
    .iter()
    .any(|suffix| name.ends_with(suffix));
    script
        && (name.contains(".test.")
            || name.contains(".spec.")
            || name.starts_with("test_")
            || name.starts_with("test-")
            || name.contains("Tests.swift"))
}

fn relative_file(root: &Path, path: &str) -> Result<String> {
    let candidate = Path::new(path);
    ensure!(
        !candidate.is_absolute()
            && !path.contains('\\')
            && candidate
                .components()
                .all(|part| matches!(part, Component::Normal(_))),
        "映射路径必须为仓库相对路径"
    );
    let resolved = fs::canonicalize(root.join(candidate))
        .with_context(|| format!("映射文件不存在：{path}"))?;
    ensure!(
        resolved.starts_with(fs::canonicalize(root)?),
        "映射路径越出仓库"
    );
    fs::read_to_string(resolved).with_context(|| format!("无法读取映射文件：{path}"))
}

fn validate(root: &Path, manifest: Manifest, legacy: &BTreeSet<String>) -> Result<Report> {
    ensure!(!legacy.is_empty(), "历史测试清单为空，拒绝通过");
    let mut mappings = BTreeMap::new();
    for mapping in manifest.mappings {
        ensure!(
            legacy.contains(&mapping.legacy),
            "历史测试不在固定基线中：{}",
            mapping.legacy
        );
        ensure!(
            !mapping.rust.is_empty() && !mapping.note.trim().is_empty(),
            "缺少替代测试或映射说明"
        );
        let mut tests = BTreeSet::new();
        for test in &mapping.rust {
            ensure!(
                test.file.ends_with(".rs")
                    && !test.test.is_empty()
                    && test
                        .test
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'),
                "Rust 测试引用无效"
            );
            ensure!(tests.insert((&test.file, &test.test)), "重复 Rust 测试映射");
            let source = relative_file(root, &test.file)?;
            ensure!(
                source.contains(&format!("fn {}(", test.test)),
                "Rust 测试入口不存在：{}::{}",
                test.file,
                test.test
            );
        }
        ensure!(
            !mappings.contains_key(&mapping.legacy),
            "历史测试重复映射：{}",
            mapping.legacy
        );
        mappings.insert(mapping.legacy.clone(), mapping);
    }
    let pending: Vec<_> = legacy
        .iter()
        .filter(|path| !mappings.contains_key(*path))
        .cloned()
        .collect();
    let complete = mappings
        .values()
        .filter(|mapping| mapping.coverage == Coverage::Complete)
        .count();
    let partial = mappings.len() - complete;
    Ok(Report {
        schema: 1,
        baseline_commit: manifest.baseline_commit,
        source_tests: legacy.len(),
        complete,
        partial,
        mapping_complete: pending.is_empty() && partial == 0,
        pending,
        mappings: mappings.into_values().collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest() -> Manifest {
        serde_json::from_value(serde_json::json!({
            "schema":1, "baselineCommit":"ef8953de1b5d2aa6d719cb0409cb5f48f2b99b42",
            "mappings":[{"legacy":"tests/a.test.ts", "coverage":"complete", "rust":[{"file":"test.rs", "test":"replacement"}], "note":"替代断言"}]
        })).unwrap()
    }

    #[test]
    fn rejects_missing_duplicate_and_empty_history() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join("test.rs"), "#[test] fn replacement() {}\n").unwrap();
        let legacy = BTreeSet::from(["tests/a.test.ts".into()]);
        assert!(
            validate(root.path(), manifest(), &legacy)
                .unwrap()
                .mapping_complete
        );
        assert!(validate(root.path(), manifest(), &BTreeSet::new()).is_err());
        let mut duplicate = manifest();
        duplicate.mappings.push(duplicate.mappings[0].clone());
        assert!(validate(root.path(), duplicate, &legacy).is_err());
        let mut missing = manifest();
        missing.mappings[0].rust[0].test = "missing".into();
        assert!(validate(root.path(), missing, &legacy).is_err());
        let mut partial = manifest();
        partial.mappings[0].coverage = Coverage::Partial;
        assert!(
            !validate(root.path(), partial, &legacy)
                .unwrap()
                .mapping_complete
        );
        let extra = BTreeSet::from(["tests/a.test.ts".into(), "tests/b.test.mjs".into()]);
        let report = validate(root.path(), manifest(), &extra).unwrap();
        assert!(!report.mapping_complete);
        assert_eq!(report.pending, ["tests/b.test.mjs"]);
    }

    #[test]
    fn inventory_distinguishes_tests_from_helpers_and_fixtures() {
        assert!(is_legacy_test("tests/unit/a.test.mjs"));
        assert!(is_legacy_test("tests/integration/a.test.ts"));
        assert!(is_legacy_test("tests/test_example.py"));
        assert!(!is_legacy_test("tests/unit/fixture.mjs"));
        assert!(!is_legacy_test("tests/unit/fixtures/events.jsonl"));
    }
}
