//! 根据固定 Git 基线列出全部旧测试；未映射或部分承接的测试不能通过清理门禁。

use anyhow::{bail, ensure, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::process::Command;

mod cases;
use cases::{CaseMapping, CaseReport, CollectedTest, SourceFile, BASELINE};

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
    #[serde(default)]
    source_hash: Option<String>,
    #[serde(default)]
    cases: Vec<CaseMapping>,
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
    cases: BTreeMap<String, CaseReport>,
}

pub fn audit(
    root: &Path,
    output: &Path,
    require_complete: bool,
    retained_report: Option<&Path>,
) -> Result<()> {
    let manifest: Manifest = serde_json::from_slice(&fs::read(
        root.join("tests/fixtures/rust-migration-map.json"),
    )?)?;
    ensure!(manifest.schema == 2, "未知迁移映射 schema");
    ensure!(manifest.baseline_commit == BASELINE, "历史测试基线不可变更");
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
    let inventory = cases::inventory(root)?;
    ensure!(
        inventory.keys().cloned().collect::<BTreeSet<_>>() == legacy,
        "用例解析清单与 Git 文件清单不一致"
    );
    let collected: Vec<CollectedTest> = match retained_report {
        Some(path) => serde_json::from_slice(&fs::read(root.join(path))?)?,
        None => Vec::new(),
    };
    let report = validate(root, manifest, &legacy, &inventory, &collected)?;
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
    eprintln!(
        "历史用例声明 {}：Rust {}，原语言保留 {}，退役 {}，部分 {}，未映射 {}。参数化声明不等于展开后的测试数量。",
        report.cases.values().map(|item| item.total).sum::<usize>(),
        report.cases.values().map(|item| item.rust).sum::<usize>(),
        report.cases.values().map(|item| item.retained).sum::<usize>(),
        report.cases.values().map(|item| item.retired).sum::<usize>(),
        report.cases.values().map(|item| item.partial).sum::<usize>(),
        report.cases.values().map(|item| item.pending.len()).sum::<usize>()
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
    fs::read_to_string(relative_path(root, path)?)
        .with_context(|| format!("无法读取映射文件：{path}"))
}

fn relative_path(root: &Path, path: &str) -> Result<PathBuf> {
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
    ensure!(resolved.is_file(), "映射目标必须是文件：{path}");
    Ok(resolved)
}

fn validate(
    root: &Path,
    manifest: Manifest,
    legacy: &BTreeSet<String>,
    inventory: &BTreeMap<String, SourceFile>,
    collected: &[CollectedTest],
) -> Result<Report> {
    ensure!(!legacy.is_empty(), "历史测试清单为空，拒绝通过");
    ensure!(
        manifest.schema == 2 && manifest.baseline_commit == BASELINE,
        "历史映射格式或基线无效"
    );
    let mut mappings = BTreeMap::new();
    let mut case_reports = BTreeMap::new();
    let mut test_index = cases::RustTestIndex::default();
    for mapping in manifest.mappings {
        ensure!(
            legacy.contains(&mapping.legacy),
            "历史测试不在固定基线中：{}",
            mapping.legacy
        );
        ensure!(
            (!mapping.rust.is_empty() || !mapping.cases.is_empty())
                && !mapping.note.trim().is_empty(),
            "缺少替代测试或映射说明"
        );
        if !mapping.rust.is_empty() {
            test_index.check(root, &mapping.rust)?;
        }
        let source = inventory.get(&mapping.legacy).context("缺少历史用例清单")?;
        case_reports.insert(
            mapping.legacy.clone(),
            cases::validate_with_index(
                root,
                source,
                mapping.source_hash.as_deref(),
                &mapping.cases,
                collected,
                &mut test_index,
            )?,
        );
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
    for path in &pending {
        let source = inventory
            .get(path)
            .context("缺少未映射文件的历史用例清单")?;
        case_reports.insert(
            path.clone(),
            cases::validate_with_index(root, source, None, &[], collected, &mut test_index)?,
        );
    }
    let complete = mappings
        .values()
        .filter(|mapping| {
            mapping.coverage == Coverage::Complete
                && case_reports[&mapping.legacy].mapping_complete()
        })
        .count();
    let partial = mappings.len() - complete;
    Ok(Report {
        schema: 2,
        baseline_commit: manifest.baseline_commit,
        source_tests: legacy.len(),
        complete,
        partial,
        mapping_complete: pending.is_empty() && partial == 0,
        pending,
        mappings: mappings.into_values().collect(),
        cases: case_reports,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest() -> Manifest {
        serde_json::from_value(serde_json::json!({
            "schema":2, "baselineCommit":BASELINE,
            "mappings":[{"legacy":"tests/a.test.ts", "coverage":"complete", "rust":[{"file":"test.rs", "test":"replacement"}], "note":"替代断言", "source_hash":"fixed", "cases":[{
                "source":{"id":"L1:C1","title":"old","suites":[],"parameters":[]},
                "coverage":"complete", "behavior":"旧断言", "platforms":["linux"],
                "target":{"kind":"rust","tests":[{"file":"test.rs","test":"replacement"}]}
            }]}]
        })).unwrap()
    }

    fn validate_fixture(
        root: &Path,
        manifest: Manifest,
        legacy: &BTreeSet<String>,
    ) -> Result<Report> {
        let inventory = legacy
            .iter()
            .map(|path| {
                (
                    path.clone(),
                    SourceFile {
                        path: path.clone(),
                        source_hash: "fixed".into(),
                        cases: vec![cases::SourceCase {
                            id: "L1:C1".into(),
                            title: "old".into(),
                            suites: vec![],
                            parameters: vec![],
                        }],
                    },
                )
            })
            .collect();
        validate(root, manifest, legacy, &inventory, &[])
    }

    #[test]
    fn rejects_missing_duplicate_and_empty_history() {
        let validate = validate_fixture;
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
    fn baseline_notes_and_case_level_coverage_cannot_be_bypassed() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join("test.rs"), "#[test] fn replacement() {}\n").unwrap();
        let legacy = BTreeSet::from(["tests/a.test.ts".into()]);
        let mut changed = manifest();
        changed.baseline_commit = "0".repeat(40);
        assert!(validate_fixture(root.path(), changed, &legacy).is_err());
        let mut empty_note = manifest();
        empty_note.mappings[0].note = "  ".into();
        assert!(validate_fixture(root.path(), empty_note, &legacy).is_err());
        let mut file_only = manifest();
        file_only.mappings[0].cases.clear();
        let report = validate_fixture(root.path(), file_only, &legacy).unwrap();
        assert_eq!(report.complete, 0);
        assert_eq!(report.partial, 1);
        assert_eq!(report.cases["tests/a.test.ts"].pending.len(), 1);
        assert!(!report.mapping_complete);
        for path in ["../test.rs", "/test.rs", "dir\\test.rs", ""] {
            assert!(relative_path(root.path(), path).is_err());
        }
    }

    #[test]
    fn missing_git_baseline_fails_before_creating_a_report() {
        let root = tempfile::tempdir().unwrap();
        assert!(Command::new("git")
            .args(["init", "--quiet"])
            .current_dir(root.path())
            .status()
            .unwrap()
            .success());
        fs::create_dir_all(root.path().join("tests/fixtures")).unwrap();
        fs::write(
            root.path().join("tests/fixtures/rust-migration-map.json"),
            serde_json::to_vec(&serde_json::json!({
                "schema":2,"baselineCommit":BASELINE,"mappings":[]
            }))
            .unwrap(),
        )
        .unwrap();
        let error = audit(root.path(), Path::new("report"), true, None).unwrap_err();
        assert!(error.to_string().contains("Git 基线不可用"), "{error}");
        assert!(!root.path().join("report").exists());
    }

    #[cfg(unix)]
    #[test]
    fn mapping_symlinks_cannot_escape_the_repository() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let resource = outside.path().join("resource.mjs");
        fs::write(&resource, "export default {};").unwrap();
        std::os::unix::fs::symlink(resource, root.path().join("resource.mjs")).unwrap();
        assert!(relative_path(root.path(), "resource.mjs").is_err());
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
