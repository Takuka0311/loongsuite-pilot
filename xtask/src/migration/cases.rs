//! 逐用例核对与目标入口验证；静态映射和实际执行证据分别报告。

use super::{relative_file, Coverage, TestRef};
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::process::Command;

pub(super) const BASELINE: &str = "ef8953de1b5d2aa6d719cb0409cb5f48f2b99b42";

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct SourceCase {
    pub id: String,
    pub title: String,
    pub suites: Vec<String>,
    pub parameters: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct SourceFile {
    pub path: String,
    pub source_hash: String,
    pub cases: Vec<SourceCase>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CaseMapping {
    pub source: SourceCase,
    pub coverage: Coverage,
    pub behavior: String,
    pub platforms: Vec<Platform>,
    pub target: Target,
}

#[derive(Clone, Copy, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "lowercase")]
pub(super) enum Platform {
    Linux,
    Macos,
    Windows,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "lowercase", deny_unknown_fields)]
pub(super) enum Target {
    Rust {
        tests: Vec<TestRef>,
    },
    Retained {
        file: String,
        names: Vec<String>,
        resources: Vec<String>,
        npm_script: String,
    },
    Retired {
        reason: String,
        replacement: Vec<TestRef>,
    },
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct CollectedTest {
    pub file: String,
    pub source_hash: String,
    pub name: String,
    pub platform: Platform,
    pub state: TestState,
}

#[derive(Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub(super) enum TestState {
    Pass,
    Fail,
    Skip,
    Todo,
}

#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct CaseReport {
    pub total: usize,
    pub rust: usize,
    pub retained: usize,
    pub retired: usize,
    pub partial: usize,
    pub pending: Vec<SourceCase>,
    pub retained_uncollected: Vec<String>,
    pub retained_not_passed: Vec<String>,
    pub retained_results: Vec<CollectedTest>,
}

impl CaseReport {
    pub fn mapping_complete(&self) -> bool {
        self.total > 0
            && self.pending.is_empty()
            && self.partial == 0
            && self.retained_uncollected.is_empty()
    }
}

pub(super) fn inventory(root: &Path) -> Result<BTreeMap<String, SourceFile>> {
    let result = Command::new("node")
        .current_dir(root)
        .arg(root.join("scripts/testing/legacy-test-inventory.mjs"))
        .arg(root)
        .arg(BASELINE)
        .output()
        .context("用例清单需要开发用 Node 和 TypeScript；请先 npm ci --ignore-scripts")?;
    ensure!(
        result.status.success(),
        "无法解析固定历史用例：{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let files: Vec<SourceFile> = serde_json::from_slice(&result.stdout)?;
    let mut output = BTreeMap::new();
    for file in files {
        ensure!(!file.cases.is_empty(), "历史文件缺少用例：{}", file.path);
        ensure!(
            file.cases
                .iter()
                .map(|case| &case.id)
                .collect::<BTreeSet<_>>()
                .len()
                == file.cases.len(),
            "历史用例标识重复：{}",
            file.path
        );
        ensure!(
            output.insert(file.path.clone(), file).is_none(),
            "历史文件重复"
        );
    }
    Ok(output)
}

#[derive(Default)]
pub(super) struct RustTestIndex {
    files: BTreeMap<String, Vec<String>>,
}

impl RustTestIndex {
    pub fn check(&mut self, root: &Path, tests: &[TestRef]) -> Result<()> {
        ensure!(!tests.is_empty(), "缺少 Rust 替代测试");
        let mut seen = BTreeSet::new();
        for test in tests {
            ensure!(test.file.ends_with(".rs"), "Rust 测试文件后缀无效");
            ensure!(seen.insert((&test.file, &test.test)), "重复 Rust 测试映射");
            let candidates = match self.files.entry(test.file.clone()) {
                std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::btree_map::Entry::Vacant(entry) => {
                    let source = relative_file(root, &test.file)?;
                    let syntax = syn::parse_file(&source)
                        .with_context(|| format!("Rust 测试源码语法无效：{}", test.file))?;
                    let mut candidates = Vec::new();
                    test_functions(&syntax.items, "", &mut candidates);
                    entry.insert(candidates)
                }
            };
            let count = candidates
                .iter()
                .filter(|name| {
                    **name == test.test
                        || (!test.test.contains("::")
                            && name.rsplit("::").next() == Some(&test.test))
                })
                .count();
            ensure!(
                count == 1,
                "Rust 测试入口不存在、被 ignore 或名称歧义：{}::{}",
                test.file,
                test.test
            );
        }
        Ok(())
    }
}

#[cfg(test)]
fn rust_tests(root: &Path, tests: &[TestRef]) -> Result<()> {
    RustTestIndex::default().check(root, tests)
}

fn test_functions(items: &[syn::Item], prefix: &str, output: &mut Vec<String>) {
    for item in items {
        match item {
            syn::Item::Fn(function) => {
                let is_test = function.attrs.iter().any(|attribute| {
                    let parts: Vec<_> = attribute
                        .path()
                        .segments
                        .iter()
                        .map(|part| part.ident.to_string())
                        .collect();
                    parts == ["test"] || parts == ["tokio", "test"]
                });
                let ignored = function
                    .attrs
                    .iter()
                    .any(|attribute| attribute.path().is_ident("ignore"));
                if is_test && !ignored {
                    output.push(format!("{prefix}{}", function.sig.ident));
                }
            }
            syn::Item::Mod(module) => {
                if let Some((_, items)) = &module.content {
                    test_functions(items, &format!("{prefix}{}::", module.ident), output);
                }
            }
            _ => {}
        }
    }
}

pub(super) fn validate_with_index(
    root: &Path,
    source: &SourceFile,
    source_hash: Option<&str>,
    mappings: &[CaseMapping],
    collected: &[CollectedTest],
    index: &mut RustTestIndex,
) -> Result<CaseReport> {
    if !mappings.is_empty() {
        ensure!(
            source_hash == Some(source.source_hash.as_str()),
            "历史源码摘要不匹配：{}",
            source.path
        );
    }
    let mut report = CaseReport {
        total: source.cases.len(),
        ..CaseReport::default()
    };
    let mut seen = BTreeSet::new();
    for mapping in mappings {
        ensure!(
            seen.insert(&mapping.source.id),
            "历史用例重复映射：{}",
            mapping.source.id
        );
        ensure!(
            source.cases.contains(&mapping.source),
            "历史标题、套件或参数不匹配：{}",
            mapping.source.id
        );
        ensure!(!mapping.behavior.trim().is_empty(), "缺少行为断言对应说明");
        ensure!(!mapping.platforms.is_empty(), "缺少适用平台");
        ensure!(
            mapping.platforms.iter().collect::<BTreeSet<_>>().len() == mapping.platforms.len(),
            "适用平台重复"
        );
        if mapping.coverage == Coverage::Partial {
            report.partial += 1;
        }
        match &mapping.target {
            Target::Rust { tests } => {
                index.check(root, tests)?;
                report.rust += 1;
            }
            Target::Retired {
                reason,
                replacement,
            } => {
                ensure!(
                    !reason.trim().is_empty(),
                    "退役必须说明实现机制变化与原生替代合同"
                );
                index.check(root, replacement)?;
                report.retired += 1;
            }
            Target::Retained {
                file,
                names,
                resources,
                npm_script,
            } => {
                let text = relative_file(root, file)?;
                ensure!(
                    !names.is_empty() && names.iter().all(|name| !name.trim().is_empty()),
                    "缺少保留用例名称"
                );
                ensure!(
                    names.iter().collect::<BTreeSet<_>>().len() == names.len(),
                    "保留用例名称重复"
                );
                ensure!(!resources.is_empty(), "缺少被测宿主资源");
                for resource in resources {
                    super::relative_path(root, resource)?;
                }
                let package: serde_json::Value =
                    serde_json::from_str(&relative_file(root, "package.json")?)?;
                ensure!(
                    package["scripts"][npm_script]
                        .as_str()
                        .is_some_and(|script| !script.trim().is_empty()),
                    "保留测试执行入口不存在"
                );
                let hash = format!("{:x}", Sha256::digest(text.as_bytes()));
                for name in names {
                    let entries: Vec<_> = collected
                        .iter()
                        .filter(|entry| {
                            entry.file == *file
                                && entry.name == *name
                                && entry.source_hash == hash
                                && mapping.platforms.contains(&entry.platform)
                        })
                        .collect();
                    ensure!(
                        entries
                            .iter()
                            .map(|entry| entry.platform)
                            .collect::<BTreeSet<_>>()
                            .len()
                            == entries.len(),
                        "保留测试收集结果重复：{file}::{name}"
                    );
                    if entries.is_empty() {
                        report.retained_uncollected.push(format!("{file}::{name}"));
                    }
                    for entry in entries {
                        if entry.state != TestState::Pass {
                            report.retained_not_passed.push(format!("{file}::{name}"));
                        }
                        report.retained_results.push(entry.clone());
                    }
                }
                report.retained += 1;
            }
        }
    }
    report.pending = source
        .cases
        .iter()
        .filter(|case| !seen.contains(&case.id))
        .cloned()
        .collect();
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn validate(
        root: &Path,
        source: &SourceFile,
        source_hash: Option<&str>,
        mappings: &[CaseMapping],
        collected: &[CollectedTest],
    ) -> Result<CaseReport> {
        validate_with_index(
            root,
            source,
            source_hash,
            mappings,
            collected,
            &mut RustTestIndex::default(),
        )
    }

    fn fixture() -> (tempfile::TempDir, SourceFile, CaseMapping) {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("test.rs"), "#[test] fn replacement() {}\n").unwrap();
        let source: SourceCase = serde_json::from_value(
            json!({"id":"L2:C1", "title":"old", "suites":["suite"], "parameters":[]}),
        )
        .unwrap();
        let file = SourceFile {
            path: "tests/old.test.ts".into(),
            source_hash: "fixed".into(),
            cases: vec![source.clone()],
        };
        let mapping = CaseMapping {
            source,
            coverage: Coverage::Complete,
            behavior: "保留原始断言".into(),
            platforms: vec![Platform::Linux],
            target: Target::Rust {
                tests: vec![TestRef {
                    file: "test.rs".into(),
                    test: "replacement".into(),
                }],
            },
        };
        (root, file, mapping)
    }

    #[test]
    fn omitted_duplicate_changed_and_partial_cases_cannot_complete() {
        let (root, file, mapping) = fixture();
        assert!(validate(
            root.path(),
            &file,
            Some("fixed"),
            std::slice::from_ref(&mapping),
            &[]
        )
        .unwrap()
        .mapping_complete());
        assert!(!validate(root.path(), &file, None, &[], &[])
            .unwrap()
            .mapping_complete());
        assert!(validate(
            root.path(),
            &file,
            Some("wrong"),
            std::slice::from_ref(&mapping),
            &[]
        )
        .is_err());
        assert!(validate(
            root.path(),
            &file,
            Some("fixed"),
            &[mapping.clone(), mapping.clone()],
            &[]
        )
        .is_err());
        for field in ["id", "title", "suites", "parameters"] {
            let mut changed = mapping.clone();
            match field {
                "id" => changed.source.id = "L9:C9".into(),
                "title" => changed.source.title = "invented".into(),
                "suites" => changed.source.suites.push("invented".into()),
                _ => changed.source.parameters.push("[1, 2]".into()),
            }
            assert!(validate(root.path(), &file, Some("fixed"), &[changed], &[]).is_err());
        }
        for (behavior, platforms) in [
            (" ", vec![Platform::Linux]),
            ("原断言", vec![]),
            ("原断言", vec![Platform::Linux, Platform::Linux]),
        ] {
            let mut changed = mapping.clone();
            changed.behavior = behavior.into();
            changed.platforms = platforms;
            assert!(validate(root.path(), &file, Some("fixed"), &[changed], &[]).is_err());
        }
        let mut partial = mapping;
        partial.coverage = Coverage::Partial;
        assert!(
            !validate(root.path(), &file, Some("fixed"), &[partial], &[])
                .unwrap()
                .mapping_complete()
        );
    }

    #[test]
    fn rust_references_require_unambiguous_non_ignored_test_attributes() {
        let root = tempfile::tempdir().unwrap();
        let tests = vec![TestRef {
            file: "test.rs".into(),
            test: "replacement".into(),
        }];
        for source in [
            "// #[test] fn replacement() {}",
            "fn replacement() {}",
            "#[test] #[ignore] fn replacement() {}",
            "mod a { #[test] fn replacement() {} } mod b { #[test] fn replacement() {} }",
            "#[test] fn malformed( {",
        ] {
            std::fs::write(root.path().join("test.rs"), source).unwrap();
            assert!(rust_tests(root.path(), &tests).is_err(), "{source}");
        }
        std::fs::write(
            root.path().join("test.rs"),
            "mod tests { #[tokio::test] async fn replacement() {} }",
        )
        .unwrap();
        rust_tests(root.path(), &tests).unwrap();
        rust_tests(
            root.path(),
            &[TestRef {
                file: "test.rs".into(),
                test: "tests::replacement".into(),
            }],
        )
        .unwrap();
        assert!(rust_tests(
            root.path(),
            &[TestRef {
                file: "../test.rs".into(),
                test: "replacement".into()
            }]
        )
        .is_err());
    }

    #[test]
    fn retained_requires_resources_entrypoint_and_fresh_collection() {
        let (root, file, mut mapping) = fixture();
        std::fs::write(root.path().join("host.test.mjs"), "test('host', () => {});").unwrap();
        std::fs::write(root.path().join("plugin.mjs"), "export default {};").unwrap();
        std::fs::write(
            root.path().join("package.json"),
            r#"{"scripts":{"test:host":"vitest run"}}"#,
        )
        .unwrap();
        mapping.target = Target::Retained {
            file: "host.test.mjs".into(),
            names: vec!["host".into()],
            resources: vec!["plugin.mjs".into()],
            npm_script: "test:host".into(),
        };
        let report = validate(
            root.path(),
            &file,
            Some("fixed"),
            std::slice::from_ref(&mapping),
            &[],
        )
        .unwrap();
        assert_eq!(report.retained_uncollected.len(), 1);
        assert!(!report.mapping_complete());
        let mut collected = vec![CollectedTest {
            file: "host.test.mjs".into(),
            name: "host".into(),
            source_hash: "0".repeat(64),
            platform: Platform::Linux,
            state: TestState::Pass,
        }];
        assert!(!validate(
            root.path(),
            &file,
            Some("fixed"),
            std::slice::from_ref(&mapping),
            &collected
        )
        .unwrap()
        .mapping_complete());
        collected[0].source_hash = format!(
            "{:x}",
            Sha256::digest(std::fs::read(root.path().join("host.test.mjs")).unwrap())
        );
        for state in [
            TestState::Pass,
            TestState::Fail,
            TestState::Skip,
            TestState::Todo,
        ] {
            collected[0].state = state;
            let report = validate(
                root.path(),
                &file,
                Some("fixed"),
                std::slice::from_ref(&mapping),
                &collected,
            )
            .unwrap();
            assert!(report.mapping_complete());
            assert_eq!(
                report.retained_not_passed.len(),
                usize::from(state != TestState::Pass)
            );
            assert_eq!(report.retained_results.len(), 1);
            assert!(report.retained_results[0].state == state);
        }
        let check = |target: &CaseMapping, reports: &[CollectedTest]| {
            validate(
                root.path(),
                &file,
                Some("fixed"),
                std::slice::from_ref(target),
                reports,
            )
        };
        for field in ["file", "name", "platform"] {
            let mut wrong = collected[0].clone();
            match field {
                "file" => wrong.file = "another.test.mjs".into(),
                "name" => wrong.name = "another".into(),
                _ => wrong.platform = Platform::Windows,
            }
            assert!(!check(&mapping, &[wrong]).unwrap().mapping_complete());
        }
        assert!(check(&mapping, &[collected[0].clone(), collected[0].clone()]).is_err());
        let mut multi_platform = mapping.clone();
        multi_platform.platforms.push(Platform::Windows);
        let mut windows = collected[0].clone();
        windows.platform = Platform::Windows;
        let report = check(&multi_platform, &[collected[0].clone(), windows]).unwrap();
        assert!(report.mapping_complete());
        assert_eq!(report.retained_results.len(), 2);
        for (names, resources, script) in [
            (vec![], vec!["plugin.mjs"], "test:host"),
            (vec![" "], vec!["plugin.mjs"], "test:host"),
            (vec!["host", "host"], vec!["plugin.mjs"], "test:host"),
            (vec!["host"], vec![], "test:host"),
            (vec!["host"], vec!["missing.mjs"], "test:host"),
            (vec!["host"], vec!["plugin.mjs"], "missing"),
        ] {
            let mut invalid = mapping.clone();
            invalid.target = Target::Retained {
                file: "host.test.mjs".into(),
                names: names.into_iter().map(str::to_owned).collect(),
                resources: resources.into_iter().map(str::to_owned).collect(),
                npm_script: script.into(),
            };
            assert!(check(&invalid, &collected).is_err());
        }
        std::fs::write(root.path().join("plugin.mjs"), [0, 255, 128]).unwrap();
        assert!(check(&mapping, &collected).unwrap().mapping_complete());
        std::fs::write(
            root.path().join("package.json"),
            r#"{"scripts":{"test:host":" "}}"#,
        )
        .unwrap();
        assert!(check(&mapping, &collected).is_err());
        std::fs::write(
            root.path().join("package.json"),
            r#"{"scripts":{"test:host":"vitest run"}}"#,
        )
        .unwrap();
        std::fs::rename(
            root.path().join("host.test.mjs"),
            root.path().join("moved.mjs"),
        )
        .unwrap();
        assert!(validate(root.path(), &file, Some("fixed"), &[mapping], &collected).is_err());
    }

    #[test]
    fn retirement_requires_reason_and_a_real_replacement_contract() {
        let (root, file, mut mapping) = fixture();
        for (reason, name) in [
            ("", "replacement"),
            (" ", "replacement"),
            ("原生替代", "missing"),
        ] {
            mapping.target = Target::Retired {
                reason: reason.into(),
                replacement: vec![TestRef {
                    file: "test.rs".into(),
                    test: name.into(),
                }],
            };
            assert!(validate(
                root.path(),
                &file,
                Some("fixed"),
                std::slice::from_ref(&mapping),
                &[]
            )
            .is_err());
        }
        mapping.target = Target::Retired {
            reason: "原生替代".into(),
            replacement: vec![],
        };
        assert!(validate(
            root.path(),
            &file,
            Some("fixed"),
            std::slice::from_ref(&mapping),
            &[]
        )
        .is_err());
        mapping.target = Target::Retired {
            reason: "旧运行时机制由原生入口承接".into(),
            replacement: vec![TestRef {
                file: "test.rs".into(),
                test: "replacement".into(),
            }],
        };
        let report = validate(root.path(), &file, Some("fixed"), &[mapping], &[]).unwrap();
        assert!(report.mapping_complete());
        assert_eq!((report.rust, report.retained, report.retired), (0, 0, 1));
    }
}
