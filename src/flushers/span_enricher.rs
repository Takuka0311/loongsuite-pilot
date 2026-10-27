//! Port of `src/flushers/span-enricher.ts`.
//!
//! Patch validation matches the TypeScript runner: synchronous object patches,
//! scalar or homogeneous arrays, and rejection of `__proto__` / `constructor` /
//! `prototype`. Loading `.mjs` plugins cannot run inside this process. Paths
//! are recorded and skipped with the same warning the TypeScript loader emits
//! when a module fails to load. In-process [`SpanEnricher`] implementations
//! can be registered with [`SpanEnricherRunner::from_plugins`].

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use serde_json::{Map, Value};

use super::otlp_json_serializer::ReadableSpan;
use crate::utils::create_logger;

#[derive(Clone, Debug)]
pub struct SpanEnricherContext {
    pub agent_type: String,
    pub service_name: String,
}

pub trait SpanEnricher: Send + Sync {
    fn enrich(
        &self,
        span: &SpanView,
        context: &SpanEnricherContext,
    ) -> Result<Option<Map<String, Value>>, String>;
}

#[derive(Clone, Debug)]
pub struct SpanView {
    pub name: String,
    pub trace_id: String,
    pub span_id: String,
    pub attributes: Map<String, Value>,
}

struct LoadedEnricher {
    path: String,
    plugin: Arc<dyn SpanEnricher>,
    warned: AtomicBool,
}

pub struct SpanEnricherRunner {
    plugins: Vec<LoadedEnricher>,
}

pub const LEGACY_ENRICHER_ERROR: &str = "spanEnrichers/spanEnricherPaths 不再支持外部脚本；请将扩展实现为 Rust SpanEnricher 并加入编译期注册表，确认迁移后手动移除旧配置";

pub fn validate_legacy_enricher_paths(paths: &[String]) -> Result<(), String> {
    if !paths.is_empty() {
        return Err(LEGACY_ENRICHER_ERROR.into());
    }
    Ok(())
}

/// 扩展在此显式注册，发行包不动态加载脚本或本机库。
fn builtin_enrichers() -> Vec<(String, Arc<dyn SpanEnricher>)> {
    Vec::new()
}

impl SpanEnricherRunner {
    pub fn new(paths: &[String]) -> Result<Self, String> {
        validate_legacy_enricher_paths(paths)?;
        Ok(Self::from_plugins(builtin_enrichers()))
    }

    pub fn from_plugins(plugins: Vec<(String, Arc<dyn SpanEnricher>)>) -> Self {
        Self {
            plugins: plugins
                .into_iter()
                .map(|(path, plugin)| LoadedEnricher {
                    path,
                    plugin,
                    warned: AtomicBool::new(false),
                })
                .collect(),
        }
    }

    pub async fn enrich(
        &self,
        spans: Vec<ReadableSpan>,
        context: SpanEnricherContext,
    ) -> Vec<ReadableSpan> {
        spans
            .into_iter()
            .map(|span| self.enrich_one(span, &context))
            .collect()
    }

    fn enrich_one(&self, mut span: ReadableSpan, context: &SpanEnricherContext) -> ReadableSpan {
        for entry in &self.plugins {
            let view = SpanView {
                name: span.name.clone(),
                trace_id: span.trace_id.clone(),
                span_id: span.span_id.clone(),
                attributes: snapshot_attributes(&span.attributes),
            };
            match entry.plugin.enrich(&view, context) {
                Ok(None) => {}
                Ok(Some(patch)) => match apply_patch(&mut span.attributes, &patch) {
                    Ok(()) => {}
                    Err(_) => warn_once(entry),
                },
                Err(_) => warn_once(entry),
            }
        }
        span
    }
}

fn warn_once(entry: &LoadedEnricher) {
    if entry.warned.swap(true, Ordering::SeqCst) {
        return;
    }
    create_logger("span-enricher").warn(
        "Span enricher failed; ignoring its patch (further errors suppressed)",
        Some(&meta(&[("path", Value::String(entry.path.clone()))])),
    );
}

pub fn apply_patch(
    attributes: &mut Map<String, Value>,
    patch: &Map<String, Value>,
) -> Result<(), String> {
    let mut updates = Map::new();
    for (key, value) in patch {
        if key.is_empty()
            || matches!(key.as_str(), "__proto__" | "prototype" | "constructor")
            || !valid_value(value)
        {
            return Err("Invalid attribute".to_string());
        }
        updates.insert(key.clone(), value.clone());
    }
    for (key, value) in updates {
        attributes.insert(key, value);
    }
    Ok(())
}

pub fn valid_value(value: &Value) -> bool {
    fn scalar(value: &Value) -> bool {
        match value {
            Value::String(_) | Value::Bool(_) => true,
            Value::Number(number) => number.as_f64().is_some_and(|number| number.is_finite()),
            _ => false,
        }
    }
    if scalar(value) {
        return true;
    }
    let Value::Array(items) = value else {
        return false;
    };
    let values: Vec<&Value> = items.iter().filter(|item| !item.is_null()).collect();
    if values.is_empty() {
        return true;
    }
    let first = type_tag(values[0]);
    values
        .iter()
        .all(|item| scalar(item) && type_tag(item) == first)
}

fn type_tag(value: &Value) -> &'static str {
    match value {
        Value::String(_) => "string",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        _ => "other",
    }
}

fn snapshot_attributes(attributes: &Map<String, Value>) -> Map<String, Value> {
    let mut copy = Map::new();
    for (key, value) in attributes {
        copy.insert(
            key.clone(),
            if let Value::Array(items) = value {
                Value::Array(items.clone())
            } else {
                value.clone()
            },
        );
    }
    copy
}

fn meta(pairs: &[(&str, Value)]) -> Map<String, Value> {
    pairs
        .iter()
        .map(|(key, value)| ((*key).to_string(), value.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::super::otlp_json_serializer::ReadableSpan;
    use super::*;
    use serde_json::json;
    use std::sync::Arc;

    struct Plugin {
        patch: Value,
        fail: bool,
    }

    impl SpanEnricher for Plugin {
        fn enrich(
            &self,
            span: &SpanView,
            context: &SpanEnricherContext,
        ) -> Result<Option<Map<String, Value>>, String> {
            if self.fail {
                return Err("secret".to_string());
            }
            let mut patch = match &self.patch {
                Value::Object(map) => map.clone(),
                _ => return Err("invalid".to_string()),
            };
            if patch.get("use-view").is_some() {
                let tool = span
                    .attributes
                    .get("gen_ai.tool.name")
                    .and_then(|value| value.as_str())
                    .unwrap_or("");
                patch.insert(
                    "gen_ai.skill.name".to_string(),
                    Value::String(format!("{tool}:{}", context.agent_type)),
                );
                patch.remove("use-view");
            }
            Ok(Some(patch))
        }
    }

    fn span() -> ReadableSpan {
        let mut span = ReadableSpan::minimal("execute_tool shell", "aa", "bb");
        span.attributes.insert(
            "gen_ai.tool.name".to_string(),
            Value::String("shell".to_string()),
        );
        span.attributes
            .insert("tags".to_string(), json!(["original"]));
        span
    }

    #[tokio::test]
    async fn applies_plugins_in_order_without_mutating_the_input_snapshot() {
        let original = span();
        let runner = SpanEnricherRunner::from_plugins(vec![
            (
                "first".to_string(),
                Arc::new(Plugin {
                    patch: json!({"use-view": true, "count": 1}),
                    fail: false,
                }),
            ),
            (
                "second".to_string(),
                Arc::new(Plugin {
                    patch: json!({"gen_ai.skill.name": "shell:codex:next"}),
                    fail: false,
                }),
            ),
        ]);
        let context = SpanEnricherContext {
            agent_type: "codex".to_string(),
            service_name: "test-codex".to_string(),
        };
        let enriched = runner.enrich(vec![original.clone()], context).await;
        assert_eq!(
            enriched[0].attributes["gen_ai.skill.name"],
            "shell:codex:next"
        );
        assert_eq!(enriched[0].attributes["count"], 1);
        assert!(original.attributes.get("gen_ai.skill.name").is_none());
        assert_eq!(enriched[0].trace_id, original.trace_id);
        assert_eq!(enriched[0].ended, original.ended);
    }

    #[tokio::test]
    async fn invalid_patches_are_ignored_and_later_plugins_still_run() {
        let cases = [
            json!(null),
            json!([]),
            json!({"good": true, "bad": {}}),
            json!({"bad": [1, "x"]}),
            json!({"__proto__": "bad"}),
        ];
        for patch in cases {
            let runner = SpanEnricherRunner::from_plugins(vec![(
                "invalid".to_string(),
                Arc::new(Plugin { patch, fail: false }),
            )]);
            let original = span();
            let enriched = runner
                .enrich(
                    vec![original.clone()],
                    SpanEnricherContext {
                        agent_type: "codex".to_string(),
                        service_name: "svc".to_string(),
                    },
                )
                .await;
            assert_eq!(enriched[0].attributes, original.attributes);
        }

        let runner = SpanEnricherRunner::from_plugins(vec![
            (
                "throw".to_string(),
                Arc::new(Plugin {
                    patch: json!({}),
                    fail: true,
                }),
            ),
            (
                "good".to_string(),
                Arc::new(Plugin {
                    patch: json!({"ok": true}),
                    fail: false,
                }),
            ),
        ]);
        let enriched = runner
            .enrich(
                vec![span()],
                SpanEnricherContext {
                    agent_type: "codex".to_string(),
                    service_name: "svc".to_string(),
                },
            )
            .await;
        assert_eq!(enriched[0].attributes["ok"], true);
        assert_eq!(enriched[0].attributes["tags"], json!(["original"]));
    }

    #[test]
    fn accepts_scalars_and_homogeneous_arrays() {
        let mut attributes = Map::new();
        let patch = json!({"s": "x", "n": 1, "b": false, "a": [1, null, 2], "empty": []})
            .as_object()
            .unwrap()
            .clone();
        apply_patch(&mut attributes, &patch).unwrap();
        assert_eq!(attributes["s"], "x");
        assert_eq!(attributes["a"], json!([1, null, 2]));
        assert!(!valid_value(&serde_json::Value::Null));
    }

    #[test]
    fn script_paths_require_explicit_migration() {
        assert!(SpanEnricherRunner::new(&[]).is_ok());
        for path in ["/tmp/plugin.mjs", "/tmp/nope.js", " "] {
            let error = SpanEnricherRunner::new(&[path.into()]).err().unwrap();
            assert_eq!(error, LEGACY_ENRICHER_ERROR);
        }
    }
}
