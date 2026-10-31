//! Port of `src/inputs/qoder-trace/qoder-trace-input.ts`.
//!
//! Hook JSONL is the primary source. CLI turns take intercept usage and, when
//! usage or the request/response interval is missing, session segments. IDE
//! turns take SQLite usage, with one shared retry when a response is still
//! unmatched. Multimodal upload is invoked only when a `path_to_uri` callback
//! is supplied; the processor itself lives outside this inputs port.

use super::{
    collect_cli_expected_request_ids, enrich_cli_from_segments, enrich_cli_primary,
    enrich_ide_turn, inject_trace_id, is_idea_db_path, needs_cli_segment_fallback,
    read_intercept_data, read_segment_tokens_for_session, read_sqlite_tokens_for_session,
    resolve_qoder_app_root, QODER_SEGMENTS_ROOT,
};
use crate::inputs::base::{
    build_canonical_hook_entry, create_hook_history_startup_checkpoint,
    filter_bootstrap_history_turns, CanonicalHookRecordOptions, InputBody, InputRuntime,
};
use crate::normalization::enrich_canonical_entry_with_git;
use crate::types::{AgentActivityEntry, ClientType, CollectionMethod, MultimodalUploadMode};
use crate::utils::{
    directory_exists, ensure_dir, get_today_date_string, resolve_home, DEFAULT_POLL_INTERVAL_MS,
};
use async_trait::async_trait;
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub struct QoderTraceOptions {
    pub state_store: Arc<crate::checkpoints::StateStore>,
    pub log_dir: Option<String>,
    pub poll_interval_ms: Option<u64>,
    pub multimodal_enabled: bool,
    pub upload_mode: Option<MultimodalUploadMode>,
    pub allowed_root_paths: Vec<String>,
}

pub struct QoderTraceInput {
    pub inner: Arc<crate::inputs::base::HostedInput<QoderTraceBody>>,
}

pub struct QoderTraceBody {
    log_dir: String,
    stopped: AtomicBool,
    allowed_roots: Vec<String>,
}

impl QoderTraceInput {
    pub(crate) fn collector_runtime_info(&self) -> crate::inputs::base::InputRuntimeInfo {
        self.inner.runtime.runtime_info()
    }

    /// Collector subscription. Forwards to the hosted runtime without exposing its body type.
    pub fn bind_collector(
        &self,
        on_entries: impl Fn(&[crate::types::AgentActivityEntry]) + Send + Sync + 'static,
        on_delta: impl Fn(&crate::inputs::base::InputRuntimeDelta) + Send + Sync + 'static,
    ) {
        self.inner.on_entries(on_entries);
        self.inner.on_runtime_delta(on_delta);
    }

    pub fn collector_running(&self) -> bool {
        self.inner.running()
    }

    pub fn new(opts: QoderTraceOptions) -> Self {
        let enabled = opts.multimodal_enabled;
        Self {
            inner: crate::inputs::base::HostedInput::new(
                QoderTraceBody {
                    log_dir: opts
                        .log_dir
                        .unwrap_or_else(|| resolve_home("~/.loongsuite-pilot/logs/qoder/history")),
                    stopped: AtomicBool::new(false),
                    allowed_roots: if enabled {
                        resolve_qoder_allowed_root_paths(&opts.allowed_root_paths)
                    } else {
                        Vec::new()
                    },
                },
                "QoderTraceInput",
                opts.state_store,
                opts.poll_interval_ms.or(Some(DEFAULT_POLL_INTERVAL_MS)),
            ),
        }
    }

    pub async fn start(&self) -> Result<(), String> {
        self.inner.start().await
    }
    pub async fn stop(&self) -> Result<(), String> {
        self.inner.stop().await
    }
    pub async fn check_availability() -> bool {
        directory_exists(&resolve_home("~/.qoder")).await
    }
    pub fn get_watch_paths() -> Vec<String> {
        vec![
            resolve_home("~/.loongsuite-pilot/logs/qoder/history"),
            resolve_home(QODER_SEGMENTS_ROOT),
        ]
    }
}

pub fn qoder_default_allowed_root_paths() -> Vec<String> {
    let app_root = resolve_qoder_app_root();
    let mut roots = vec![
        resolve_home("~/.qoder/tmp"),
        resolve_home("~/.qoder/vibe_images"),
        Path::new(&app_root)
            .join("SharedClientCache")
            .join("cache")
            .join("images")
            .to_string_lossy()
            .into_owned(),
    ];
    if let Ok(entries) = std::fs::read_dir(&app_root) {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name == "SharedClientCache" {
                continue;
            }
            roots.push(
                Path::new(&app_root)
                    .join(name)
                    .join("SharedClientCache")
                    .join("cache")
                    .join("images")
                    .to_string_lossy()
                    .into_owned(),
            );
        }
    }
    roots
}

pub fn resolve_qoder_allowed_root_paths(user_paths: &[String]) -> Vec<String> {
    let mut merged = Vec::new();
    for raw in qoder_default_allowed_root_paths()
        .into_iter()
        .chain(user_paths.iter().cloned())
    {
        let trimmed = raw.trim();
        if trimmed.is_empty() || merged.iter().any(|existing| existing == trimmed) {
            continue;
        }
        merged.push(trimmed.to_owned());
    }
    merged
}

#[async_trait]
impl InputBody for QoderTraceBody {
    fn checkpoint_namespaces(&self) -> Option<Vec<String>> {
        Some(vec![self.id().to_owned()])
    }

    fn id(&self) -> &str {
        "qoder-trace"
    }
    fn agent_type(&self) -> ClientType {
        ClientType::QoderCli
    }
    fn collection_method(&self) -> CollectionMethod {
        CollectionMethod::HookJsonl
    }
    fn before_stop(&self) {
        self.stopped.store(true, Ordering::SeqCst);
    }

    async fn on_start(&self, rt: &InputRuntime) -> anyhow::Result<()> {
        self.stopped.store(false, Ordering::SeqCst);
        ensure_dir(&self.log_dir).await;
        let state = rt.state();
        if let Some(checkpoint) =
            create_hook_history_startup_checkpoint(&state, &self.log_dir, "qoder").await?
        {
            rt.set_state_fields(
                Some(checkpoint.last_file),
                Some(checkpoint.last_offset),
                Some(checkpoint.extra),
            );
            if checkpoint.skipped_existing_bytes > 0 {
                rt.warn(
                    "history checkpoint missing, baselining existing file without replay",
                    serde_json::json!({ "skippedBytes": checkpoint.skipped_existing_bytes })
                        .as_object(),
                );
            } else {
                rt.info(
                    "history checkpoint initialized before first hook record",
                    None,
                );
            }
        }
        Ok(())
    }

    async fn collect(&self, rt: &InputRuntime) -> anyhow::Result<Vec<AgentActivityEntry>> {
        let mut raw = read_hook_jsonl(&self.log_dir, rt).await?;
        if raw.is_empty() {
            return Ok(Vec::new());
        }
        let groups = group_by_turn(&raw);
        let mut intercept = None;
        let mut ide_sessions: HashMap<String, Vec<usize>> = HashMap::new();
        let mut cli_fallback: HashMap<String, Vec<Vec<usize>>> = HashMap::new();
        for indexes in groups.values() {
            let variant = infer_variant(&raw, indexes);
            let session = extract_session(&raw, indexes);
            if variant == "qoder-cli" {
                if let Some(session) = session {
                    if intercept.is_none() {
                        intercept = Some(read_intercept_data(None, None).await);
                    }
                    let data = intercept.as_ref().unwrap();
                    let mut slice: Vec<AgentActivityEntry> =
                        indexes.iter().map(|index| raw[*index].clone()).collect();
                    enrich_cli_primary(
                        &mut slice,
                        data.system_prompt
                            .as_ref()
                            .map(|prompt| prompt.content.as_str()),
                        &data.tokens,
                    );
                    for (pos, index) in indexes.iter().enumerate() {
                        raw[*index] = slice[pos].clone();
                    }
                    if needs_cli_segment_fallback(&slice) {
                        cli_fallback
                            .entry(session)
                            .or_default()
                            .push(indexes.clone());
                    }
                }
            } else if variant == "qoder" || variant == "qoder-idea" {
                if let Some(session) = session {
                    ide_sessions
                        .entry(session)
                        .or_default()
                        .extend(indexes.iter().copied());
                }
            }
        }
        for (session, turns) in &cli_fallback {
            let expected = turns
                .iter()
                .flat_map(|indexes| {
                    let slice: Vec<AgentActivityEntry> =
                        indexes.iter().map(|index| raw[*index].clone()).collect();
                    collect_cli_expected_request_ids(&slice)
                })
                .collect::<Vec<_>>();
            let segments =
                read_segment_tokens_for_session(session, &expected, QODER_SEGMENTS_ROOT).await;
            for indexes in turns {
                let mut slice: Vec<AgentActivityEntry> =
                    indexes.iter().map(|index| raw[*index].clone()).collect();
                enrich_cli_from_segments(&mut slice, &segments);
                for (pos, index) in indexes.iter().enumerate() {
                    raw[*index] = slice[pos].clone();
                }
            }
        }
        for indexes in ide_sessions.values() {
            let session = raw[indexes[0]].session_id.clone();
            let mut result = read_sqlite_tokens_for_session(&session).await;
            let mut probe: Vec<AgentActivityEntry> =
                indexes.iter().map(|index| raw[*index].clone()).collect();
            enrich_ide_turn(&mut probe, &result.rows);
            if needs_ide_retry(&probe) && !self.stopped.load(Ordering::SeqCst) {
                tokio::time::sleep(std::time::Duration::from_millis(1_000)).await;
                if !self.stopped.load(Ordering::SeqCst) {
                    let retry = read_sqlite_tokens_for_session(&session).await;
                    if retry.rows.len() >= result.rows.len() {
                        result = retry;
                        probe = indexes.iter().map(|index| raw[*index].clone()).collect();
                        enrich_ide_turn(&mut probe, &result.rows);
                    }
                }
            }
            let relabel = indexes
                .iter()
                .all(|index| raw[*index].agent_type == ClientType::Qoder.as_str())
                && is_idea_db_path(result.matched_db_path.as_deref());
            for (pos, index) in indexes.iter().enumerate() {
                raw[*index] = probe[pos].clone();
                if relabel {
                    raw[*index].agent_type = ClientType::QoderIdea.as_str().to_owned();
                }
            }
        }
        rt.with_workspace_policy(|policy| policy.preflight_entries(&raw, ClientType::QoderCli));
        for entry in &mut raw {
            entry.extra.remove("agent.qoder.attachments");
        }
        for indexes in group_by_turn(&raw).values() {
            let mut slice: Vec<AgentActivityEntry> =
                indexes.iter().map(|index| raw[*index].clone()).collect();
            inject_trace_id(&mut slice);
            for (pos, index) in indexes.iter().enumerate() {
                raw[*index] = slice[pos].clone();
            }
        }
        let _ = &self.allowed_roots;
        Ok(raw)
    }
}

fn needs_ide_retry(entries: &[AgentActivityEntry]) -> bool {
    entries.iter().any(|entry| {
        if !entry
            .event_name
            .map(|name| name.as_str() == "llm.response")
            .unwrap_or(false)
        {
            return false;
        }
        let positive =
            entry.input_tokens.is_some_and(|n| n > 0) || entry.output_tokens.is_some_and(|n| n > 0);
        entry.response_id.as_ref().is_none_or(|id| id.is_empty()) || !positive
    })
}

async fn read_hook_jsonl(
    log_dir: &str,
    rt: &InputRuntime,
) -> anyhow::Result<Vec<AgentActivityEntry>> {
    let today = get_today_date_string();
    let name = format!("qoder-{today}.jsonl");
    let path = Path::new(log_dir).join(&name);
    let Ok(meta) = tokio::fs::metadata(&path).await else {
        return Ok(Vec::new());
    };
    let state = rt.state();
    let mut offset = if state.last_file() == Some(name.as_str()) {
        state.last_offset().unwrap_or(0.0)
    } else {
        0.0
    };
    if offset > 0.0 && (meta.len() as f64) < offset {
        rt.info(
            "file truncated, resetting offset",
            serde_json::json!({ "file": path, "recorded": offset, "actual": meta.len() })
                .as_object(),
        );
        offset = 0.0;
    }
    if (meta.len() as f64) <= offset {
        return Ok(Vec::new());
    }
    rt.with_accumulator(|acc| acc.observe_backlog(meta.len().saturating_sub(offset as u64)));
    let Some(slice) =
        crate::inputs::base::read_file_slice(path.to_string_lossy().as_ref(), offset as u64)
            .await?
    else {
        return Ok(Vec::new());
    };
    let Some(newline) = slice.bytes.iter().rposition(|byte| *byte == b'\n') else {
        return Ok(Vec::new());
    };
    let complete_len = newline + 1;
    rt.set_state_fields(Some(name), Some(offset + complete_len as f64), None);
    let text = String::from_utf8_lossy(&slice.bytes[..complete_len]);
    let mut entries = Vec::new();
    for line in text.split('\n') {
        if line.trim().is_empty() {
            continue;
        }
        let Ok(record) = serde_json::from_str::<Value>(line) else {
            rt.warn("invalid JSONL line", None);
            continue;
        };
        let Some(record) = record.as_object() else {
            continue;
        };
        if let Some(entry) = transform_record(record).await {
            entries.push(entry);
        }
    }
    Ok(filter_bootstrap_history_turns(entries))
}

async fn transform_record(record: &Map<String, Value>) -> Option<AgentActivityEntry> {
    let mut entry = build_canonical_hook_entry(
        record,
        ClientType::QoderCli.as_str(),
        None,
        CanonicalHookRecordOptions {
            preserve_safe_custom_top_level_fields: true,
        },
    )?;
    let _ = enrich_canonical_entry_with_git(&mut entry, record, "qoder").await;
    Some(entry)
}

fn group_by_turn(entries: &[AgentActivityEntry]) -> HashMap<String, Vec<usize>> {
    let mut groups = HashMap::new();
    for (index, entry) in entries.iter().enumerate() {
        let turn = entry
            .turn_id
            .clone()
            .filter(|id| !id.is_empty())
            .unwrap_or_else(|| "unknown".into());
        groups.entry(turn).or_insert_with(Vec::new).push(index);
    }
    groups
}

fn infer_variant(entries: &[AgentActivityEntry], indexes: &[usize]) -> &'static str {
    for index in indexes {
        match entries[*index].agent_type.as_str() {
            "qoder-cli" => return "qoder-cli",
            "qoder-idea" => return "qoder-idea",
            "qoder" => return "qoder",
            _ => {}
        }
    }
    "qoder-cli"
}

fn extract_session(entries: &[AgentActivityEntry], indexes: &[usize]) -> Option<String> {
    indexes
        .iter()
        .map(|index| entries[*index].session_id.clone())
        .find(|id| !id.is_empty())
}
