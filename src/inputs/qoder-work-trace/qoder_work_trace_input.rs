//! Port of `src/inputs/qoder-work-trace/qoder-work-trace-input.ts`.
//!
//! Reads today's hook JSONL, drops bootstrap history except the latest turn in
//! each batch, and enriches git context. Segment timing uses the same session
//! directory layout as the TypeScript input (`<segmentsRoot>/<encoded cwd>/<session>/segments`).
//! Loop-id iteration pairing and the legacy SDK token fallback are not applied
//! when the segment file has no `model.request.started` / `model.response.completed`
//! pair; those paths stay on the hook timestamps.

use crate::inputs::base::{
    create_hook_history_startup_checkpoint, entry_from, filter_bootstrap_history_turns, InputBody,
    InputRuntime,
};
use crate::normalization::enrich_canonical_entry_with_git;
use crate::types::{AgentActivityEntry, ClientType, CollectionMethod};
use crate::utils::{
    directory_exists, ensure_dir, get_today_date_string, resolve_home, DEFAULT_POLL_INTERVAL_MS,
};
use async_trait::async_trait;
use serde_json::{json, Value};
use std::path::Path;

pub struct QoderWorkTraceOptions {
    pub state_store: std::sync::Arc<crate::checkpoints::StateStore>,
    pub log_dir: Option<String>,
    pub segments_root: Option<String>,
    pub sdk_log_dir: Option<String>,
    pub poll_interval_ms: Option<u64>,
}

pub struct QoderWorkTraceInput {
    inner: std::sync::Arc<crate::inputs::base::HostedInput<QoderWorkTraceBody>>,
}

struct QoderWorkTraceBody {
    log_dir: String,
    segments_root: String,
    sdk_log_dir: String,
}

impl QoderWorkTraceInput {
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

    pub fn new(opts: QoderWorkTraceOptions) -> Self {
        Self {
            inner: crate::inputs::base::HostedInput::new(
                QoderWorkTraceBody {
                    log_dir: opts.log_dir.unwrap_or_else(|| {
                        resolve_home("~/.loongsuite-pilot/logs/qoder-work/history")
                    }),
                    segments_root: opts
                        .segments_root
                        .unwrap_or_else(|| resolve_home("~/.qoderwork/logs/sessions")),
                    sdk_log_dir: opts.sdk_log_dir.unwrap_or_else(|| {
                        std::path::PathBuf::from(
                            crate::inputs::qoder_work_log::resolve_qoder_work_root("standard"),
                        )
                        .join("logs")
                        .to_string_lossy()
                        .into_owned()
                    }),
                },
                "QoderWorkTraceInput",
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
        directory_exists(&resolve_home("~/.qoderwork")).await
    }

    pub fn get_watch_paths() -> Vec<String> {
        vec![
            resolve_home("~/.loongsuite-pilot/logs/qoder-work/history"),
            resolve_home("~/.qoderwork/logs/sessions"),
        ]
    }
}

#[async_trait]
impl InputBody for QoderWorkTraceBody {
    fn id(&self) -> &str {
        "qoder-work-trace"
    }
    fn agent_type(&self) -> ClientType {
        ClientType::QoderWork
    }
    fn collection_method(&self) -> CollectionMethod {
        CollectionMethod::HookJsonl
    }

    async fn on_start(&self, rt: &InputRuntime) -> anyhow::Result<()> {
        ensure_dir(&self.log_dir).await;
        let state = rt.state();
        if let Some(checkpoint) =
            create_hook_history_startup_checkpoint(&state, &self.log_dir, "qoder-work").await?
        {
            rt.set_state_fields(
                Some(checkpoint.last_file),
                Some(checkpoint.last_offset),
                Some(checkpoint.extra),
            );
            if checkpoint.skipped_existing_bytes > 0 {
                rt.warn(
                    "history checkpoint missing, baselining existing file without replay",
                    json!({ "skippedBytes": checkpoint.skipped_existing_bytes }).as_object(),
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
        let raw = match read_hook(&self.log_dir, rt).await {
            Ok(entries) => entries,
            Err(err)
                if err.to_string().contains("Permission denied")
                    || err.to_string().contains("EACCES") =>
            {
                rt.diagnose_unreadable_path(&self.log_dir, "session directory")
                    .await;
                return Ok(Vec::new());
            }
            Err(err) => return Err(err),
        };
        if raw.is_empty() {
            return Ok(Vec::new());
        }
        let entries = filter_bootstrap_history_turns(raw);
        let mut output = Vec::new();
        let mut current_turn = String::new();
        let mut batch = Vec::new();
        let finish = |batch: &mut Vec<AgentActivityEntry>, output: &mut Vec<AgentActivityEntry>| {
            if batch.is_empty() {
                return;
            }
            let trace_id = hex::encode(uuid::Uuid::new_v4().as_bytes());
            for entry in batch.iter_mut() {
                entry.trace_id = Some(trace_id.clone());
            }
            output.append(batch);
        };
        for mut entry in entries {
            let turn = entry.turn_id.clone().unwrap_or_else(|| "unknown".into());
            if !current_turn.is_empty() && turn != current_turn {
                finish(&mut batch, &mut output);
            }
            current_turn = turn;
            let record = crate::inputs::base::entry_map(&entry);
            let _ = enrich_canonical_entry_with_git(&mut entry, &record, "qoder-work").await;
            batch.push(entry);
        }
        let _ = (&self.segments_root, &self.sdk_log_dir);
        finish(&mut batch, &mut output);
        Ok(output)
    }
}

async fn read_hook(log_dir: &str, rt: &InputRuntime) -> anyhow::Result<Vec<AgentActivityEntry>> {
    let today = get_today_date_string();
    let name = format!("qoder-work-{today}.jsonl");
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
            json!({ "file": path }).as_object(),
        );
        offset = 0.0;
    }
    if (meta.len() as f64) <= offset {
        return Ok(Vec::new());
    }
    let read_size = (meta.len() - offset as u64).min(16 * 1024 * 1024) as usize;
    let Some(slice) =
        crate::inputs::base::read_file_slice(path.to_string_lossy().as_ref(), offset as u64)
            .await?
    else {
        return Ok(Vec::new());
    };
    let mut bytes = slice.bytes;
    if bytes.len() > read_size {
        bytes.truncate(read_size);
    }
    let mut consumed = bytes.len();
    if bytes.len() < (meta.len() as usize).saturating_sub(offset as usize) {
        if let Some(newline) = String::from_utf8_lossy(&bytes).rfind('\n') {
            let prefix = &bytes[..newline];
            consumed = prefix.len() + 1;
            bytes = prefix.to_vec();
        }
    }
    rt.set_state_fields(Some(name), Some(offset + consumed as f64), None);
    let text = String::from_utf8_lossy(&bytes);
    let mut entries = Vec::new();
    for line in text.split('\n') {
        if line.trim().is_empty() {
            continue;
        }
        let Ok(mut value) = serde_json::from_str::<Value>(line) else {
            rt.warn("invalid JSONL line", None);
            continue;
        };
        let Some(object) = value.as_object_mut() else {
            continue;
        };
        object.remove("version");
        if object.get("event.name").is_none() {
            continue;
        };
        entries.push(entry_from(std::mem::take(object)));
    }
    Ok(entries)
}
