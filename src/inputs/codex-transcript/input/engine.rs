//! Scan, checkpoint, and subagent fusion for [`super::CodexTranscriptBody`].
//! Port of the instance methods in `codex-transcript-input.ts`.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::LazyLock;

use regex::Regex;
use serde_json::{json, Map, Value};

use crate::checkpoints::InputStatePatch;
use crate::inputs::base::{CommittedInputBatch, InputRuntime};
use crate::multimodal::types::{BlobToUriParams, UriResult};
use crate::normalization::{build_agent_activity_entry, is_reserved_key};
use crate::types::{AgentActivityEntry, AgentEventName};

use super::super::builder::{
    build_codex_transcript_segment, next_input_messages_for_step, CodexTranscriptBuildOptions,
};
use super::super::checkpoint::{
    checkpoint_to_value, create_active_turn, empty_checkpoint, has_emitted_active_turn_state,
    merge_pending_fusion_children, new_pending_terminal, parse_checkpoint,
    update_active_turn_from_extracted, update_active_turn_metadata,
};
use super::super::discovery::{
    canonical_directory_path, collect_recent_rollout_files, collect_rollout_files,
    read_codex_wakeup_roots, safe_wakeup_session_part, span_context_marker_name,
    DiscoveredSessionFile, WalkBudget, DYNAMIC_SESSION_FILE_MAX_IDLE_MS,
    MAX_DYNAMIC_DIRECTORIES_PER_ROOT, MAX_DYNAMIC_ROLLOUT_FILES_PER_ROOT, MAX_DYNAMIC_SESSION_DIRS,
    MAX_DYNAMIC_SESSION_FILES, MAX_SPAN_CONTEXTS_PER_CLEANUP, MAX_WAKEUP_MARKERS_FOR_DISCOVERY,
    MAX_WAKEUP_MARKERS_PER_CLEANUP, WAKEUP_MARKER_DISCOVERY_TTL_MS,
};
use super::super::extractor::{
    extract_codex_partial_turn, extract_codex_partial_turn_with_boundaries,
    extract_codex_transcript_meta, session_id_from_transcript_path, ExtractOptions,
};
use super::super::scan::{
    self, file_meta_snapshot, find_owner_session_meta_offset, find_turn_start_offset,
    last_complete_jsonl_offset, read_json_line_at, read_json_lines, scan_json_lines,
    select_owner_session_meta_offset, terminal_turn_id_for, turn_id_for_start,
    uuid_v7_timestamp_ms, JsonLine,
};
use super::super::subagent_linker::{
    extract_codex_spawn_descriptors, CodexSpawnDescriptor, CodexSubagentLinkConfidence,
    MAX_LINK_DESCRIPTORS,
};
use super::super::types::{
    CodexActiveTranscriptTurn, CodexForkBootstrap, CodexPendingFusionChild, CodexPendingFusionTurn,
    CodexPendingSubagentTurn, CodexTerminalStatus, CodexThreadSource, CodexTranscriptCheckpoint,
    CodexTranscriptInputContext, CodexTranscriptMeta, CodexTranscriptSourceRecord,
    ForkBootstrapState, FusionConfidence,
};
use super::super::utils::{self, as_record, json_bytes, now_ms, string_value};
use super::{CachedMeta, CodexTranscriptBody, RolloutStat};

pub(super) const MAX_EMIT_BATCH_ENTRIES: usize = 256;
pub(super) const MAX_EMIT_BATCH_BYTES: usize = 1024 * 1024;
const MAX_PERSISTED_INPUT_CONTEXT_BYTES: usize = 1024 * 1024;
const MAX_TERMINALS_PER_FILE_CYCLE: usize = 100;
const MAX_SCAN_BYTES_PER_FILE_CYCLE: i64 = 16 * 1024 * 1024;
const MAX_CACHED_TRANSCRIPT_META_BYTES: usize = 64 * 1024 * 1024;
const CACHED_TRANSCRIPT_META_ABSENCE_TTL_MS: i64 = 48 * 60 * 60 * 1_000;
const SPAN_CONTEXT_TTL_MS: i64 = 48 * 60 * 60 * 1_000;
const SPAN_CONTEXT_CLEANUP_INTERVAL_MS: i64 = 5 * 60 * 1_000;
const WAKEUP_MARKER_CLEANUP_INTERVAL_MS: i64 = 5 * 60 * 1_000;
const MAX_WAKEUP_RESOURCE_ATTRIBUTE_VALUE_LENGTH: usize = 512;
const MAX_SPAN_ATTRIBUTE_VALUE_LENGTH: usize = 512;
const WAKEUP_RESOURCE_ATTRIBUTE_KEYS: &[&str] =
    &["agentteams.worker.name", "agentteams.instance.id"];

static SENSITIVE_SPAN_ATTRIBUTE_NAME_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)(^|[_.-])(TOKEN|SECRET|PASSWORD|CREDENTIAL|COOKIE)([_.-]|$)|^(API_KEY|API_HEADER)$",
    )
    .expect("sensitive span attribute")
});

pub(super) struct CodexTranscriptEngine<'a> {
    pub body: &'a CodexTranscriptBody,
}

enum RecoveryKind {
    Unparseable,
    ProcessedEmpty,
    ProcessedEmitted,
}

struct Diagnostics {
    source_record_count: usize,
}

struct Recovery {
    kind: RecoveryKind,
    entries: Vec<AgentActivityEntry>,
    consumed_end_offset: i64,
    diagnostics: Diagnostics,
}

struct RecoveryBoundary {
    end_offset: i64,
    terminal: bool,
    inferred: Option<CodexTerminalStatus>,
}

struct PendingRecovery {
    blocked: bool,
    emitted_count: usize,
    processed_terminal_count: usize,
}

pub(super) fn has_copied_history_prefix(meta: Option<&CodexTranscriptMeta>) -> bool {
    meta.is_some_and(|meta| {
        meta.forked_from_id.is_some()
            || meta.parent_thread_id.is_some()
            || meta.thread_source == CodexThreadSource::Subagent
    })
}

impl<'a> CodexTranscriptEngine<'a> {
    pub fn discard_legacy_global_turn_registry(&self, rt: &InputRuntime) {
        let current = rt.state_store.get("codex-transcript");
        let Some(mut extra) = current.extra() else {
            return;
        };
        let Some(object) = extra.as_object_mut() else {
            return;
        };
        if object.remove("codexTranscriptGlobal").is_none() {
            return;
        }
        rt.state_store
            .set("codex-transcript", current.with_extra(extra));
    }

    pub fn state_key(&self, file_path: &str) -> String {
        format!("codex-transcript:{file_path}")
    }

    pub fn read_checkpoint(
        &self,
        rt: &InputRuntime,
        key: &str,
    ) -> Option<CodexTranscriptCheckpoint> {
        let extra = rt.state_store.get(key).extra()?;
        parse_checkpoint(extra.get("codexTranscript")?)
    }

    pub fn save_checkpoint(
        &self,
        rt: &InputRuntime,
        key: &str,
        checkpoint: &CodexTranscriptCheckpoint,
    ) {
        rt.state_store.update(
            key,
            InputStatePatch {
                last_offset: Some(checkpoint.scan_offset as f64),
                extra: Some(json!({ "codexTranscript": checkpoint_to_value(checkpoint) })),
                ..InputStatePatch::default()
            },
        );
    }

    pub async fn discover_session_files(&self) -> Vec<DiscoveredSessionFile> {
        let canonical_default = canonical_directory_path(&self.body.session_dir).await;
        let default_files = self.list_default_rollout_files().await;
        let mut discovered: IndexMap<String, DiscoveredSessionFile> = IndexMap::new();
        for file_path in default_files {
            let relative = path_relative(&self.body.session_dir, &file_path);
            let canonical_path = if relative.is_empty() {
                canonical_default.clone()
            } else {
                Path::new(&canonical_default)
                    .join(relative)
                    .to_string_lossy()
                    .into_owned()
            };
            discovered.insert(
                canonical_path,
                DiscoveredSessionFile {
                    file_path,
                    baseline_on_start: true,
                },
            );
        }
        let mut remaining = MAX_DYNAMIC_SESSION_FILES;
        for session_dir in self.discover_dynamic_session_dirs(&canonical_default).await {
            if remaining == 0 {
                break;
            }
            let mut files = Vec::new();
            let mut budget = WalkBudget {
                remaining_directories: MAX_DYNAMIC_DIRECTORIES_PER_ROOT,
                remaining_rollout_files: MAX_DYNAMIC_ROLLOUT_FILES_PER_ROOT,
            };
            collect_recent_rollout_files(
                &session_dir,
                &mut files,
                (now_ms() as i64) - DYNAMIC_SESSION_FILE_MAX_IDLE_MS,
                remaining,
                &mut budget,
                0,
            )
            .await;
            for file_path in files {
                if discovered.contains_key(&file_path) {
                    continue;
                }
                discovered.insert(
                    file_path.clone(),
                    DiscoveredSessionFile {
                        file_path,
                        baseline_on_start: false,
                    },
                );
                remaining -= 1;
                if remaining == 0 {
                    break;
                }
            }
        }
        let mut files: Vec<DiscoveredSessionFile> = discovered.into_values().collect();
        files.sort_by(|left, right| left.file_path.cmp(&right.file_path));
        files
    }

    async fn list_default_rollout_files(&self) -> Vec<String> {
        let cached = self.body.with_shared(|shared| {
            shared
                .cached_default
                .as_ref()
                .map(|cached| (cached.dir_mtimes.clone(), cached.files.clone()))
        });
        if let Some((dir_mtimes, files)) = cached {
            if rollout_dirs_unchanged(&dir_mtimes).await {
                return files;
            }
        }
        let mut files = Vec::new();
        let mut dir_mtimes = Vec::new();
        collect_rollout_files(&self.body.session_dir, &mut files, &mut dir_mtimes).await;
        self.body.with_shared(|shared| {
            shared.cached_default = if dir_mtimes.is_empty() {
                None
            } else {
                Some(super::CachedDefault {
                    dir_mtimes,
                    files: files.clone(),
                })
            };
        });
        files
    }

    pub fn historical_skip_paths(
        &self,
        files: &[DiscoveredSessionFile],
        now: i64,
    ) -> HashSet<String> {
        let refresh = self.body.with_shared(|shared| {
            if now - shared.last_historical_stat_at >= super::HISTORICAL_ROLLOUT_RECHECK_MS {
                shared.last_historical_stat_at = now;
                true
            } else {
                false
            }
        });
        if refresh {
            return HashSet::new();
        }
        let mut skip = HashSet::new();
        self.body.with_shared(|shared| {
            for file in files {
                if !file.baseline_on_start {
                    continue;
                }
                let Some(snap) = shared.idle.get(&file.file_path) else {
                    continue;
                };
                if now - snap.mtime_ms < DYNAMIC_SESSION_FILE_MAX_IDLE_MS {
                    continue;
                }
                skip.insert(file.file_path.clone());
            }
        });
        skip
    }

    fn note_rollout_idle(&self, rt: &InputRuntime, file_path: &str, stat: Option<RolloutStat>) {
        let Some(stat) = stat else {
            self.body.with_shared(|shared| {
                shared.idle.shift_remove(file_path);
            });
            return;
        };
        let caught_up = self
            .read_checkpoint(rt, &self.state_key(file_path))
            .is_some_and(|checkpoint| rollout_caught_up(&checkpoint, stat.ino, stat.size));
        self.body.with_shared(|shared| {
            if caught_up {
                shared.idle.insert(file_path.to_owned(), stat);
            } else {
                shared.idle.shift_remove(file_path);
            }
        });
    }

    pub async fn process_file(&self, rt: &InputRuntime, file_path: &str) -> anyhow::Result<usize> {
        let stat = match tokio::fs::metadata(file_path).await {
            Ok(meta) => {
                let (ino, size, mtime_ms) = file_meta_snapshot(&meta);
                Some(RolloutStat {
                    ino,
                    size,
                    mtime_ms,
                })
            }
            Err(_) => None,
        };
        let result = match stat {
            Some(stat) => self.scan_rollout_file(rt, file_path, stat).await,
            None => Ok(0),
        };
        self.note_rollout_idle(rt, file_path, stat);
        result
    }

    async fn scan_rollout_file(
        &self,
        rt: &InputRuntime,
        file_path: &str,
        stat: RolloutStat,
    ) -> anyhow::Result<usize> {
        let key = self.state_key(file_path);
        let indexed = self
            .body
            .with_shared(|shared| shared.meta_by_path.get(file_path).cloned());
        let has_indexed_copied = has_copied_history_prefix(indexed.as_ref());
        let mut checkpoint = self.read_checkpoint(rt, &key);
        let mut changed = false;
        if checkpoint.is_none() {
            checkpoint = Some(empty_checkpoint(stat.ino));
            changed = true;
        }
        let mut checkpoint = checkpoint.expect("checkpoint");
        if checkpoint.inode != stat.ino {
            let bootstrap = checkpoint.fork_bootstrap.clone();
            if has_indexed_copied
                && bootstrap
                    .as_ref()
                    .is_some_and(|item| item.state == ForkBootstrapState::LivePending)
            {
                let meta = json!({
                    "transcriptPath": file_path,
                    "previousInode": checkpoint.inode,
                    "currentInode": stat.ino,
                    "previousScanOffset": checkpoint.scan_offset,
                    "previousSearchOffset": bootstrap.as_ref().map(|item| item.search_offset).unwrap_or(0),
                });
                rt.warn(
                    "Codex fork transcript inode changed before owned history was consumed; restarting bootstrap",
                    meta.as_object(),
                );
                let mut next = empty_checkpoint(stat.ino);
                if let Some(mut bootstrap) = bootstrap {
                    bootstrap.state = ForkBootstrapState::LivePending;
                    bootstrap.search_offset = 0;
                    next.fork_bootstrap = Some(bootstrap);
                }
                checkpoint = next;
                changed = true;
            } else {
                let meta = json!({
                    "transcriptPath": file_path,
                    "previousInode": checkpoint.inode,
                    "currentInode": stat.ino,
                    "previousScanOffset": checkpoint.scan_offset,
                    "hadForkBootstrap": checkpoint.fork_bootstrap.is_some(),
                    "hasCopiedPrefix": has_indexed_copied,
                });
                rt.warn(
                    "Codex transcript inode changed; applying no-replay baseline",
                    meta.as_object(),
                );
                if has_indexed_copied {
                    self.park_fork_baseline(rt, file_path, &key).await?;
                } else {
                    self.baseline_file(rt, file_path, &key).await?;
                }
                return Ok(0);
            }
        }
        let backlog = (stat.size - checkpoint.scan_offset).max(0) as u64;
        scan::observe_backlog(rt, backlog);
        if rollout_caught_up(&checkpoint, stat.ino, stat.size) {
            return Ok(0);
        }
        if checkpoint.owner_session_meta_offset.is_none() {
            checkpoint.owner_session_meta_offset =
                find_owner_session_meta_offset(file_path, stat.size, Some(rt)).await?;
            changed = true;
        }
        let active_access = checkpoint.scan_offset < stat.size
            || checkpoint.active_turn.is_some()
            || checkpoint.pending_terminal.is_some()
            || checkpoint.pending_fusion.is_some()
            || checkpoint.pending_subagent.is_some()
            || checkpoint
                .fork_bootstrap
                .as_ref()
                .is_some_and(|item| item.state == ForkBootstrapState::LivePending);
        self.register_subagent_owner_meta(
            rt,
            file_path,
            checkpoint.inode,
            stat.size,
            checkpoint.owner_session_meta_offset,
            active_access,
        )
        .await?;
        let owner_meta = self
            .body
            .with_shared(|shared| shared.meta_by_path.get(file_path).cloned());
        let is_direct_subagent = owner_meta.as_ref().is_some_and(|meta| {
            meta.thread_source == CodexThreadSource::Subagent && meta.depth == 1
        });
        if owner_meta.as_ref().is_some_and(|meta| meta.depth == 0) {
            if let Some(thread_id) = owner_meta.as_ref().map(|meta| meta.thread_id.clone()) {
                self.register_persisted_subagent_spawns(&checkpoint, &thread_id);
            }
        }
        if has_copied_history_prefix(owner_meta.as_ref())
            && checkpoint.active_turn.is_none()
            && checkpoint.pending_terminal.is_none()
        {
            if let Some(meta) = owner_meta.clone() {
                let prepared = self
                    .prepare_copied_transcript(rt, file_path, stat.size, &meta, &mut checkpoint)
                    .await?;
                changed |= prepared.changed;
                if !prepared.ready {
                    self.save_checkpoint(rt, &key, &checkpoint);
                    return Ok(0);
                }
            }
        }
        let fork_at_start = checkpoint.fork_bootstrap.clone();
        let owned_scan_start = checkpoint.scan_offset;
        let mut emitted_count = 0usize;
        let mut processed_terminals = 0usize;
        if is_direct_subagent
            && checkpoint.pending_terminal.is_some()
            && checkpoint.active_turn.as_ref().is_some_and(|active| {
                checkpoint
                    .pending_terminal
                    .as_ref()
                    .is_some_and(|pending| active.turn_id == pending.turn_id)
            })
            && should_skip_copied_parent_turn(
                checkpoint
                    .pending_terminal
                    .as_ref()
                    .map(|pending| pending.turn_id.as_str())
                    .unwrap_or(""),
                checkpoint
                    .active_turn
                    .as_ref()
                    .map(|active| active.started_at_ms)
                    .unwrap_or(0.0),
                owner_meta.as_ref().and_then(|meta| meta.created_at_ms),
            )
        {
            checkpoint.active_turn = None;
            checkpoint.pending_terminal = None;
            processed_terminals += 1;
            changed = true;
        }
        if is_direct_subagent && owner_meta.is_some() && checkpoint.pending_subagent.is_some() {
            let candidate = owner_meta
                .as_ref()
                .and_then(|meta| self.reliable_candidate_for_child(rt, meta));
            let mismatch = match (candidate.as_ref(), checkpoint.pending_subagent.as_ref()) {
                (Some(candidate), Some(pending)) => {
                    candidate.parent_thread_id != pending.parent_thread_id
                        || candidate.parent_turn_id.as_deref()
                            != Some(pending.parent_turn_id.as_str())
                        || candidate.parent_trace_id.as_deref()
                            != Some(pending.parent_trace_id.as_str())
                        || candidate.parent_tool_call_id.as_deref()
                            != Some(pending.parent_tool_call_id.as_str())
                }
                _ => true,
            };
            if mismatch {
                let released = self
                    .release_captured_subagent_independently(rt, file_path, &mut checkpoint)
                    .await?;
                emitted_count += released.0;
                processed_terminals += released.1;
                changed = true;
            }
        }
        if checkpoint.pending_fusion.is_some() {
            let parent = checkpoint
                .pending_fusion
                .as_ref()
                .map(|fusion| fusion.parent_thread_id.clone())
                .unwrap_or_default();
            let turn_id = checkpoint
                .pending_fusion
                .as_ref()
                .map(|fusion| fusion.turn_id.clone())
                .unwrap_or_default();
            let children = checkpoint
                .pending_fusion
                .as_ref()
                .map(|fusion| fusion.children.clone())
                .unwrap_or_default();
            let reliable = self.reliable_fusion_children(rt, &parent, &turn_id, &children);
            if checkpoint
                .pending_fusion
                .as_ref()
                .is_some_and(|fusion| reliable.len() != fusion.children.len())
            {
                if let Some(fusion) = checkpoint.pending_fusion.as_mut() {
                    fusion.children = reliable.clone();
                }
                if let Some(active) = checkpoint.active_turn.as_mut() {
                    active.subagent_spawns = reliable;
                }
                changed = true;
            }
        }
        if checkpoint
            .pending_fusion
            .as_ref()
            .is_some_and(|fusion| fusion.children.is_empty())
        {
            let released = self
                .release_parent_fusion_independently(rt, file_path, &mut checkpoint)
                .await?;
            emitted_count += released.0;
            processed_terminals += released.1;
            changed = true;
        }
        if checkpoint.pending_fusion.is_some() {
            if changed {
                self.save_checkpoint(rt, &key, &checkpoint);
            }
            return Ok(emitted_count);
        }

        let mut scanned_bytes = 0i64;
        let had_pending = checkpoint.pending_terminal.is_some();
        let pending = self
            .recover_pending_terminal(rt, file_path, &mut checkpoint)
            .await?;
        changed |= had_pending;
        emitted_count += pending.emitted_count;
        processed_terminals += pending.processed_terminal_count;
        if pending.blocked {
            if changed {
                self.save_checkpoint(rt, &key, &checkpoint);
            }
            return Ok(emitted_count);
        }

        while checkpoint.scan_offset < stat.size
            && processed_terminals < MAX_TERMINALS_PER_FILE_CYCLE
            && scanned_bytes < MAX_SCAN_BYTES_PER_FILE_CYCLE
        {
            let scan_start = checkpoint.scan_offset;
            let scan_end = stat
                .size
                .min(scan_start + (MAX_SCAN_BYTES_PER_FILE_CYCLE - scanned_bytes));
            let mut terminal_turn_id: Option<String> = None;
            let mut terminal_end: Option<i64> = None;
            let mut scan = scan_json_lines(file_path, scan_start, scan_end, Some(rt), |line| {
                process_scanned_line(
                    &mut checkpoint,
                    file_path,
                    stat.size,
                    &line,
                    &mut terminal_turn_id,
                    &mut terminal_end,
                    self,
                )
            })
            .await?;
            if scan.next_offset == scan_start && scan_end < stat.size {
                scan = scan_json_lines(file_path, scan_start, stat.size, Some(rt), |line| {
                    process_scanned_line(
                        &mut checkpoint,
                        file_path,
                        stat.size,
                        &line,
                        &mut terminal_turn_id,
                        &mut terminal_end,
                        self,
                    );
                    false
                })
                .await?;
            }
            if scan.next_offset == scan_start {
                break;
            }
            changed = true;
            let next_scan_offset = terminal_end.unwrap_or(scan.next_offset);
            scan::observe_batch(
                rt,
                CommittedInputBatch {
                    bytes: (next_scan_offset - scan_start).max(0) as u64,
                    records: scan.records,
                    parse_success_records: scan.parse_success_records,
                    parse_failed_records: scan.parse_failed_records,
                    max_record_bytes: scan.max_record_bytes,
                },
            );
            scanned_bytes += next_scan_offset - scan_start;
            let mut blocked = false;
            if checkpoint
                .active_turn
                .as_ref()
                .is_some_and(|active| next_scan_offset > active.start_offset)
            {
                if is_direct_subagent
                    && terminal_turn_id.is_some()
                    && should_skip_copied_parent_turn(
                        terminal_turn_id.as_deref().unwrap_or(""),
                        checkpoint
                            .active_turn
                            .as_ref()
                            .map(|active| active.started_at_ms)
                            .unwrap_or(0.0),
                        owner_meta.as_ref().and_then(|meta| meta.created_at_ms),
                    )
                {
                    checkpoint.active_turn = None;
                    processed_terminals += 1;
                    checkpoint.scan_offset = next_scan_offset;
                    continue;
                }
                let active_before = if terminal_turn_id.is_some() {
                    checkpoint.active_turn.clone()
                } else {
                    None
                };
                let reliable = if is_direct_subagent
                    && owner_meta.is_some()
                    && checkpoint.pending_subagent.is_none()
                {
                    owner_meta
                        .as_ref()
                        .and_then(|meta| self.reliable_candidate_for_child(rt, meta))
                } else {
                    None
                };
                let recovered = if reliable.is_some() {
                    let mut cloned = checkpoint.clone();
                    self.recover_turn_segment(
                        rt,
                        file_path,
                        &mut cloned,
                        RecoveryBoundary {
                            end_offset: next_scan_offset,
                            terminal: terminal_turn_id.is_some(),
                            inferred: None,
                        },
                        reliable.as_ref().map(|item| item.parent_thread_id.as_str()),
                    )
                    .await?
                } else {
                    self.recover_turn_segment(
                        rt,
                        file_path,
                        &mut checkpoint,
                        RecoveryBoundary {
                            end_offset: next_scan_offset,
                            terminal: terminal_turn_id.is_some(),
                            inferred: None,
                        },
                        None,
                    )
                    .await?
                };
                if let (true, Some(link), Some(active)) = (
                    terminal_turn_id.is_some()
                        && !matches!(recovered.kind, RecoveryKind::Unparseable),
                    reliable.as_ref(),
                    active_before.as_ref(),
                ) {
                    checkpoint.pending_subagent = Some(CodexPendingSubagentTurn {
                        turn_id: terminal_turn_id.clone().unwrap_or_default(),
                        parent_thread_id: link.parent_thread_id.clone(),
                        parent_turn_id: link.parent_turn_id.clone().unwrap_or_default(),
                        parent_trace_id: link.parent_trace_id.clone().unwrap_or_default(),
                        parent_tool_call_id: link.parent_tool_call_id.clone().unwrap_or_default(),
                        confidence: match link.confidence {
                            CodexSubagentLinkConfidence::AgentPath => FusionConfidence::AgentPath,
                            _ => FusionConfidence::ExplicitId,
                        },
                        terminal_end_offset: next_scan_offset,
                        active_turn: active.clone(),
                    });
                    checkpoint.active_turn = None;
                    checkpoint.scan_offset = next_scan_offset;
                    processed_terminals += 1;
                    continue;
                }
                let fusion_parent = owner_meta
                    .as_ref()
                    .map(|meta| meta.thread_id.clone())
                    .unwrap_or_else(|| session_id_from_transcript_path(file_path));
                let fusion_children = if terminal_turn_id.is_some() {
                    let spawns = checkpoint
                        .active_turn
                        .as_ref()
                        .map(|active| active.subagent_spawns.clone())
                        .unwrap_or_default();
                    self.reliable_fusion_children(
                        rt,
                        &fusion_parent,
                        terminal_turn_id.as_deref().unwrap_or(""),
                        &spawns,
                    )
                } else {
                    Vec::new()
                };
                if terminal_turn_id.is_some()
                    && !matches!(recovered.kind, RecoveryKind::Unparseable)
                    && !fusion_children.is_empty()
                    && active_before.is_some()
                    && next_scan_offset >= stat.size
                {
                    if let Some(active) = checkpoint.active_turn.as_mut() {
                        active.subagent_spawns = fusion_children.clone();
                    } else if let Some(mut active) = active_before.clone() {
                        active.subagent_spawns = fusion_children.clone();
                        checkpoint.active_turn = Some(active);
                    }
                    checkpoint.pending_fusion = Some(CodexPendingFusionTurn {
                        turn_id: terminal_turn_id.clone().unwrap_or_default(),
                        parent_thread_id: fusion_parent,
                        parent_trace_id: fusion_children
                            .first()
                            .map(|child| child.parent_trace_id.clone())
                            .unwrap_or_default(),
                        terminal_end_offset: next_scan_offset,
                        children: fusion_children,
                    });
                    blocked = true;
                } else if reliable.is_none() {
                    emitted_count += self.body.emit_batches(recovered.entries.clone());
                }
                if !matches!(recovered.kind, RecoveryKind::Unparseable)
                    && checkpoint.pending_fusion.is_none()
                    && reliable.is_none()
                    && checkpoint
                        .active_turn
                        .as_ref()
                        .is_some_and(|active| recovered.consumed_end_offset > active.start_offset)
                {
                    if let Some(active) = checkpoint.active_turn.as_mut() {
                        active.start_offset = recovered.consumed_end_offset;
                    }
                }
                if terminal_turn_id.is_some()
                    && checkpoint.active_turn.as_ref().is_some_and(|active| {
                        Some(active.turn_id.as_str()) == terminal_turn_id.as_deref()
                    })
                {
                    if matches!(recovered.kind, RecoveryKind::Unparseable) {
                        let turn_id = terminal_turn_id.clone().unwrap_or_default();
                        let start = checkpoint
                            .active_turn
                            .as_ref()
                            .map(|active| active.start_offset)
                            .unwrap_or(0);
                        checkpoint.pending_terminal = Some(new_pending_terminal(
                            turn_id.clone(),
                            next_scan_offset,
                            recovered.diagnostics.source_record_count as u64,
                        ));
                        let meta = json!({
                            "transcriptPath": file_path,
                            "turnId": turn_id,
                            "range": { "startOffset": start, "endOffset": next_scan_offset },
                            "retryCount": checkpoint.pending_terminal.as_ref().and_then(|item| item.retry_count),
                            "sourceRecordCount": recovered.diagnostics.source_record_count,
                        });
                        rt.warn(
                            "terminal Codex turn could not be parsed; retaining it for the next scan",
                            meta.as_object(),
                        );
                        blocked = true;
                    } else if checkpoint.pending_fusion.is_none() {
                        checkpoint.active_turn = None;
                        checkpoint.pending_terminal = None;
                        processed_terminals += 1;
                    }
                }
            }
            checkpoint.scan_offset = next_scan_offset;
            if blocked || terminal_turn_id.is_none() {
                break;
            }
        }
        if fork_at_start.is_some() && checkpoint.scan_offset > owned_scan_start {
            checkpoint.fork_bootstrap = None;
            changed = true;
        }
        if changed {
            self.save_checkpoint(rt, &key, &checkpoint);
        }
        Ok(emitted_count)
    }
}

fn process_scanned_line(
    checkpoint: &mut CodexTranscriptCheckpoint,
    file_path: &str,
    file_size: i64,
    line: &JsonLine,
    terminal_turn_id: &mut Option<String>,
    terminal_end: &mut Option<i64>,
    engine: &CodexTranscriptEngine<'_>,
) -> bool {
    let Some(payload) = line.record.get("payload").and_then(Value::as_object) else {
        return true;
    };
    if line.record.get("type").and_then(Value::as_str) == Some("session_meta") {
        let owner =
            select_owner_session_meta_offset(file_path, checkpoint.owner_session_meta_offset, line);
        checkpoint.owner_session_meta_offset = owner;
        if owner == Some(line.start_offset) {
            if let Some(meta) = extract_codex_transcript_meta(&line.record) {
                engine.remember_transcript_owner_meta(
                    file_path,
                    checkpoint.inode,
                    file_size,
                    line.start_offset,
                    meta.clone(),
                    true,
                );
                engine.publish_transcript_owner_meta(file_path, meta);
            }
        }
        return true;
    }
    if let Some(turn_id) = turn_id_for_start(&line.record, payload) {
        if checkpoint
            .active_turn
            .as_ref()
            .is_none_or(|active| active.turn_id != turn_id)
        {
            checkpoint.active_turn = Some(create_active_turn(
                turn_id.clone(),
                line.start_offset,
                utils::timestamp_ms(&line.record, now_ms()),
                false,
            ));
        }
        if let Some(active) = checkpoint.active_turn.as_mut() {
            update_active_turn_metadata(active, &line.record, payload);
        }
        return true;
    }
    if let Some(terminal) = terminal_turn_id_for(&line.record, payload) {
        if checkpoint
            .active_turn
            .as_ref()
            .is_some_and(|active| active.turn_id == terminal)
        {
            *terminal_turn_id = Some(terminal);
            *terminal_end = Some(line.end_offset);
            return false;
        }
    }
    true
}

use indexmap::IndexMap;

fn path_relative(from: &str, to: &str) -> String {
    match Path::new(to).strip_prefix(Path::new(from)) {
        Ok(relative) => relative.to_string_lossy().into_owned(),
        Err(_) => String::new(),
    }
}

async fn rollout_dirs_unchanged(dir_mtimes: &[(String, i64)]) -> bool {
    if dir_mtimes.is_empty() {
        return false;
    }
    for (dir, mtime) in dir_mtimes {
        match tokio::fs::metadata(dir).await {
            Ok(meta) => {
                if file_meta_snapshot(&meta).2 != *mtime {
                    return false;
                }
            }
            Err(_) => return false,
        }
    }
    true
}

fn rollout_caught_up(checkpoint: &CodexTranscriptCheckpoint, ino: u64, size: i64) -> bool {
    checkpoint.inode == ino
        && checkpoint.scan_offset >= size
        && checkpoint.owner_session_meta_offset.is_some()
        && checkpoint.active_turn.is_none()
        && checkpoint.pending_terminal.is_none()
        && checkpoint.pending_fusion.is_none()
        && checkpoint.pending_subagent.is_none()
        && checkpoint.fork_bootstrap.is_none()
}

fn should_skip_copied_parent_turn(
    turn_id: &str,
    observed_started_at_ms: f64,
    owner_created_at_ms: Option<f64>,
) -> bool {
    let Some(owner_created_at_ms) = owner_created_at_ms else {
        return false;
    };
    uuid_v7_timestamp_ms(turn_id).unwrap_or(observed_started_at_ms) < owner_created_at_ms
}

impl<'a> CodexTranscriptEngine<'a> {
    async fn recover_pending_terminal(
        &self,
        rt: &InputRuntime,
        file_path: &str,
        checkpoint: &mut CodexTranscriptCheckpoint,
    ) -> anyhow::Result<PendingRecovery> {
        let Some(pending) = checkpoint.pending_terminal.clone() else {
            return Ok(PendingRecovery {
                blocked: false,
                emitted_count: 0,
                processed_terminal_count: 0,
            });
        };
        if checkpoint
            .active_turn
            .as_ref()
            .is_none_or(|active| active.turn_id != pending.turn_id)
        {
            checkpoint.pending_terminal = None;
            return Ok(PendingRecovery {
                blocked: false,
                emitted_count: 0,
                processed_terminal_count: 0,
            });
        }
        let recovered = self
            .recover_turn_segment(
                rt,
                file_path,
                checkpoint,
                RecoveryBoundary {
                    end_offset: pending.terminal_end_offset,
                    terminal: true,
                    inferred: None,
                },
                None,
            )
            .await?;
        if matches!(recovered.kind, RecoveryKind::Unparseable) {
            let now = now_ms();
            checkpoint.pending_terminal = Some(super::super::types::CodexPendingTerminalTurn {
                turn_id: pending.turn_id.clone(),
                terminal_end_offset: pending.terminal_end_offset,
                retry_count: Some(pending.retry_count.unwrap_or(0) + 1),
                first_pending_at_ms: Some(pending.first_pending_at_ms.unwrap_or(now)),
                last_attempt_at_ms: Some(now),
                source_record_count: Some(recovered.diagnostics.source_record_count as u64),
            });
            let meta = json!({
                "transcriptPath": file_path,
                "turnId": pending.turn_id,
                "retryCount": checkpoint.pending_terminal.as_ref().and_then(|item| item.retry_count),
                "sourceRecordCount": recovered.diagnostics.source_record_count,
            });
            rt.warn(
                "pending Codex terminal turn still could not be parsed; will retry",
                meta.as_object(),
            );
            return Ok(PendingRecovery {
                blocked: true,
                emitted_count: 0,
                processed_terminal_count: 0,
            });
        }
        let emitted = self.body.emit_batches(recovered.entries);
        checkpoint.active_turn = None;
        checkpoint.pending_terminal = None;
        Ok(PendingRecovery {
            blocked: false,
            emitted_count: emitted,
            processed_terminal_count: 1,
        })
    }

    async fn recover_turn_segment(
        &self,
        rt: &InputRuntime,
        file_path: &str,
        checkpoint: &mut CodexTranscriptCheckpoint,
        boundary: RecoveryBoundary,
        attachment_parent_thread_id: Option<&str>,
    ) -> anyhow::Result<Recovery> {
        let RecoveryBoundary {
            end_offset,
            terminal,
            inferred,
        } = boundary;
        let Some(active) = checkpoint.active_turn.clone() else {
            return Ok(empty_unparseable(end_offset));
        };
        let records = read_json_lines(file_path, active.start_offset, end_offset, Some(rt)).await?;
        let current_stat = tokio::fs::metadata(file_path).await.ok();
        let current_ino = current_stat.as_ref().map(|meta| file_meta_snapshot(meta).0);
        let current_size = current_stat.as_ref().map(|meta| file_meta_snapshot(meta).1);
        let mut meta = if current_ino == Some(checkpoint.inode) {
            current_size
                .and_then(|size| self.cached_meta_for_checkpoint(file_path, checkpoint, size))
        } else {
            None
        };
        if meta.is_none() {
            if let Some(offset) = checkpoint.owner_session_meta_offset {
                if let Some(record) = read_json_line_at(file_path, offset, Some(rt)).await? {
                    meta = extract_codex_transcript_meta(&record);
                    if let Some(meta) = meta.clone() {
                        if current_ino == Some(checkpoint.inode)
                            && current_size.is_some_and(|size| offset >= 0 && offset < size)
                        {
                            self.remember_transcript_owner_meta(
                                file_path,
                                checkpoint.inode,
                                current_size.unwrap_or(0),
                                offset,
                                meta.clone(),
                                true,
                            );
                        }
                        self.publish_transcript_owner_meta(file_path, meta);
                    }
                }
            }
        }
        let sources: Vec<CodexTranscriptSourceRecord> = records
            .items
            .iter()
            .map(|line| CodexTranscriptSourceRecord {
                start_offset: line.start_offset,
                end_offset: line.end_offset,
                record: line.record.clone(),
            })
            .collect();
        let upload_mode = self.body.upload_mode;
        let processor = self.body.processor.clone();
        let include = self.body.include_multimodal && processor.is_some();
        let blob = if include {
            Some(self.make_blob(
                rt,
                file_path,
                active.cwd.as_deref(),
                attachment_parent_thread_id,
            ))
        } else {
            None
        };
        let blob_to_uri = blob.as_ref().map(blob_as_dyn);
        let extraction = extract_codex_partial_turn_with_boundaries(
            &sources,
            meta.as_ref(),
            &session_id_from_transcript_path(file_path),
            &active.turn_id,
            &ExtractOptions {
                require_terminal: false,
                started_at_ms: Some(active.started_at_ms),
                next_step_started_at_ms: active.next_step_started_at_ms,
                last_cumulative_token_total: active.last_cumulative_token_total,
                model: active.model.clone(),
                cwd: active.cwd.clone(),
                developer_instructions: active.developer_instructions.clone(),
                blob_to_uri,
                upload_mode: include.then_some(upload_mode),
            },
        );
        let Some(mut extraction) = extraction else {
            return Ok(Recovery {
                kind: RecoveryKind::Unparseable,
                entries: Vec::new(),
                consumed_end_offset: active.start_offset,
                diagnostics: Diagnostics {
                    source_record_count: records.items.len(),
                },
            });
        };
        if let Some(status) = inferred {
            extraction.turn.status = status;
        }
        if let Some(slot) = checkpoint.active_turn.as_mut() {
            update_active_turn_from_extracted(
                slot,
                &extraction.turn.model,
                extraction.turn.cwd.as_deref(),
                extraction.turn.developer_instructions.as_deref(),
            );
        }
        if !extraction.turn.unmatched_token_usages.is_empty() {
            let meta = json!({
                "transcriptPath": file_path,
                "turnId": active.turn_id,
                "count": extraction.turn.unmatched_token_usages.len(),
            });
            rt.warn(
                "Codex transcript token samples could not be assigned to a response wave",
                meta.as_object(),
            );
        }
        let step_start = checkpoint
            .active_turn
            .as_ref()
            .map(|active| active.emitted_step_count)
            .unwrap_or(0)
            + 1;
        let closed = if terminal {
            extraction.turn.steps.len()
        } else {
            extraction.committed_step_count
        };
        let mut committed = extraction.turn.clone();
        if closed != extraction.turn.steps.len() {
            committed.steps.truncate(closed);
        }
        let input_context = self
            .resolve_input_context(rt, file_path, checkpoint, meta.as_ref())
            .await?;
        let built = build_codex_transcript_segment(
            &committed,
            &CodexTranscriptBuildOptions {
                include_prompt: checkpoint
                    .active_turn
                    .as_ref()
                    .is_none_or(|active| !active.emitted_prompt),
                start_step_number: step_start,
                input_context,
                context_step_count: Some(committed.steps.len()),
            },
        );
        if meta.as_ref().is_some_and(|meta| meta.depth == 0) {
            if let Some(parent_trace) = built.entries.first_trace_id() {
                let spawns =
                    extract_codex_spawn_descriptors(&extraction.turn, &sources, &parent_trace);
                self.body
                    .with_shared(|shared| shared.linker.register_spawns(&spawns));
                let children = spawns_to_children(&spawns);
                if let Some(active) = checkpoint.active_turn.as_mut() {
                    active.subagent_spawns =
                        merge_pending_fusion_children(&active.subagent_spawns, &children);
                }
            }
        }
        let mut ready = built.entries.clone();
        if terminal && !built.entries.is_empty() {
            if let Some(marker) = codex_turn_end_marker(&built.entries, extraction.turn.status) {
                ready.push(marker);
            }
        }
        let entries = if let Some(active) = checkpoint.active_turn.as_mut() {
            filter_new_segment_entries(ready.clone(), active)
        } else {
            ready.clone()
        };
        if let Some(active) = checkpoint.active_turn.as_mut() {
            active.emitted_step_count += closed as u64;
            if closed > 0 {
                let range = extraction.committed_step_ranges.get(closed - 1);
                active.input_context =
                    Some(persisted_input_context(&built.next_input_context, range));
                if let Some(next) = extraction.next_step_started_at_ms {
                    active.next_step_started_at_ms = Some(next);
                }
                if let Some(total) = extraction.last_cumulative_token_total {
                    active.last_cumulative_token_total = Some(total);
                }
            }
        }
        let consumed = if terminal {
            end_offset
        } else {
            extraction.consumed_end_offset
        };
        let resource = self
            .read_wakeup_resource_attributes(rt, &extraction.turn.session_id)
            .await;
        let span = self
            .read_turn_span_attributes(rt, &extraction.turn.session_id, &active.turn_id)
            .await;
        let mut output = if let Some(resource) = resource {
            attach_wakeup_resource_attributes(entries, &resource)
        } else {
            entries
        };
        if let Some(span) = span {
            attach_turn_span_attributes(&mut output, &span);
        }
        let kind = if output.is_empty() {
            RecoveryKind::ProcessedEmpty
        } else {
            RecoveryKind::ProcessedEmitted
        };
        Ok(Recovery {
            kind,
            entries: output,
            consumed_end_offset: consumed,
            diagnostics: Diagnostics {
                source_record_count: records.items.len(),
            },
        })
    }
}

trait FirstTrace {
    fn first_trace_id(&self) -> Option<String>;
}

impl FirstTrace for Vec<AgentActivityEntry> {
    fn first_trace_id(&self) -> Option<String> {
        self.first().and_then(|entry| entry.trace_id.clone())
    }
}

fn spawns_to_children(spawns: &[CodexSpawnDescriptor]) -> Vec<CodexPendingFusionChild> {
    spawns
        .iter()
        .map(|spawn| CodexPendingFusionChild {
            parent_tool_call_id: spawn.parent_tool_call_id.clone(),
            parent_trace_id: spawn.parent_trace_id.clone(),
            spawned_at_ms: spawn.spawned_at_ms,
            task_name: spawn.task_name.clone(),
            agent_path: spawn.agent_path.clone(),
            child_thread_id: spawn.child_thread_id.clone(),
        })
        .collect()
}

fn empty_unparseable(end_offset: i64) -> Recovery {
    Recovery {
        kind: RecoveryKind::Unparseable,
        entries: Vec::new(),
        consumed_end_offset: end_offset,
        diagnostics: Diagnostics {
            source_record_count: 0,
        },
    }
}

fn filter_new_segment_entries(
    entries: Vec<AgentActivityEntry>,
    active: &mut CodexActiveTranscriptTurn,
) -> Vec<AgentActivityEntry> {
    let mut requests: HashSet<String> = active.emitted_step_request_ids.iter().cloned().collect();
    let mut responses: HashSet<String> = active.emitted_step_response_ids.iter().cloned().collect();
    let mut calls: HashSet<String> = active.emitted_tool_call_ids.iter().cloned().collect();
    let mut results: HashSet<String> = active.emitted_tool_result_ids.iter().cloned().collect();
    let mut out = Vec::new();
    for entry in entries {
        if entry.event_name == Some(AgentEventName::Other) {
            if entry.turn_end == Some(true) {
                out.push(entry);
                continue;
            }
            if active.emitted_prompt {
                continue;
            }
            active.emitted_prompt = true;
            out.push(entry);
            continue;
        }
        let step_id = entry.step_id.clone().unwrap_or_default();
        let tool_id = entry.tool_call_id.clone().unwrap_or_default();
        if entry.event_name == Some(AgentEventName::LlmRequest) {
            if step_id.is_empty() || !requests.insert(step_id) {
                continue;
            }
            out.push(entry);
        } else if entry.event_name == Some(AgentEventName::LlmResponse) {
            if step_id.is_empty() || !responses.insert(step_id) {
                continue;
            }
            out.push(entry);
        } else if entry.event_name == Some(AgentEventName::ToolCall) {
            if tool_id.is_empty() || !calls.insert(tool_id) {
                continue;
            }
            out.push(entry);
        } else if entry.event_name == Some(AgentEventName::ToolResult) {
            if tool_id.is_empty() || !results.insert(tool_id) {
                continue;
            }
            out.push(entry);
        } else {
            out.push(entry);
        }
    }
    active.emitted_step_request_ids = requests.into_iter().collect();
    active.emitted_step_response_ids = responses.into_iter().collect();
    active.emitted_tool_call_ids = calls.into_iter().collect();
    active.emitted_tool_result_ids = results.into_iter().collect();
    out
}

fn persisted_input_context(
    context: &CodexTranscriptInputContext,
    range: Option<&super::super::types::CodexTranscriptSourceRange>,
) -> CodexTranscriptInputContext {
    let delta = context.delta.clone().unwrap_or_default();
    let bytes = json_bytes(&Value::Array(delta.clone()));
    if bytes <= MAX_PERSISTED_INPUT_CONTEXT_BYTES || range.is_none() {
        CodexTranscriptInputContext {
            hash: context.hash.clone(),
            delta: Some(delta),
            full_messages: context.full_messages.clone(),
            delta_range: None,
        }
    } else {
        CodexTranscriptInputContext {
            hash: context.hash.clone(),
            delta: None,
            full_messages: context.full_messages.clone(),
            delta_range: range.cloned(),
        }
    }
}

fn codex_turn_end_marker(
    entries: &[AgentActivityEntry],
    status: CodexTerminalStatus,
) -> Option<AgentActivityEntry> {
    let source = entries
        .iter()
        .rev()
        .find(|entry| entry.event_name == Some(AgentEventName::LlmResponse))
        .or_else(|| entries.last())?;
    let mut fields = Map::new();
    fields.insert(
        "time_unix_nano".into(),
        Value::String(source.time_unix_nano.clone()),
    );
    if let Some(observed) = &source.observed_time_unix_nano {
        fields.insert(
            "observed_time_unix_nano".into(),
            Value::String(observed.clone()),
        );
    }
    fields.insert(
        "event.id".into(),
        Value::String(format!("{}:turn-end", source.event_id)),
    );
    fields.insert("event.name".into(), Value::String("other".into()));
    fields.insert("user.id".into(), Value::String(source.user_id.clone()));
    if let Some(trace) = &source.trace_id {
        fields.insert("trace_id".into(), Value::String(trace.clone()));
    }
    fields.insert(
        "parent_span_id".into(),
        Value::String("0000000000000001".into()),
    );
    fields.insert(
        "gen_ai.session.id".into(),
        Value::String(source.session_id.clone()),
    );
    if let Some(turn) = &source.turn_id {
        fields.insert("gen_ai.turn.id".into(), Value::String(turn.clone()));
    }
    fields.insert("gen_ai.turn.end".into(), Value::Bool(true));
    fields.insert(
        "gen_ai.agent.type".into(),
        Value::String(source.agent_type.clone()),
    );
    if let Some(agent) = &source.agent_id {
        fields.insert("gen_ai.agent.id".into(), Value::String(agent.clone()));
    }
    if let Some(name) = &source.agent_name {
        fields.insert("gen_ai.agent.name".into(), Value::String(name.clone()));
    }
    fields.insert(
        "gen_ai.provider.name".into(),
        Value::String(source.provider_name.clone()),
    );
    if let Some(value) = source.extra.get("agent.codex.transcript_turn_id") {
        fields.insert("agent.codex.transcript_turn_id".into(), value.clone());
    }
    if let Some(value) = source.extra.get("agent.codex.cwd") {
        fields.insert("agent.codex.cwd".into(), value.clone());
    }
    fields.insert(
        "agent.codex.turn_status".into(),
        Value::String(status.as_str().to_owned()),
    );
    fields.insert(
        "timestamp".into(),
        Value::String(source.time_unix_nano.clone()),
    );
    Some(build_agent_activity_entry(&Value::Object(fields)))
}

fn attach_wakeup_resource_attributes(
    mut entries: Vec<AgentActivityEntry>,
    resource: &Map<String, Value>,
) -> Vec<AgentActivityEntry> {
    let worker = resource
        .get("agentteams.worker.name")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(str::to_owned);
    for entry in &mut entries {
        entry.resource_attributes = Some(resource.clone());
        if let Some(worker) = &worker {
            entry.agent_name = Some(worker.clone());
        }
    }
    entries
}

fn attach_turn_span_attributes(entries: &mut [AgentActivityEntry], span: &HashMap<String, String>) {
    for entry in entries {
        for (key, value) in span {
            set_if_absent(entry, key, value);
        }
    }
}

fn set_if_absent(entry: &mut AgentActivityEntry, key: &str, value: &str) {
    let Ok(Value::Object(mut map)) = serde_json::to_value(&*entry) else {
        return;
    };
    if map.contains_key(key) {
        return;
    }
    map.insert(key.to_owned(), Value::String(value.to_owned()));
    if let Ok(updated) = serde_json::from_value(Value::Object(map)) {
        *entry = updated;
    }
}

fn blob_as_dyn<F>(func: &F) -> &dyn Fn(&BlobToUriParams) -> Option<UriResult>
where
    F: Fn(&BlobToUriParams) -> Option<UriResult>,
{
    func
}

impl<'a> CodexTranscriptEngine<'a> {
    fn make_blob<'b>(
        &'b self,
        rt: &'b InputRuntime,
        transcript_path: &str,
        cwd: Option<&str>,
        attachment_parent: Option<&str>,
    ) -> impl Fn(&BlobToUriParams) -> Option<UriResult> + 'b {
        let transcript_path = transcript_path.to_owned();
        let cwd = cwd.map(str::to_owned);
        let attachment_parent = attachment_parent.map(str::to_owned);
        move |params: &BlobToUriParams| {
            let meta = self
                .body
                .with_shared(|shared| shared.meta_by_path.get(&transcript_path).cloned());
            let session = meta
                .as_ref()
                .map(|meta| meta.thread_id.clone())
                .filter(|id| !id.is_empty())
                .unwrap_or_else(|| session_id_from_transcript_path(&transcript_path));
            let mut record = Map::new();
            record.insert("gen_ai.session.id".into(), Value::String(session));
            if let Some(cwd) = &cwd {
                record.insert("cwd".into(), Value::String(cwd.clone()));
            }
            let allowed = rt.with_workspace_policy(|policy| policy.allows(&record, "codex"));
            if !allowed {
                return None;
            }
            if let Some(parent) = &attachment_parent {
                let mut parent_record = Map::new();
                parent_record.insert("gen_ai.session.id".into(), Value::String(parent.clone()));
                if !rt.with_workspace_policy(|policy| policy.allows(&parent_record, "codex")) {
                    return None;
                }
            }
            let processor = self.body.processor.as_ref()?;
            let cache_key = params
                .reuse_key
                .as_ref()
                .map(|key| format!("{transcript_path}\0{key}"));
            if let Some(cache_key) = &cache_key {
                if let Ok(mut cache) = self.body.uri_cache.lock() {
                    if let Some(hit) = cache.get_copied(cache_key) {
                        return Some(hit);
                    }
                }
            }
            let result = processor.blob_to_uri(params.clone());
            if let (Some(result), Some(cache_key)) = (result.clone(), cache_key) {
                if let Ok(mut cache) = self.body.uri_cache.lock() {
                    cache.set(cache_key, result.clone());
                }
            }
            result
        }
    }

    pub fn has_pending_fusion_for_parent(&self, rt: &InputRuntime, parent_thread_id: &str) -> bool {
        for key in rt.state_store.keys() {
            if !key.starts_with("codex-transcript:") {
                continue;
            }
            if self
                .read_checkpoint(rt, &key)
                .and_then(|checkpoint| checkpoint.pending_fusion)
                .is_some_and(|fusion| fusion.parent_thread_id == parent_thread_id)
            {
                return true;
            }
        }
        false
    }

    pub async fn baseline_file(
        &self,
        rt: &InputRuntime,
        file_path: &str,
        key: &str,
    ) -> anyhow::Result<()> {
        let Ok(meta) = tokio::fs::metadata(file_path).await else {
            return Ok(());
        };
        let (inode, size, _) = file_meta_snapshot(&meta);
        let mut owner = None;
        let mut active: Option<CodexActiveTranscriptTurn> = None;
        let stats = scan_json_lines(file_path, 0, size, Some(rt), |line| {
            let Some(payload) = line.record.get("payload").and_then(Value::as_object) else {
                return true;
            };
            if line.record.get("type").and_then(Value::as_str) == Some("session_meta") {
                owner = select_owner_session_meta_offset(file_path, owner, &line);
                return true;
            }
            if let Some(turn_id) = turn_id_for_start(&line.record, payload) {
                if active
                    .as_ref()
                    .is_none_or(|current| current.turn_id != turn_id)
                {
                    active = Some(create_active_turn(
                        turn_id.clone(),
                        line.start_offset,
                        utils::timestamp_ms(&line.record, now_ms()),
                        true,
                    ));
                }
                if let Some(current) = active.as_mut() {
                    if current.turn_id == turn_id {
                        update_active_turn_metadata(current, &line.record, payload);
                    }
                }
                return true;
            }
            if terminal_turn_id_for(&line.record, payload).as_deref()
                == active.as_ref().map(|item| item.turn_id.as_str())
            {
                active = None;
            }
            true
        })
        .await?;
        if let Some(active) = active.as_mut() {
            active.start_offset = stats.next_offset;
        }
        self.save_checkpoint(
            rt,
            key,
            &CodexTranscriptCheckpoint {
                inode,
                scan_offset: stats.next_offset,
                active_turn: active,
                pending_terminal: None,
                pending_fusion: None,
                pending_subagent: None,
                owner_session_meta_offset: owner,
                fork_bootstrap: None,
            },
        );
        Ok(())
    }

    pub async fn park_fork_baseline(
        &self,
        rt: &InputRuntime,
        file_path: &str,
        key: &str,
    ) -> anyhow::Result<()> {
        let Ok(meta) = tokio::fs::metadata(file_path).await else {
            return Ok(());
        };
        let (inode, size, _) = file_meta_snapshot(&meta);
        let scan_offset = last_complete_jsonl_offset(file_path, size, Some(rt)).await?;
        let owner = find_owner_session_meta_offset(file_path, scan_offset, Some(rt)).await?;
        self.save_checkpoint(
            rt,
            key,
            &CodexTranscriptCheckpoint {
                inode,
                scan_offset,
                active_turn: None,
                pending_terminal: None,
                pending_fusion: None,
                pending_subagent: None,
                owner_session_meta_offset: owner,
                fork_bootstrap: Some(CodexForkBootstrap {
                    state: ForkBootstrapState::BaselineSearch,
                    search_offset: 0,
                    initial_turn_id: None,
                    recovery_turn_id: None,
                    tail_candidate: None,
                }),
            },
        );
        Ok(())
    }
}

struct Prepared {
    ready: bool,
    changed: bool,
}

impl<'a> CodexTranscriptEngine<'a> {
    async fn prepare_copied_transcript(
        &self,
        rt: &InputRuntime,
        file_path: &str,
        file_size: i64,
        owner_meta: &CodexTranscriptMeta,
        checkpoint: &mut CodexTranscriptCheckpoint,
    ) -> anyhow::Result<Prepared> {
        if checkpoint.scan_offset == 0 && checkpoint.fork_bootstrap.is_none() {
            checkpoint.fork_bootstrap = Some(CodexForkBootstrap {
                state: ForkBootstrapState::LivePending,
                search_offset: 0,
                initial_turn_id: None,
                recovery_turn_id: None,
                tail_candidate: None,
            });
        }
        let Some(current) = checkpoint.fork_bootstrap.clone() else {
            return Ok(Prepared {
                ready: true,
                changed: false,
            });
        };
        if current.state == ForkBootstrapState::BaselineTail {
            let rebuilt = self
                .rebuild_fork_baseline_tail(rt, file_path, checkpoint)
                .await?;
            return Ok(Prepared {
                ready: rebuilt,
                changed: true,
            });
        }
        let marker = self
            .read_wakeup_marker(&owner_meta.thread_id, file_path)
            .await;
        let mut bootstrap = current;
        if marker
            .as_ref()
            .and_then(|marker| marker.initial_turn_id.as_ref())
            .is_some_and(|id| bootstrap.initial_turn_id.as_deref() != Some(id.as_str()))
        {
            bootstrap.initial_turn_id = marker
                .as_ref()
                .and_then(|marker| marker.initial_turn_id.clone());
            bootstrap.recovery_turn_id = marker
                .as_ref()
                .and_then(|marker| marker.recovery_turn_id.clone());
            bootstrap.search_offset = 0;
        } else if marker
            .as_ref()
            .and_then(|marker| marker.recovery_turn_id.as_ref())
            .is_some_and(|id| bootstrap.recovery_turn_id.as_deref() != Some(id.as_str()))
        {
            bootstrap.recovery_turn_id = marker
                .as_ref()
                .and_then(|marker| marker.recovery_turn_id.clone());
            if bootstrap.initial_turn_id.is_none() {
                bootstrap.search_offset = 0;
            }
        }
        let safe = scan::bounded_search_offset(bootstrap.search_offset, file_size);
        if safe != bootstrap.search_offset {
            bootstrap.search_offset = safe;
        }
        checkpoint.fork_bootstrap = Some(bootstrap.clone());
        let searched_new = bootstrap.search_offset < file_size;
        let located = find_turn_start_offset(
            file_path,
            &owner_meta.thread_id,
            bootstrap.initial_turn_id.as_deref(),
            bootstrap.recovery_turn_id.as_deref(),
            bootstrap.search_offset,
            file_size,
            Some(rt),
        )
        .await?;
        let Some(start) = located.start_offset else {
            checkpoint.fork_bootstrap = Some(CodexForkBootstrap {
                state: bootstrap.state,
                initial_turn_id: bootstrap.initial_turn_id.clone(),
                recovery_turn_id: bootstrap.recovery_turn_id.clone(),
                search_offset: located.next_offset,
                tail_candidate: None,
            });
            if located.next_offset >= file_size
                && searched_new
                && bootstrap.initial_turn_id.is_none()
                && uuid_v7_timestamp_ms(&owner_meta.thread_id).is_none()
                && bootstrap.recovery_turn_id.is_none()
            {
                let meta = json!({
                    "transcriptPath": file_path,
                    "threadId": owner_meta.thread_id,
                    "searchedBytes": located.next_offset,
                });
                rt.warn(
                    "Codex fork bootstrap has no usable Hook or UUIDv7 ownership evidence",
                    meta.as_object(),
                );
            }
            return Ok(Prepared {
                ready: false,
                changed: true,
            });
        };
        let previous = checkpoint.scan_offset;
        if bootstrap.state == ForkBootstrapState::BaselineSearch && start < previous {
            checkpoint.fork_bootstrap = Some(CodexForkBootstrap {
                state: ForkBootstrapState::BaselineTail,
                initial_turn_id: bootstrap.initial_turn_id.clone(),
                recovery_turn_id: bootstrap.recovery_turn_id.clone(),
                search_offset: start,
                tail_candidate: None,
            });
            let rebuilt = self
                .rebuild_fork_baseline_tail(rt, file_path, checkpoint)
                .await?;
            return Ok(Prepared {
                ready: rebuilt,
                changed: true,
            });
        }
        checkpoint.scan_offset = previous.max(start);
        checkpoint.fork_bootstrap = if previous < start {
            Some(CodexForkBootstrap {
                state: ForkBootstrapState::LivePending,
                initial_turn_id: bootstrap.initial_turn_id.clone(),
                recovery_turn_id: bootstrap.recovery_turn_id.clone(),
                search_offset: start,
                tail_candidate: None,
            })
        } else {
            None
        };
        let start_hook_only = previous < start
            && marker
                .as_ref()
                .and_then(|marker| marker.hook_event.as_deref())
                .is_some_and(|event| event == "user-prompt-submit" || event == "subagent-start");
        Ok(Prepared {
            ready: !start_hook_only,
            changed: true,
        })
    }

    async fn rebuild_fork_baseline_tail(
        &self,
        rt: &InputRuntime,
        file_path: &str,
        checkpoint: &mut CodexTranscriptCheckpoint,
    ) -> anyhow::Result<bool> {
        let Some(bootstrap) = checkpoint.fork_bootstrap.clone() else {
            return Ok(true);
        };
        if bootstrap.state != ForkBootstrapState::BaselineTail {
            return Ok(true);
        }
        let baseline_end = checkpoint.scan_offset;
        let search_offset = scan::bounded_search_offset(bootstrap.search_offset, baseline_end);
        let scan_end = baseline_end.min(search_offset + MAX_SCAN_BYTES_PER_FILE_CYCLE);
        let mut candidate = bootstrap.tail_candidate.clone();
        let mut scan = scan_json_lines(file_path, search_offset, scan_end, Some(rt), |line| {
            apply_tail_line(&mut candidate, &line);
            true
        })
        .await?;
        if scan.next_offset == search_offset && scan_end < baseline_end {
            scan = scan_json_lines(file_path, search_offset, baseline_end, Some(rt), |line| {
                apply_tail_line(&mut candidate, &line);
                false
            })
            .await?;
        }
        if scan.next_offset < baseline_end {
            checkpoint.fork_bootstrap = Some(CodexForkBootstrap {
                search_offset: scan.next_offset,
                tail_candidate: candidate,
                ..bootstrap
            });
            return Ok(false);
        }
        if let Some(mut candidate) = candidate {
            candidate.start_offset = baseline_end;
            checkpoint.active_turn = Some(candidate);
        }
        checkpoint.fork_bootstrap = None;
        Ok(true)
    }

    async fn resolve_input_context(
        &self,
        rt: &InputRuntime,
        file_path: &str,
        checkpoint: &CodexTranscriptCheckpoint,
        meta: Option<&CodexTranscriptMeta>,
    ) -> anyhow::Result<Option<CodexTranscriptInputContext>> {
        let Some(active) = checkpoint.active_turn.as_ref() else {
            return Ok(None);
        };
        let Some(context) = active.input_context.clone() else {
            return Ok(None);
        };
        if context.delta.is_some() {
            return Ok(Some(context));
        }
        let Some(range) = context.delta_range.clone() else {
            return Ok(Some(context));
        };
        let records =
            read_json_lines(file_path, range.start_offset, range.end_offset, Some(rt)).await?;
        let maps: Vec<Map<String, Value>> =
            records.items.into_iter().map(|line| line.record).collect();
        let previous = extract_codex_partial_turn(
            &maps,
            meta,
            &session_id_from_transcript_path(file_path),
            &active.turn_id,
            &ExtractOptions {
                require_terminal: false,
                started_at_ms: Some(active.started_at_ms),
                next_step_started_at_ms: active.next_step_started_at_ms,
                last_cumulative_token_total: active.last_cumulative_token_total,
                model: active.model.clone(),
                cwd: active.cwd.clone(),
                developer_instructions: active.developer_instructions.clone(),
                blob_to_uri: None,
                upload_mode: None,
            },
        );
        let Some(last) = previous.as_ref().and_then(|turn| turn.steps.last()) else {
            let meta = json!({ "transcriptPath": file_path, "turnId": active.turn_id });
            rt.warn(
                "could not rebuild oversized Codex input delta from transcript range",
                meta.as_object(),
            );
            return Ok(Some(context));
        };
        Ok(Some(CodexTranscriptInputContext {
            delta: Some(next_input_messages_for_step(last)),
            ..context
        }))
    }
}

fn apply_tail_line(candidate: &mut Option<CodexActiveTranscriptTurn>, line: &JsonLine) {
    let Some(payload) = line.record.get("payload").and_then(Value::as_object) else {
        return;
    };
    if line.record.get("type").and_then(Value::as_str) == Some("session_meta") {
        return;
    }
    if let Some(turn_id) = turn_id_for_start(&line.record, payload) {
        if candidate
            .as_ref()
            .is_none_or(|current| current.turn_id != turn_id)
        {
            *candidate = Some(create_active_turn(
                turn_id.clone(),
                line.start_offset,
                utils::timestamp_ms(&line.record, now_ms()),
                true,
            ));
        }
        if let Some(current) = candidate.as_mut() {
            update_active_turn_metadata(current, &line.record, payload);
        }
        return;
    }
    if terminal_turn_id_for(&line.record, payload).as_deref()
        == candidate.as_ref().map(|item| item.turn_id.as_str())
    {
        *candidate = None;
    }
}

struct WakeupMarker {
    initial_turn_id: Option<String>,
    recovery_turn_id: Option<String>,
    hook_event: Option<String>,
}

impl<'a> CodexTranscriptEngine<'a> {
    async fn read_wakeup_marker(
        &self,
        session_id: &str,
        transcript_path: &str,
    ) -> Option<WakeupMarker> {
        let path = Path::new(&self.body.wakeup_dir)
            .join(format!("{}.json", safe_wakeup_session_part(session_id)));
        let raw = tokio::fs::read_to_string(&path).await.ok()?;
        let parsed: Value = serde_json::from_str(&raw).ok()?;
        let record = as_record(&parsed)?;
        if string_value(record.get("session_id")) != Some(session_id) {
            return None;
        }
        let _ = transcript_path;
        Some(WakeupMarker {
            initial_turn_id: string_value(record.get("initial_turn_id")).map(str::to_owned),
            recovery_turn_id: string_value(record.get("recovery_turn_id")).map(str::to_owned),
            hook_event: string_value(record.get("hook_event")).map(str::to_owned),
        })
    }

    async fn read_wakeup_resource_attributes(
        &self,
        rt: &InputRuntime,
        session_id: &str,
    ) -> Option<Map<String, Value>> {
        let marker = Path::new(&self.body.wakeup_dir)
            .join(format!("{}.json", safe_wakeup_session_part(session_id)));
        let raw = tokio::fs::read_to_string(&marker).await.ok()?;
        let parsed = match serde_json::from_str::<Value>(&raw) {
            Ok(value) => value,
            Err(_) => {
                let meta = json!({ "marker": marker.to_string_lossy() });
                rt.debug(
                    "Codex wakeup marker could not be parsed; resource attributes skipped",
                    meta.as_object(),
                );
                return None;
            }
        };
        let record = as_record(&parsed)?;
        let Some(attributes) = record.get("resourceAttributes").and_then(as_record) else {
            let meta = json!({ "marker": marker.to_string_lossy() });
            rt.debug(
                "Codex wakeup marker has no resourceAttributes; attribution skipped",
                meta.as_object(),
            );
            return None;
        };
        let mut out = Map::new();
        for key in WAKEUP_RESOURCE_ATTRIBUTE_KEYS {
            let Some(value) = attributes.get(*key).and_then(Value::as_str) else {
                continue;
            };
            let trimmed = value.trim();
            if trimmed.len() > MAX_WAKEUP_RESOURCE_ATTRIBUTE_VALUE_LENGTH || trimmed.is_empty() {
                continue;
            }
            out.insert((*key).to_owned(), Value::String(trimmed.to_owned()));
        }
        if out.is_empty() {
            None
        } else {
            Some(out)
        }
    }

    async fn read_turn_span_attributes(
        &self,
        rt: &InputRuntime,
        session_id: &str,
        turn_id: &str,
    ) -> Option<HashMap<String, String>> {
        let marker = Path::new(&self.body.span_context_dir)
            .join(span_context_marker_name(session_id, turn_id));
        let raw = tokio::fs::read_to_string(&marker).await.ok()?;
        let parsed = serde_json::from_str::<Value>(&raw).ok()?;
        let record = as_record(&parsed)?;
        if string_value(record.get("session_id")) != Some(session_id)
            || string_value(record.get("turn_id")) != Some(turn_id)
        {
            let meta = json!({ "marker": marker.to_string_lossy(), "sessionId": session_id, "turnId": turn_id });
            rt.debug(
                "Codex span context identifiers do not match the transcript turn",
                meta.as_object(),
            );
            return None;
        }
        if let Some(received) =
            string_value(record.get("received_at")).and_then(utils::parse_timestamp_ms)
        {
            if (now_ms() as i64) - received as i64 > SPAN_CONTEXT_TTL_MS {
                return None;
            }
        }
        let attributes = record.get("spanAttributes").and_then(as_record)?;
        let mut out = HashMap::new();
        for (raw_key, raw_value) in attributes {
            let key = raw_key.trim();
            if key.is_empty()
                || is_reserved_key(key)
                || SENSITIVE_SPAN_ATTRIBUTE_NAME_RE.is_match(key)
                || !raw_value.is_string()
            {
                continue;
            }
            let value = raw_value.as_str().unwrap_or("").trim();
            if value.is_empty() || value.len() > MAX_SPAN_ATTRIBUTE_VALUE_LENGTH {
                continue;
            }
            out.insert(key.to_owned(), value.to_owned());
        }
        if out.is_empty() {
            None
        } else {
            Some(out)
        }
    }

    pub async fn cleanup_expired_span_contexts(&self, _rt: &InputRuntime, now: i64) {
        let due = self.body.with_shared(|shared| {
            if now - shared.last_span_cleanup_at < SPAN_CONTEXT_CLEANUP_INTERVAL_MS {
                false
            } else {
                shared.last_span_cleanup_at = now;
                true
            }
        });
        if !due {
            return;
        }
        let mut entries = match tokio::fs::read_dir(&self.body.span_context_dir).await {
            Ok(entries) => entries,
            Err(_) => return,
        };
        let mut names = Vec::new();
        while let Ok(Some(entry)) = entries.next_entry().await {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.ends_with(".json") {
                names.push(name);
            }
        }
        names.sort();
        names.truncate(MAX_SPAN_CONTEXTS_PER_CLEANUP);
        for name in names {
            let path = Path::new(&self.body.span_context_dir).join(&name);
            if marker_expired(&path, now, SPAN_CONTEXT_TTL_MS).await {
                let _ = tokio::fs::remove_file(path).await;
            }
        }
    }

    async fn discover_dynamic_session_dirs(&self, canonical_default: &str) -> Vec<String> {
        let mut entries = match tokio::fs::read_dir(&self.body.wakeup_dir).await {
            Ok(entries) => entries,
            Err(_) => return Vec::new(),
        };
        let mut names = Vec::new();
        while let Ok(Some(entry)) = entries.next_entry().await {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.ends_with(".json") {
                names.push(name);
            }
        }
        let now = now_ms() as i64;
        self.cleanup_wakeup_markers(&names, now).await;
        names.sort_by(|left, right| right.cmp(left));
        names.truncate(MAX_WAKEUP_MARKERS_FOR_DISCOVERY);
        let mut tiers: [IndexMap<String, ()>; 3] =
            [IndexMap::new(), IndexMap::new(), IndexMap::new()];
        for name in names {
            let roots = read_codex_wakeup_roots(
                &Path::new(&self.body.wakeup_dir)
                    .join(name)
                    .to_string_lossy(),
                now,
            )
            .await;
            for (tier, candidates) in roots.into_iter().enumerate() {
                for candidate in candidates {
                    tiers[tier].insert(candidate, ());
                }
            }
        }
        let mut session_dirs = IndexMap::new();
        for tier in tiers {
            for candidate in tier.into_keys() {
                let Ok(canonical) = tokio::fs::canonicalize(&candidate).await else {
                    continue;
                };
                let canonical = canonical.to_string_lossy().into_owned();
                if canonical == canonical_default {
                    continue;
                }
                if tokio::fs::metadata(&canonical)
                    .await
                    .ok()
                    .is_some_and(|meta| meta.is_dir())
                {
                    session_dirs.insert(canonical, ());
                }
            }
        }
        let dropped = session_dirs.len().saturating_sub(MAX_DYNAMIC_SESSION_DIRS);
        if dropped > 0 {
            let warn = self.body.with_shared(|shared| {
                if now - shared.last_dynamic_warn_at >= WAKEUP_MARKER_CLEANUP_INTERVAL_MS {
                    shared.last_dynamic_warn_at = now;
                    true
                } else {
                    false
                }
            });
            if warn {
                // The runtime logger is not available here; discovery still caps the set.
                let _ = dropped;
            }
        }
        session_dirs
            .into_keys()
            .take(MAX_DYNAMIC_SESSION_DIRS)
            .collect()
    }

    async fn cleanup_wakeup_markers(&self, names: &[String], now: i64) {
        let due = self.body.with_shared(|shared| {
            if now - shared.last_wakeup_cleanup_at < WAKEUP_MARKER_CLEANUP_INTERVAL_MS {
                false
            } else {
                shared.last_wakeup_cleanup_at = now;
                true
            }
        });
        if !due {
            return;
        }
        let mut ordered = names.to_vec();
        ordered.sort();
        ordered.truncate(MAX_WAKEUP_MARKERS_PER_CLEANUP);
        for name in ordered {
            let path = Path::new(&self.body.wakeup_dir).join(name);
            if marker_expired(&path, now, WAKEUP_MARKER_DISCOVERY_TTL_MS).await {
                let _ = tokio::fs::remove_file(path).await;
            }
        }
    }
}

async fn marker_expired(path: &Path, now: i64, ttl: i64) -> bool {
    if let Ok(raw) = tokio::fs::read_to_string(path).await {
        if let Ok(parsed) = serde_json::from_str::<Value>(&raw) {
            if let Some(record) = as_record(&parsed) {
                if let Some(received) =
                    string_value(record.get("received_at")).and_then(utils::parse_timestamp_ms)
                {
                    return now - received as i64 > ttl;
                }
            }
        }
    }
    tokio::fs::metadata(path)
        .await
        .ok()
        .map(|meta| now - file_meta_snapshot(&meta).2 > ttl)
        .unwrap_or(false)
}

impl<'a> CodexTranscriptEngine<'a> {
    pub async fn index_discovered_transcript_owners(
        &self,
        rt: &InputRuntime,
        files: &[DiscoveredSessionFile],
        skip: Option<&HashSet<String>>,
    ) -> anyhow::Result<()> {
        let kept = self.body.with_shared(|shared| {
            let mut kept = Vec::new();
            if let Some(skip) = skip {
                for path in skip {
                    if let Some(meta) = shared.meta_by_path.get(path).cloned() {
                        kept.push((path.clone(), meta));
                    }
                }
            }
            shared.meta_by_path.clear();
            shared.path_by_thread.clear();
            kept
        });
        let discovered: HashSet<String> = files.iter().map(|file| file.file_path.clone()).collect();
        self.prune_absent_meta_cache(&discovered, now_ms() as i64);
        for (path, meta) in kept {
            self.publish_transcript_owner_meta(&path, meta);
        }
        for file in files {
            if skip.is_some_and(|skip| skip.contains(&file.file_path)) {
                continue;
            }
            let Ok(meta_fs) = tokio::fs::metadata(&file.file_path).await else {
                continue;
            };
            let (inode, size, _) = file_meta_snapshot(&meta_fs);
            let checkpoint = self.read_checkpoint(rt, &self.state_key(&file.file_path));
            let owner = if checkpoint.as_ref().is_some_and(|item| item.inode == inode) {
                if let Some(offset) = checkpoint.and_then(|item| item.owner_session_meta_offset) {
                    Some(offset)
                } else {
                    find_owner_session_meta_offset(&file.file_path, size, Some(rt)).await?
                }
            } else {
                find_owner_session_meta_offset(&file.file_path, size, Some(rt)).await?
            };
            let Some(owner) = owner else {
                continue;
            };
            if let Some(meta) = self
                .load_transcript_owner_meta(rt, &file.file_path, inode, size, owner, false)
                .await?
            {
                self.publish_transcript_owner_meta(&file.file_path, meta);
            }
        }
        Ok(())
    }

    async fn register_subagent_owner_meta(
        &self,
        rt: &InputRuntime,
        file_path: &str,
        inode: u64,
        file_size: i64,
        owner_offset: Option<i64>,
        active: bool,
    ) -> anyhow::Result<()> {
        let Some(owner_offset) = owner_offset else {
            return Ok(());
        };
        let indexed = if active {
            let missing = self
                .body
                .with_shared(|shared| !shared.meta_cache.contains_key(file_path));
            if missing {
                self.body
                    .with_shared(|shared| shared.meta_by_path.get(file_path).cloned())
            } else {
                None
            }
        } else {
            None
        };
        if let Some(meta) = indexed {
            self.remember_transcript_owner_meta(
                file_path,
                inode,
                file_size,
                owner_offset,
                meta.clone(),
                true,
            );
            self.publish_transcript_owner_meta(file_path, meta);
            return Ok(());
        }
        if let Some(meta) = self
            .load_transcript_owner_meta(rt, file_path, inode, file_size, owner_offset, active)
            .await?
        {
            self.publish_transcript_owner_meta(file_path, meta);
        }
        Ok(())
    }

    async fn load_transcript_owner_meta(
        &self,
        rt: &InputRuntime,
        file_path: &str,
        inode: u64,
        file_size: i64,
        owner_offset: i64,
        active: bool,
    ) -> anyhow::Result<Option<CodexTranscriptMeta>> {
        let cached = self.body.with_shared(|shared| {
            shared.meta_cache.get(file_path).and_then(|cached| {
                if reusable_cached(cached, inode, file_size, owner_offset) {
                    Some(cached.meta.clone())
                } else {
                    None
                }
            })
        });
        if let Some(meta) = cached {
            self.body.with_shared(|shared| {
                if let Some(mut cached) = shared.meta_cache.shift_remove(file_path) {
                    cached.file_size = file_size;
                    cached.last_seen_at_ms = now_ms() as i64;
                    shared.meta_cache.insert(file_path.to_owned(), cached);
                }
            });
            return Ok(Some(meta));
        }
        self.remove_meta_cache(file_path);
        if owner_offset < 0 || owner_offset >= file_size {
            return Ok(None);
        }
        let Some(record) = read_json_line_at(file_path, owner_offset, Some(rt)).await? else {
            return Ok(None);
        };
        let Some(meta) = extract_codex_transcript_meta(&record) else {
            return Ok(None);
        };
        self.remember_transcript_owner_meta(
            file_path,
            inode,
            file_size,
            owner_offset,
            meta.clone(),
            active,
        );
        Ok(Some(meta))
    }

    fn remember_transcript_owner_meta(
        &self,
        file_path: &str,
        inode: u64,
        file_size: i64,
        owner_offset: i64,
        meta: CodexTranscriptMeta,
        active: bool,
    ) {
        self.remove_meta_cache(file_path);
        let estimated = file_path.len()
            + serde_json::to_vec(&meta)
                .map(|bytes| bytes.len())
                .unwrap_or(0);
        if estimated > MAX_CACHED_TRANSCRIPT_META_BYTES {
            return;
        }
        let blocked = self.body.with_shared(|shared| {
            !active
                && (shared.meta_cache.len() >= MAX_LINK_DESCRIPTORS
                    || shared.meta_cache_bytes + estimated > MAX_CACHED_TRANSCRIPT_META_BYTES)
        });
        if blocked {
            return;
        }
        self.body.with_shared(|shared| {
            shared.meta_cache.insert(
                file_path.to_owned(),
                CachedMeta {
                    inode,
                    file_size,
                    owner_offset,
                    meta,
                    estimated_bytes: estimated,
                    last_seen_at_ms: now_ms() as i64,
                },
            );
            shared.meta_cache_bytes += estimated;
            while shared.meta_cache.len() > MAX_LINK_DESCRIPTORS
                || shared.meta_cache_bytes > MAX_CACHED_TRANSCRIPT_META_BYTES
            {
                if let Some((_, cached)) = shared.meta_cache.shift_remove_index(0) {
                    shared.meta_cache_bytes = shared
                        .meta_cache_bytes
                        .saturating_sub(cached.estimated_bytes);
                } else {
                    break;
                }
            }
        });
    }

    fn cached_meta_for_checkpoint(
        &self,
        file_path: &str,
        checkpoint: &CodexTranscriptCheckpoint,
        file_size: i64,
    ) -> Option<CodexTranscriptMeta> {
        let owner = checkpoint.owner_session_meta_offset?;
        self.body.with_shared(|shared| {
            let cached = shared.meta_cache.shift_remove(file_path)?;
            if !reusable_cached(&cached, checkpoint.inode, file_size, owner) {
                shared.meta_cache.insert(file_path.to_owned(), cached);
                return None;
            }
            let meta = cached.meta.clone();
            let mut cached = cached;
            cached.file_size = file_size;
            cached.last_seen_at_ms = now_ms() as i64;
            shared.meta_cache.insert(file_path.to_owned(), cached);
            Some(meta)
        })
    }

    fn prune_absent_meta_cache(&self, discovered: &HashSet<String>, now: i64) {
        self.body.with_shared(|shared| {
            let stale: Vec<String> = shared
                .meta_cache
                .iter()
                .filter(|(path, cached)| {
                    !discovered.contains(*path)
                        && now - cached.last_seen_at_ms >= CACHED_TRANSCRIPT_META_ABSENCE_TTL_MS
                })
                .map(|(path, _)| path.clone())
                .collect();
            for path in stale {
                if let Some(cached) = shared.meta_cache.shift_remove(&path) {
                    shared.meta_cache_bytes = shared
                        .meta_cache_bytes
                        .saturating_sub(cached.estimated_bytes);
                }
            }
        });
    }

    fn remove_meta_cache(&self, file_path: &str) {
        self.body.with_shared(|shared| {
            if let Some(cached) = shared.meta_cache.shift_remove(file_path) {
                shared.meta_cache_bytes = shared
                    .meta_cache_bytes
                    .saturating_sub(cached.estimated_bytes);
            }
        });
    }

    fn publish_transcript_owner_meta(&self, file_path: &str, meta: CodexTranscriptMeta) {
        self.body.with_shared(|shared| {
            if let Some(previous) = shared.meta_by_path.get(file_path) {
                if previous.thread_id != meta.thread_id
                    && shared
                        .path_by_thread
                        .get(&previous.thread_id)
                        .map(String::as_str)
                        == Some(file_path)
                {
                    shared.path_by_thread.shift_remove(&previous.thread_id);
                }
            }
            shared
                .path_by_thread
                .insert(meta.thread_id.clone(), file_path.to_owned());
            shared.linker.register_child(&meta);
            shared.meta_by_path.insert(file_path.to_owned(), meta);
        });
    }

    fn register_persisted_subagent_spawns(
        &self,
        checkpoint: &CodexTranscriptCheckpoint,
        parent_thread_id: &str,
    ) {
        let Some(active) = checkpoint.active_turn.as_ref() else {
            return;
        };
        if active.subagent_spawns.is_empty() {
            return;
        }
        let spawns = active
            .subagent_spawns
            .iter()
            .map(|spawn| CodexSpawnDescriptor {
                parent_thread_id: parent_thread_id.to_owned(),
                parent_turn_id: active.turn_id.clone(),
                parent_trace_id: spawn.parent_trace_id.clone(),
                parent_tool_call_id: spawn.parent_tool_call_id.clone(),
                spawned_at_ms: spawn.spawned_at_ms,
                task_name: spawn.task_name.clone(),
                agent_path: spawn.agent_path.clone(),
                child_thread_id: spawn.child_thread_id.clone(),
            })
            .collect::<Vec<_>>();
        self.body
            .with_shared(|shared| shared.linker.register_spawns(&spawns));
    }

    fn reliable_candidate_for_child(
        &self,
        rt: &InputRuntime,
        meta: &CodexTranscriptMeta,
    ) -> Option<super::super::subagent_linker::CodexSubagentLink> {
        let parent = meta.parent_thread_id.as_deref()?;
        let snapshot = self.body.with_shared(|shared| shared.linker.snapshot());
        let link = snapshot.links.into_iter().find(|candidate| {
            candidate.child_thread_id == meta.thread_id
                && candidate.parent_thread_id == parent
                && candidate.parent_turn_id.is_some()
                && candidate.parent_trace_id.is_some()
                && candidate.parent_tool_call_id.is_some()
                && matches!(
                    candidate.confidence,
                    CodexSubagentLinkConfidence::ExplicitId
                        | CodexSubagentLinkConfidence::AgentPath
                )
        })?;
        let parents = self.body.with_shared(|shared| {
            shared
                .meta_by_path
                .iter()
                .filter(|(_, parent_meta)| {
                    parent_meta.thread_id == link.parent_thread_id && parent_meta.depth == 0
                })
                .map(|(path, _)| path.clone())
                .collect::<Vec<_>>()
        });
        for path in parents {
            let Some(checkpoint) = self.read_checkpoint(rt, &self.state_key(&path)) else {
                continue;
            };
            let parent_turn = checkpoint
                .pending_fusion
                .as_ref()
                .map(|fusion| fusion.turn_id.clone())
                .or_else(|| {
                    checkpoint
                        .active_turn
                        .as_ref()
                        .map(|active| active.turn_id.clone())
                });
            let spawns = checkpoint
                .pending_fusion
                .as_ref()
                .map(|fusion| fusion.children.clone())
                .or_else(|| {
                    checkpoint
                        .active_turn
                        .as_ref()
                        .map(|active| active.subagent_spawns.clone())
                })
                .unwrap_or_default();
            if parent_turn.as_deref() == link.parent_turn_id.as_deref()
                && spawns.iter().any(|spawn| {
                    Some(spawn.parent_tool_call_id.as_str()) == link.parent_tool_call_id.as_deref()
                })
            {
                return Some(link);
            }
        }
        None
    }

    fn reliable_fusion_children(
        &self,
        rt: &InputRuntime,
        parent_thread_id: &str,
        parent_turn_id: &str,
        children: &[CodexPendingFusionChild],
    ) -> Vec<CodexPendingFusionChild> {
        let links = self
            .body
            .with_shared(|shared| shared.linker.snapshot().links);
        let mut reliable = Vec::new();
        for child in children {
            let matches: Vec<_> = links
                .iter()
                .filter(|link| {
                    link.parent_thread_id == parent_thread_id
                        && link.parent_turn_id.as_deref() == Some(parent_turn_id)
                        && link.parent_tool_call_id.as_deref()
                            == Some(child.parent_tool_call_id.as_str())
                        && matches!(
                            link.confidence,
                            CodexSubagentLinkConfidence::ExplicitId
                                | CodexSubagentLinkConfidence::AgentPath
                        )
                })
                .collect();
            if matches.len() != 1 {
                continue;
            }
            let link = matches[0];
            let child_path = self
                .body
                .with_shared(|shared| shared.path_by_thread.get(&link.child_thread_id).cloned());
            let checkpoint = child_path
                .as_ref()
                .and_then(|path| self.read_checkpoint(rt, &self.state_key(path)));
            if let Some(checkpoint) = checkpoint.as_ref() {
                if let Some(captured) = checkpoint.pending_subagent.as_ref() {
                    if captured.parent_tool_call_id != child.parent_tool_call_id {
                        continue;
                    }
                } else if child_collection_settled(checkpoint) {
                    continue;
                }
            }
            let mut next = child.clone();
            next.child_thread_id = Some(link.child_thread_id.clone());
            reliable.push(next);
        }
        reliable
    }

    async fn release_parent_fusion_independently(
        &self,
        rt: &InputRuntime,
        file_path: &str,
        checkpoint: &mut CodexTranscriptCheckpoint,
    ) -> anyhow::Result<(usize, usize)> {
        let Some(pending) = checkpoint.pending_fusion.clone() else {
            return Ok((0, 0));
        };
        checkpoint.scan_offset = checkpoint.scan_offset.max(pending.terminal_end_offset);
        checkpoint.pending_fusion = None;
        let meta = json!({ "transcriptPath": file_path, "turnId": pending.turn_id });
        rt.warn(
            "Codex parent fusion has no reliable child candidates; emitting independently",
            meta.as_object(),
        );
        if checkpoint.active_turn.is_none() {
            return Ok((0, 0));
        }
        let recovered = self
            .recover_turn_segment(
                rt,
                file_path,
                checkpoint,
                RecoveryBoundary {
                    end_offset: pending.terminal_end_offset,
                    terminal: true,
                    inferred: None,
                },
                None,
            )
            .await?;
        if matches!(recovered.kind, RecoveryKind::Unparseable) {
            checkpoint.pending_terminal = Some(new_pending_terminal(
                pending.turn_id,
                pending.terminal_end_offset,
                recovered.diagnostics.source_record_count as u64,
            ));
            return Ok((0, 0));
        }
        let emitted = self.body.emit_batches(recovered.entries);
        checkpoint.active_turn = None;
        checkpoint.pending_terminal = None;
        Ok((emitted, 1))
    }

    async fn release_captured_subagent_independently(
        &self,
        rt: &InputRuntime,
        file_path: &str,
        checkpoint: &mut CodexTranscriptCheckpoint,
    ) -> anyhow::Result<(usize, usize)> {
        let Some(captured) = checkpoint.pending_subagent.clone() else {
            return Ok((0, 0));
        };
        let mut recovery = checkpoint.clone();
        recovery.active_turn = Some(captured.active_turn.clone());
        recovery.pending_subagent = None;
        let recovered = self
            .recover_turn_segment(
                rt,
                file_path,
                &mut recovery,
                RecoveryBoundary {
                    end_offset: captured.terminal_end_offset,
                    terminal: true,
                    inferred: None,
                },
                None,
            )
            .await?;
        if matches!(recovered.kind, RecoveryKind::Unparseable) {
            let meta = json!({
                "transcriptPath": file_path,
                "turnId": captured.turn_id,
                "parentToolCallId": captured.parent_tool_call_id,
            });
            rt.warn(
                "captured Codex child could not be rebuilt for independent fallback",
                meta.as_object(),
            );
            return Ok((0, 0));
        }
        let emitted = self.body.emit_batches(recovered.entries);
        checkpoint.pending_subagent = None;
        Ok((emitted, 1))
    }

    pub async fn finalize_ready_subagent_fusions(
        &self,
        rt: &InputRuntime,
    ) -> anyhow::Result<usize> {
        let parents = self.body.with_shared(|shared| {
            shared
                .meta_by_path
                .iter()
                .filter(|(_, meta)| meta.depth == 0)
                .map(|(path, _)| path.clone())
                .collect::<Vec<_>>()
        });
        let mut emitted = 0usize;
        for parent_path in parents {
            let parent_key = self.state_key(&parent_path);
            let Some(mut parent_checkpoint) = self.read_checkpoint(rt, &parent_key) else {
                continue;
            };
            let Some(pending) = parent_checkpoint.pending_fusion.clone() else {
                continue;
            };
            if parent_checkpoint.active_turn.is_none() {
                continue;
            }
            let mut degraded = Vec::new();
            let mut child_outputs = Vec::new();
            for child in &pending.children {
                let child_thread = child.child_thread_id.clone();
                let child_path = child_thread.as_ref().and_then(|id| {
                    self.body
                        .with_shared(|shared| shared.path_by_thread.get(id).cloned())
                });
                let Some(child_path) = child_path else {
                    degraded.push(child.parent_tool_call_id.clone());
                    continue;
                };
                let Some(child_checkpoint) = self.read_checkpoint(rt, &self.state_key(&child_path))
                else {
                    degraded.push(child.parent_tool_call_id.clone());
                    continue;
                };
                let captured = child_checkpoint.pending_subagent.clone();
                let captured_matches = captured.as_ref().is_some_and(|captured| {
                    captured.parent_thread_id == pending.parent_thread_id
                        && captured.parent_turn_id == pending.turn_id
                        && captured.parent_trace_id == pending.parent_trace_id
                        && captured.parent_tool_call_id == child.parent_tool_call_id
                });
                let can_force = !captured_matches
                    && child_checkpoint.active_turn.as_ref().is_some_and(|active| {
                        !has_emitted_active_turn_state(active)
                            && child_checkpoint.scan_offset > active.start_offset
                    });
                if !captured_matches && !can_force {
                    degraded.push(child.parent_tool_call_id.clone());
                    continue;
                }
                let snapshot = if captured_matches {
                    captured.as_ref().map(|item| item.active_turn.clone())
                } else {
                    child_checkpoint.active_turn.clone()
                };
                let Some(snapshot) = snapshot else {
                    degraded.push(child.parent_tool_call_id.clone());
                    continue;
                };
                let terminal_end = if captured_matches {
                    captured
                        .as_ref()
                        .map(|item| item.terminal_end_offset)
                        .unwrap_or(child_checkpoint.scan_offset)
                } else {
                    child_checkpoint.scan_offset
                };
                let mut recovery = child_checkpoint.clone();
                recovery.active_turn = Some(snapshot.clone());
                recovery.pending_subagent = None;
                let recovered = self
                    .recover_turn_segment(
                        rt,
                        &child_path,
                        &mut recovery,
                        RecoveryBoundary {
                            end_offset: terminal_end,
                            terminal: true,
                            inferred: if captured_matches {
                                None
                            } else {
                                Some(CodexTerminalStatus::Interrupted)
                            },
                        },
                        Some(pending.parent_thread_id.as_str()),
                    )
                    .await?;
                if matches!(recovered.kind, RecoveryKind::Unparseable) {
                    degraded.push(child.parent_tool_call_id.clone());
                    continue;
                }
                let agent_name = self.body.with_shared(|shared| {
                    shared
                        .meta_by_path
                        .get(&child_path)
                        .and_then(|meta| meta.agent_nickname.clone())
                });
                let rewritten = rewrite_subagent_entries(
                    recovered.entries,
                    &pending.parent_thread_id,
                    &pending.turn_id,
                    &pending.parent_trace_id,
                    &child.parent_tool_call_id,
                    child.child_thread_id.as_deref().unwrap_or(""),
                    agent_name
                        .or_else(|| child.agent_path.clone())
                        .or_else(|| child.task_name.clone()),
                );
                child_outputs.push((
                    rewritten,
                    child_path,
                    child_checkpoint,
                    snapshot.turn_id,
                    child.parent_tool_call_id.clone(),
                    captured_matches,
                ));
            }
            let parent_recovered = self
                .recover_turn_segment(
                    rt,
                    &parent_path,
                    &mut parent_checkpoint,
                    RecoveryBoundary {
                        end_offset: pending.terminal_end_offset,
                        terminal: true,
                        inferred: None,
                    },
                    None,
                )
                .await?;
            if matches!(parent_recovered.kind, RecoveryKind::Unparseable) {
                continue;
            }
            if !degraded.is_empty() {
                let meta = json!({
                    "transcriptPath": parent_path,
                    "turnId": pending.turn_id,
                    "parentToolCallIds": degraded,
                });
                rt.warn(
                    "Codex parent reached task_complete before every child could be rebuilt; degrading missing children",
                    meta.as_object(),
                );
            }
            for (entries, path, mut child_checkpoint, turn_id, tool_id, captured_matches) in
                child_outputs
            {
                emitted += self.body.emit_batches(entries);
                if captured_matches
                    && child_checkpoint
                        .pending_subagent
                        .as_ref()
                        .is_some_and(|item| item.parent_tool_call_id == tool_id)
                {
                    child_checkpoint.pending_subagent = None;
                } else if !captured_matches
                    && child_checkpoint
                        .active_turn
                        .as_ref()
                        .is_some_and(|active| active.turn_id == turn_id)
                {
                    child_checkpoint.active_turn = None;
                    child_checkpoint.pending_terminal = None;
                }
                self.save_checkpoint(rt, &self.state_key(&path), &child_checkpoint);
            }
            emitted += self.body.emit_batches(parent_recovered.entries);
            parent_checkpoint.active_turn = None;
            parent_checkpoint.pending_fusion = None;
            self.save_checkpoint(rt, &parent_key, &parent_checkpoint);
        }
        Ok(emitted)
    }

    pub fn report_subagent_links(&self, rt: &InputRuntime) {
        let snapshot = self.body.with_shared(|shared| shared.linker.snapshot());
        if snapshot.detected_children == 0 && snapshot.detected_spawns == 0 {
            return;
        }
        let summary = format!(
            "{}:{}:{}:{}",
            snapshot.detected_children,
            snapshot.detected_spawns,
            snapshot.linked_children,
            snapshot.orphan_children
        );
        let changed = self.body.with_shared(|shared| {
            if shared.last_summary == summary {
                false
            } else {
                shared.last_summary = summary;
                true
            }
        });
        if changed {
            let meta = json!({
                "detectedChildren": snapshot.detected_children,
                "detectedSpawns": snapshot.detected_spawns,
                "linkedChildren": snapshot.linked_children,
                "orphanChildren": snapshot.orphan_children,
            });
            rt.info("Codex subagent linker summary", meta.as_object());
        }
        for link in snapshot.links {
            let fingerprint = format!(
                "{}:{}:{}",
                link.confidence.as_str(),
                link.parent_tool_call_id.as_deref().unwrap_or(""),
                link.orphan_reason
                    .map(|reason| reason.as_str())
                    .unwrap_or("")
            );
            let fresh = self.body.with_shared(|shared| {
                if shared
                    .reported
                    .get(&link.child_thread_id)
                    .map(String::as_str)
                    == Some(fingerprint.as_str())
                {
                    return false;
                }
                shared.reported.shift_remove(&link.child_thread_id);
                shared
                    .reported
                    .insert(link.child_thread_id.clone(), fingerprint);
                while shared.reported.len() > MAX_LINK_DESCRIPTORS {
                    shared.reported.shift_remove_index(0);
                }
                true
            });
            if fresh {
                let meta = json!({
                    "childThreadId": link.child_thread_id,
                    "parentThreadId": link.parent_thread_id,
                    "parentTurnId": link.parent_turn_id,
                    "parentToolCallId": link.parent_tool_call_id,
                    "agentPath": link.agent_path,
                    "confidence": link.confidence.as_str(),
                    "orphanReason": link.orphan_reason.map(|reason| reason.as_str()),
                });
                rt.debug("Codex subagent link resolved", meta.as_object());
            }
        }
    }
}

fn child_collection_settled(checkpoint: &CodexTranscriptCheckpoint) -> bool {
    checkpoint.scan_offset > 0
        && checkpoint.fork_bootstrap.is_none()
        && checkpoint.active_turn.is_none()
        && checkpoint.pending_terminal.is_none()
        && checkpoint.pending_subagent.is_none()
}

fn reusable_cached(cached: &CachedMeta, inode: u64, file_size: i64, owner_offset: i64) -> bool {
    cached.inode == inode
        && cached.owner_offset == owner_offset
        && file_size >= cached.file_size
        && owner_offset >= 0
        && owner_offset < file_size
}

fn rewrite_subagent_entries(
    entries: Vec<AgentActivityEntry>,
    parent_thread_id: &str,
    parent_turn_id: &str,
    parent_trace_id: &str,
    parent_tool_call_id: &str,
    child_thread_id: &str,
    agent_name: Option<String>,
) -> Vec<AgentActivityEntry> {
    let parent_turn = format!("{parent_thread_id}:{parent_turn_id}");
    entries
        .into_iter()
        .filter(|entry| entry.event_name != Some(AgentEventName::Other))
        .map(|mut entry| {
            entry.trace_id = Some(parent_trace_id.to_owned());
            entry.session_id = parent_thread_id.to_owned();
            entry.turn_id = Some(parent_turn.clone());
            entry.agent_id = Some(child_thread_id.to_owned());
            if let Some(name) = &agent_name {
                entry.agent_name = Some(name.clone());
            }
            entry
                .extra
                .insert("gen_ai.agent.scope".into(), json!("subagent"));
            entry.extra.insert("gen_ai.agent.depth".into(), json!(1));
            entry
                .extra
                .insert("gen_ai.agent.parent.id".into(), json!(parent_thread_id));
            entry.extra.insert(
                "gen_ai.subagent.parent_tool_call.id".into(),
                json!(parent_tool_call_id),
            );
            entry
        })
        .collect()
}
