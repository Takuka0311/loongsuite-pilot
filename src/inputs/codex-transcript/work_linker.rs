//! Recoverable per-work lineage. Plaintext work and result bodies never enter
//! this state: work uses a normalized digest, result equality an exact digest.
use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use super::*;
use crate::inputs::codex_transcript::types::CodexTerminalStatus;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct WorkBinding {
    child_thread_id: String,
    child_turn_id: String,
    invocation_id: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct CompletedResult {
    invocation_id: String,
    child_thread_id: String,
    child_turn_id: String,
    fingerprint: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct WorkTerminal {
    invocation_id: String,
    child_thread_id: String,
    child_turn_id: String,
    terminal_type: String,
}

fn digest(value: &str, length: usize) -> String {
    hex::encode(Sha256::digest(value.as_bytes()))[..length].to_owned()
}
fn work_fingerprint(value: &str) -> String {
    digest(value.replace("\r\n", "\n").trim(), 64)
}
fn key(thread: &str, turn: &str) -> String {
    json!([thread, turn]).to_string()
}
fn string(value: Option<&Value>) -> Option<String> {
    value
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}
fn field(value: Option<&Value>, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| string(value.and_then(|value| value.get(*key))))
}
fn parsed(value: Option<&Value>) -> Value {
    match value {
        Some(Value::String(text)) => serde_json::from_str(text).unwrap_or(Value::Null),
        Some(value) => value.clone(),
        None => Value::Null,
    }
}
fn message(value: Option<&Value>) -> Option<String> {
    let text = match value? {
        Value::String(text) => text.clone(),
        Value::Array(parts) => parts
            .iter()
            .filter_map(|part| part.get("text").and_then(Value::as_str))
            .filter(|text| !text.is_empty())
            .collect::<Vec<_>>()
            .join("\n"),
        _ => return None,
    };
    (!text.trim().is_empty()).then_some(text)
}
pub(super) fn complete_descriptor(mut spawn: CodexSpawnDescriptor) -> CodexSpawnDescriptor {
    if spawn.parent_tool_span_id.is_none() {
        spawn.parent_tool_span_id = Some(digest(
            &[
                &spawn.parent_thread_id,
                &spawn.parent_turn_id,
                "tool",
                &spawn.parent_tool_call_id,
            ]
            .join("\0"),
            16,
        ));
    }
    if spawn.invocation_id.is_none() {
        spawn.invocation_id = Some(digest(
            &json!([
                "codex-delegation",
                spawn.parent_thread_id,
                spawn.parent_turn_id,
                spawn.parent_tool_call_id,
                spawn.work_index
            ])
            .to_string(),
            32,
        ));
    }
    spawn.result_fingerprints.clear();
    spawn.result_terminal_observed = false;
    spawn.result_child_turn_ids.clear();
    spawn.result_outcome_known = false;
    spawn
}

fn matches_child(spawn: &CodexSpawnDescriptor, child: &CodexChildThreadDescriptor) -> bool {
    if let Some(id) = spawn
        .child_thread_id
        .as_ref()
        .or(spawn.resolved_child_thread_id.as_ref())
    {
        return id == &child.child_thread_id;
    }
    spawn.parent_thread_id == child.parent_thread_id
        && child.agent_path.as_ref().is_some_and(|path| {
            spawn
                .agent_path
                .as_ref()
                .or(spawn.task_name.as_ref())
                .is_some_and(|candidate| {
                    normalize_agent_path(candidate) == normalize_agent_path(path)
                })
        })
}
fn input_payload<'a>(source: &'a CodexTranscriptSourceRecord, turn: &str) -> Option<&'a Value> {
    let payload = source.record.get("payload")?;
    let record_turn = field(
        Some(
            payload
                .get("internal_chat_message_metadata_passthrough")
                .unwrap_or(&Value::Null),
        ),
        &["turn_id"],
    )
    .or_else(|| field(Some(payload), &["turn_id"]));
    if record_turn.as_deref().is_some_and(|id| id != turn) {
        return None;
    }
    Some(payload)
}
fn injected_context(payload: &Value) -> bool {
    payload
        .get("content")
        .and_then(Value::as_array)
        .is_some_and(|parts| {
            !parts.is_empty()
                && parts.iter().all(|part| {
                    if part.get("type").and_then(Value::as_str) != Some("input_text") {
                        return false;
                    }
                    part.get("text")
                        .and_then(Value::as_str)
                        .is_some_and(|text| {
                            let text = text.trim();
                            (text.starts_with("# AGENTS.md instructions")
                                && text.contains("<INSTRUCTIONS>")
                                && text.ends_with("</INSTRUCTIONS>"))
                                || (text.starts_with("<environment_context>")
                                    && text.ends_with("</environment_context>"))
                        })
                })
        })
}
fn direct_input(source: &CodexTranscriptSourceRecord, turn: &str) -> bool {
    let Some(payload) = input_payload(source, turn) else {
        return false;
    };
    (source.record["type"] == "response_item"
        && payload["type"] == "message"
        && payload["role"] == "user"
        && !injected_context(payload))
        || (source.record["type"] == "event_msg" && payload["type"] == "user_message")
}
fn agent_input(source: &CodexTranscriptSourceRecord, turn: &str) -> bool {
    let Some(payload) = input_payload(source, turn) else {
        return false;
    };
    if source.record["type"] != "response_item" || payload["type"] != "agent_message" {
        return false;
    }
    let text = message(payload.get("content")).or_else(|| message(payload.get("message")));
    !text.as_deref().is_some_and(|text| {
        text.trim_start()
            .strip_prefix("Message Type:")
            .and_then(|tail| tail.split_whitespace().next())
            == Some("FINAL_ANSWER")
    })
}
pub fn is_codex_work_input(source: &CodexTranscriptSourceRecord, turn: &str) -> bool {
    direct_input(source, turn) || agent_input(source, turn)
}
fn fingerprints(turn: &str, sources: &[CodexTranscriptSourceRecord]) -> HashSet<String> {
    let mut plain = HashSet::new();
    let mut opaque = HashSet::new();
    for source in sources.iter().filter(|source| agent_input(source, turn)) {
        let Some(payload) = input_payload(source, turn) else {
            continue;
        };
        if let Some(parts) = payload.get("content").and_then(Value::as_array) {
            for part in parts {
                if part["type"] == "encrypted_content" {
                    if let Some(text) = part.get("encrypted_content").and_then(Value::as_str) {
                        opaque.insert(work_fingerprint(text));
                    }
                }
            }
        }
        if let Some(text) =
            message(payload.get("content")).or_else(|| message(payload.get("message")))
        {
            plain.insert(work_fingerprint(&text));
        }
    }
    if opaque.is_empty() {
        plain
    } else {
        opaque
    }
}

impl CodexSubagentLinker {
    pub fn serialize(&self) -> Value {
        json!({"version": 1, "spawns": self.spawns.values().collect::<Vec<_>>(),
            "children": self.children.values().collect::<Vec<_>>(),
            "bindings": self.bindings.values().collect::<Vec<_>>(),
            "completedWorkResults": self.completed_results.values().collect::<Vec<_>>(),
            "workTerminals": self.work_terminals.values().collect::<Vec<_>>()})
    }
    pub fn restore(&mut self, value: &Value) {
        if value["version"] != 1 {
            return;
        }
        if let Some(spawns) = value["spawns"].as_array() {
            for value in spawns {
                if let Ok(spawn) = serde_json::from_value::<CodexSpawnDescriptor>(value.clone()) {
                    if !spawn.parent_thread_id.is_empty()
                        && !spawn.parent_turn_id.is_empty()
                        && !spawn.parent_trace_id.is_empty()
                        && !spawn.parent_tool_call_id.is_empty()
                        && spawn.spawned_at_ms.is_finite()
                    {
                        self.register_spawns(&[spawn]);
                    }
                }
            }
        }
        if let Some(children) = value["children"].as_array() {
            for value in children {
                if let Ok(child) =
                    serde_json::from_value::<CodexChildThreadDescriptor>(value.clone())
                {
                    if !child.child_thread_id.is_empty()
                        && !child.parent_thread_id.is_empty()
                        && !child.root_session_id.is_empty()
                        && child.created_at_ms.is_finite()
                    {
                        self.children.insert(child.child_thread_id.clone(), child);
                    }
                }
            }
        }
        if let Some(bindings) = value["bindings"].as_array() {
            for value in bindings {
                if let Ok(binding) = serde_json::from_value::<WorkBinding>(value.clone()) {
                    if !binding.child_thread_id.is_empty()
                        && !binding.child_turn_id.is_empty()
                        && !binding.invocation_id.is_empty()
                    {
                        self.bindings.insert(
                            key(&binding.child_thread_id, &binding.child_turn_id),
                            binding,
                        );
                    }
                }
            }
        }
        if let Some(results) = value["completedWorkResults"].as_array() {
            for value in results {
                if let Ok(result) = serde_json::from_value::<CompletedResult>(value.clone()) {
                    if !result.invocation_id.is_empty()
                        && !result.child_thread_id.is_empty()
                        && !result.child_turn_id.is_empty()
                        && result.fingerprint.len() == 64
                        && result
                            .fingerprint
                            .bytes()
                            .all(|ch| ch.is_ascii_digit() || (b'a'..=b'f').contains(&ch))
                    {
                        self.completed_results.insert(
                            json!([
                                result.invocation_id,
                                result.child_thread_id,
                                result.child_turn_id,
                                result.fingerprint
                            ])
                            .to_string(),
                            result,
                        );
                    }
                }
            }
        }
        if let Some(terminals) = value["workTerminals"].as_array() {
            for value in terminals {
                if let Ok(terminal) = serde_json::from_value::<WorkTerminal>(value.clone()) {
                    if !terminal.invocation_id.is_empty()
                        && !terminal.child_thread_id.is_empty()
                        && !terminal.child_turn_id.is_empty()
                        && matches!(terminal.terminal_type.as_str(), "completed" | "interrupted")
                    {
                        self.work_terminals.insert(
                            json!([
                                terminal.invocation_id,
                                terminal.child_thread_id,
                                terminal.child_turn_id
                            ])
                            .to_string(),
                            terminal,
                        );
                    }
                }
            }
        }
        trim_oldest(&mut self.children, MAX_LINK_DESCRIPTORS);
        trim_oldest(&mut self.bindings, MAX_LINK_DESCRIPTORS);
        trim_oldest(&mut self.completed_results, MAX_LINK_DESCRIPTORS);
        trim_oldest(&mut self.work_terminals, MAX_LINK_DESCRIPTORS);
    }

    pub fn resolve_work(
        &mut self,
        meta: &CodexTranscriptMeta,
        turn: &CodexExtractedTranscriptTurn,
        records: &[CodexTranscriptSourceRecord],
        allow_creation_fallback: bool,
    ) -> Option<CodexSubagentLink> {
        self.register_child(meta);
        let child = self.children.get(&meta.thread_id)?.clone();
        let candidates: Vec<_> = self
            .spawns
            .values()
            .filter(|spawn| {
                !spawn.dispatch_failed
                    && !spawn.association_conflict
                    && matches_child(spawn, &child)
                    && spawn.spawned_at_ms <= turn.started_at_ms
            })
            .cloned()
            .collect();
        let direct = records
            .iter()
            .any(|source| direct_input(source, &turn.transcript_turn_id));
        let previous: Vec<_> = self
            .bindings
            .values()
            .filter(|binding| binding.child_thread_id == meta.thread_id)
            .collect();
        if direct && !(allow_creation_fallback && previous.is_empty()) {
            return None;
        }
        let fingerprints = fingerprints(&turn.transcript_turn_id, records);
        if fingerprints.len() > 1 || (direct && !fingerprints.is_empty()) {
            return None;
        }
        let creations: Vec<_> = candidates
            .iter()
            .filter(|spawn| {
                spawn
                    .dispatch_kind
                    .as_deref()
                    .is_none_or(|kind| kind == "spawn")
            })
            .collect();
        if direct && creations.len() != 1 {
            return None;
        }
        let explicit: Vec<_> = candidates
            .iter()
            .filter(|spawn| spawn.child_turn_id.as_deref() == Some(&turn.transcript_turn_id))
            .collect();
        let matching: Vec<_> = candidates
            .iter()
            .filter(|spawn| {
                spawn
                    .work_fingerprint
                    .as_ref()
                    .is_some_and(|hash| fingerprints.contains(hash))
            })
            .collect();
        if explicit.len() > 1 || (explicit.is_empty() && matching.len() > 1) {
            return None;
        }
        let mut selected = if direct {
            creations.first().copied()
        } else {
            explicit.first().or(matching.first()).copied()
        };
        if selected.is_none() && fingerprints.is_empty() {
            if let Some(saved) = self
                .bindings
                .get(&key(&meta.thread_id, &turn.transcript_turn_id))
            {
                selected = candidates
                    .iter()
                    .find(|spawn| spawn.invocation_id.as_deref() == Some(&saved.invocation_id));
            }
        }
        if selected.is_none()
            && allow_creation_fallback
            && previous.is_empty()
            && creations.len() == 1
        {
            selected = creations.first().copied();
        }
        if selected.is_none() && fingerprints.is_empty() && !direct && turn.prompt.is_none() {
            let ids: HashSet<_> = previous
                .iter()
                .map(|binding| binding.invocation_id.as_str())
                .collect();
            if ids.len() == 1 {
                selected = candidates.iter().find(|spawn| {
                    spawn
                        .invocation_id
                        .as_deref()
                        .is_some_and(|id| ids.contains(id))
                });
            }
        }
        let mut selected = selected?.clone();
        let invocation_id = selected.invocation_id.clone()?;
        self.bindings.insert(
            key(&meta.thread_id, &turn.transcript_turn_id),
            WorkBinding {
                child_thread_id: meta.thread_id.clone(),
                child_turn_id: turn.transcript_turn_id.clone(),
                invocation_id,
            },
        );
        trim_oldest(&mut self.bindings, MAX_LINK_DESCRIPTORS);
        if selected.child_thread_id.is_none() && selected.resolved_child_thread_id.is_none() {
            selected.resolved_child_thread_id = Some(meta.thread_id.clone());
            self.spawns.insert(spawn_key(&selected), selected.clone());
        }
        let confidence = if selected.child_thread_id.is_some() {
            CodexSubagentLinkConfidence::ExplicitId
        } else {
            CodexSubagentLinkConfidence::AgentPath
        };
        let mut link = linked_child(&child, &selected, confidence);
        link.child_turn_id = Some(turn.transcript_turn_id.clone());
        Some(link)
    }

    pub fn register_completed_work_result(
        &mut self,
        link: &CodexSubagentLink,
        turn: &CodexExtractedTranscriptTurn,
        records: &[CodexTranscriptSourceRecord],
    ) {
        let Some(invocation) = &link.invocation_id else {
            return;
        };
        if !matches!(
            link.confidence,
            CodexSubagentLinkConfidence::ExplicitId | CodexSubagentLinkConfidence::AgentPath
        ) || link.child_thread_id != turn.session_id
            || link
                .child_turn_id
                .as_ref()
                .is_some_and(|id| id != &turn.transcript_turn_id)
        {
            return;
        }
        for source in records {
            let payload = &source.record["payload"];
            if source.record["type"] != "event_msg" || payload["turn_id"] != turn.transcript_turn_id
            {
                continue;
            }
            let terminal_type = match (payload["type"].as_str(), turn.status) {
                (Some("task_complete"), CodexTerminalStatus::Completed) => "completed",
                (Some("turn_aborted"), CodexTerminalStatus::Interrupted) => "interrupted",
                _ => continue,
            };
            self.work_terminals.insert(
                json!([invocation, turn.session_id, turn.transcript_turn_id]).to_string(),
                WorkTerminal {
                    invocation_id: invocation.clone(),
                    child_thread_id: turn.session_id.clone(),
                    child_turn_id: turn.transcript_turn_id.clone(),
                    terminal_type: terminal_type.into(),
                },
            );
            if terminal_type == "completed" {
                if let Some(body) = payload["last_agent_message"].as_str() {
                    let fingerprint = digest(body, 64);
                    self.completed_results.insert(
                        json!([
                            invocation,
                            turn.session_id,
                            turn.transcript_turn_id,
                            fingerprint
                        ])
                        .to_string(),
                        CompletedResult {
                            invocation_id: invocation.clone(),
                            child_thread_id: turn.session_id.clone(),
                            child_turn_id: turn.transcript_turn_id.clone(),
                            fingerprint,
                        },
                    );
                }
            }
        }
        trim_oldest(&mut self.completed_results, MAX_LINK_DESCRIPTORS);
        trim_oldest(&mut self.work_terminals, MAX_LINK_DESCRIPTORS);
    }

    pub fn get_delegations_for_parent(&self, parent: &str) -> Vec<CodexSpawnDescriptor> {
        self.spawns
            .values()
            .filter(|spawn| spawn.parent_thread_id == parent)
            .map(|spawn| {
                let mut copy = spawn.clone();
                let results: Vec<_> = self
                    .completed_results
                    .values()
                    .filter(|result| Some(&result.invocation_id) == spawn.invocation_id.as_ref())
                    .collect();
                let terminals: Vec<_> = self
                    .work_terminals
                    .values()
                    .filter(|terminal| {
                        Some(&terminal.invocation_id) == spawn.invocation_id.as_ref()
                    })
                    .collect();
                copy.result_terminal_observed = !results.is_empty() || !terminals.is_empty();
                copy.result_outcome_known = copy.result_terminal_observed
                    && terminals.iter().all(|terminal| {
                        terminal.terminal_type == "interrupted"
                            || results.iter().any(|result| {
                                result.child_thread_id == terminal.child_thread_id
                                    && result.child_turn_id == terminal.child_turn_id
                            })
                    });
                copy.result_fingerprints = results
                    .iter()
                    .map(|result| result.fingerprint.clone())
                    .collect();
                copy.result_child_turn_ids = terminals
                    .iter()
                    .map(|terminal| terminal.child_turn_id.clone())
                    .chain(results.iter().map(|result| result.child_turn_id.clone()))
                    .collect();
                copy.result_child_turn_ids.sort();
                copy.result_child_turn_ids.dedup();
                let path = spawn.agent_path.as_ref().or(spawn.task_name.as_ref());
                let known_id = spawn
                    .child_thread_id
                    .as_ref()
                    .or(spawn.resolved_child_thread_id.as_ref());
                let known = known_id.and_then(|id| self.children.get(id));
                if copy.agent_path.is_none() {
                    copy.agent_path = known.and_then(|child| child.agent_path.clone());
                }
                if copy.agent_nickname.is_none() {
                    copy.agent_nickname = known.and_then(|child| child.agent_nickname.clone());
                }
                if spawn.child_thread_id.is_some()
                    || spawn.dispatch_failed
                    || spawn.association_conflict
                {
                    return copy;
                }
                if let Some(id) = &spawn.resolved_child_thread_id {
                    copy.child_thread_id = Some(id.clone());
                    return copy;
                }
                if let Some(path) = path {
                    let creations = self
                        .spawns
                        .values()
                        .filter(|candidate| {
                            candidate.parent_thread_id == parent
                                && candidate
                                    .dispatch_kind
                                    .as_deref()
                                    .is_none_or(|kind| kind == "spawn")
                                && candidate
                                    .agent_path
                                    .as_ref()
                                    .or(candidate.task_name.as_ref())
                                    .is_some_and(|candidate| {
                                        normalize_agent_path(candidate)
                                            == normalize_agent_path(path)
                                    })
                        })
                        .count();
                    if creations > 1 {
                        return copy;
                    }
                    let children: Vec<_> = self
                        .children
                        .values()
                        .filter(|child| {
                            child.parent_thread_id == parent
                                && child.agent_path.as_ref().is_some_and(|candidate| {
                                    normalize_agent_path(candidate) == normalize_agent_path(path)
                                })
                        })
                        .collect();
                    if children.len() == 1 {
                        copy.child_thread_id = Some(children[0].child_thread_id.clone());
                        if copy.agent_nickname.is_none() {
                            copy.agent_nickname = children[0].agent_nickname.clone();
                        }
                    }
                }
                copy
            })
            .collect()
    }
}

fn select_activity<'a>(
    activities: &'a [Value],
    result: Option<&Value>,
    task: Option<&Value>,
    count: usize,
    activity_only_index: Option<usize>,
) -> (Option<&'a Value>, bool) {
    if let Some(index) = activity_only_index {
        return (activities.get(index), false);
    }
    let child = field(result, &["agent_id", "thread_id", "agent_thread_id"])
        .or_else(|| field(task, &["agent_id", "thread_id", "id"]));
    if let Some(child) = child {
        let matching: Vec<_> = activities
            .iter()
            .filter(|activity| {
                field(Some(activity), &["agent_thread_id", "agent_id"]).as_ref() == Some(&child)
            })
            .collect();
        return match matching.as_slice() {
            [only] => (Some(*only), false),
            [] => (None, count == 1 && !activities.is_empty()),
            _ => (None, true),
        };
    }
    let path = field(result, &["agent_path", "task_path", "task_name"])
        .or_else(|| field(task, &["agent_path", "task_path", "task_name", "target"]));
    let task_id = field(result, &["task_id"]).or_else(|| field(task, &["task_id"]));
    if path.is_some() || task_id.is_some() {
        let matching: Vec<_> = activities
            .iter()
            .filter(|activity| {
                if task_id.is_some() && field(Some(activity), &["task_id"]) == task_id {
                    return true;
                }
                path.as_ref().is_some_and(|path| {
                    field(Some(activity), &["agent_path", "task_path", "task_name"]).is_some_and(
                        |candidate| normalize_agent_path(&candidate) == normalize_agent_path(path),
                    )
                })
            })
            .collect();
        match matching.as_slice() {
            [only] => return (Some(*only), false),
            [] => {
                if count == 1
                    && activities.len() == 1
                    && ((path.is_some()
                        && field(
                            activities.first(),
                            &["agent_path", "task_path", "task_name"],
                        )
                        .is_some())
                        || (task_id.is_some() && field(activities.first(), &["task_id"]).is_some()))
                {
                    return (None, true);
                }
            }
            _ => return (None, true),
        }
    }
    if count == 1 && activities.len() == 1 {
        (activities.first(), false)
    } else {
        (None, false)
    }
}

pub(super) fn extract_dispatches(
    turn: &CodexExtractedTranscriptTurn,
    sources: &[CodexTranscriptSourceRecord],
    parent_trace: &str,
) -> Vec<CodexSpawnDescriptor> {
    let mut activities: IndexMap<String, Vec<Value>> = IndexMap::new();
    for source in sources {
        let payload = &source.record["payload"];
        if source.record["type"] != "event_msg"
            || payload["type"] != "sub_agent_activity"
            || payload["kind"] != "started"
        {
            continue;
        }
        if let Some(call) = field(Some(payload), &["event_id", "call_id", "tool_call_id"]) {
            let group = activities.entry(call).or_default();
            let identity = |value: &Value| {
                json!([
                    field(Some(value), &["agent_thread_id", "agent_id"]),
                    field(Some(value), &["agent_path", "task_path"]),
                    value["turn_id"]
                ])
            };
            if !group
                .iter()
                .any(|existing| identity(existing) == identity(payload))
            {
                group.push(payload.clone());
            }
        }
    }
    let mut descriptors = Vec::new();
    for step in &turn.steps {
        for tool in &step.tools {
            let name = tool.name.rsplit(['.', '/']).next().unwrap_or(&tool.name);
            let kind = match name {
                "spawn_agent" => "spawn",
                "followup_task" => "followup",
                "send_input" => "input",
                _ => continue,
            };
            let input = parsed(tool.input.as_ref());
            let output = parsed(tool.output.as_ref());
            let tasks = input
                .get("agents")
                .or_else(|| input.get("tasks"))
                .and_then(Value::as_array);
            let results = output
                .get("agents")
                .or_else(|| output.get("tasks"))
                .and_then(Value::as_array)
                .or_else(|| output.as_array());
            let activity = activities
                .get(&tool.call_id)
                .map(Vec::as_slice)
                .unwrap_or_default();
            let count = tasks
                .map_or(0, Vec::len)
                .max(results.map_or(0, Vec::len))
                .max(activity.len())
                .max(1);
            for index in 0..count {
                let task = tasks.map(|tasks| tasks.get(index)).unwrap_or(Some(&input));
                let result = results
                    .map(|results| results.get(index))
                    .unwrap_or_else(|| (count == 1).then_some(&output));
                let (started, conflict) = select_activity(
                    activity,
                    result,
                    task,
                    count,
                    (tasks.is_none() && results.is_none() && count > 1).then_some(index),
                );
                let prompt = task.and_then(|task| {
                    message(task.get("message"))
                        .or_else(|| message(task.get("prompt")))
                        .or_else(|| message(task.get("input")))
                });
                if kind != "spawn"
                    && (prompt.is_none()
                        || ["cancel", "notification", "notify_only", "notification_only"]
                            .iter()
                            .any(|key| {
                                task.and_then(|task| task.get(*key)) == Some(&Value::Bool(true))
                            }))
                {
                    continue;
                }
                let child = field(started, &["agent_thread_id", "agent_id"])
                    .or_else(|| field(result, &["agent_id", "thread_id", "agent_thread_id"]))
                    .or_else(|| {
                        (kind != "spawn")
                            .then(|| field(task, &["id", "agent_id", "thread_id"]))
                            .flatten()
                    });
                let path = field(started, &["agent_path", "task_path"])
                    .or_else(|| field(result, &["agent_path", "task_path", "task_name"]))
                    .or_else(|| {
                        (kind != "spawn")
                            .then(|| field(task, &["target"]))
                            .flatten()
                    });
                let failed_text = tool
                    .output
                    .as_ref()
                    .and_then(Value::as_str)
                    .map(str::to_lowercase);
                let failed = output
                    .get("error")
                    .is_some_and(|error| !error.is_null() && error != &Value::Bool(false))
                    || matches!(output["status"].as_str(), Some("failed" | "rejected"))
                    || failed_text.as_ref().is_some_and(|text| {
                        ["spawn failed", "spawn rejected", "agent thread limit"]
                            .iter()
                            .any(|pattern| text.contains(pattern))
                    });
                descriptors.push(complete_descriptor(CodexSpawnDescriptor {
                    parent_thread_id: turn.session_id.clone(),
                    parent_turn_id: turn.transcript_turn_id.clone(),
                    parent_trace_id: parent_trace.into(),
                    parent_tool_call_id: tool.call_id.clone(),
                    spawned_at_ms: started
                        .and_then(|value| value["occurred_at_ms"].as_f64())
                        .unwrap_or(tool.started_at_ms),
                    task_name: field(task, &["task_name", "agent_path"]),
                    agent_path: path,
                    child_thread_id: child,
                    dispatch_kind: Some(kind.into()),
                    work_index: index,
                    work_fingerprint: prompt.as_deref().map(work_fingerprint),
                    child_turn_id: field(started, &["turn_id"])
                        .or_else(|| field(result, &["turn_id"])),
                    agent_nickname: field(result, &["nickname"]),
                    dispatch_failed: failed,
                    association_conflict: conflict,
                    ..Default::default()
                }));
            }
        }
    }
    descriptors
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inputs::codex_transcript::CodexTranscriptStep;

    fn meta() -> CodexTranscriptMeta {
        CodexTranscriptMeta {
            thread_id: "child".into(),
            root_session_id: "parent".into(),
            thread_source: super::super::super::types::CodexThreadSource::Subagent,
            parent_thread_id: Some("parent".into()),
            forked_from_id: None,
            depth: 2,
            created_at_ms: Some(1.0),
            agent_path: Some("/root/worker".into()),
            agent_nickname: Some("Worker".into()),
            agent_role: None,
            provider: "openai".into(),
            base_instructions: None,
            tool_definitions: None,
        }
    }
    fn turn(id: &str) -> CodexExtractedTranscriptTurn {
        CodexExtractedTranscriptTurn {
            session_id: "child".into(),
            transcript_turn_id: id.into(),
            provider: "openai".into(),
            model: "model".into(),
            status: CodexTerminalStatus::Completed,
            started_at_ms: 100.0,
            terminal_at_ms: 200.0,
            prompt: None,
            prompt_ready: false,
            input_messages: Vec::new(),
            cwd: None,
            developer_instructions: None,
            base_instructions: None,
            tool_definitions: None,
            steps: Vec::new(),
            unmatched_token_usages: Vec::new(),
        }
    }
    fn spawn(call: &str, prompt: &str) -> CodexSpawnDescriptor {
        complete_descriptor(CodexSpawnDescriptor {
            parent_thread_id: "parent".into(),
            parent_turn_id: "parent-turn".into(),
            parent_trace_id: "0123456789abcdef0123456789abcdef".into(),
            parent_tool_call_id: call.into(),
            spawned_at_ms: 1.0,
            child_thread_id: Some("child".into()),
            work_fingerprint: Some(work_fingerprint(prompt)),
            dispatch_kind: Some("followup".into()),
            ..Default::default()
        })
    }
    fn source(payload: Value) -> CodexTranscriptSourceRecord {
        CodexTranscriptSourceRecord {
            start_offset: 0,
            end_offset: 100,
            record: json!({"type":"response_item","payload":payload})
                .as_object()
                .unwrap()
                .clone(),
        }
    }
    #[test]
    fn followup_work_is_bound_by_native_input_and_survives_restart_without_plaintext() {
        let mut linker = CodexSubagentLinker::new();
        linker.register_spawns(&[
            spawn("first", "first task"),
            spawn("second", "secret second task"),
        ]);
        let input = source(
            json!({"type":"agent_message","content":[{"type":"input_text","text":"secret second task"}]}),
        );
        let link = linker
            .resolve_work(&meta(), &turn("second-turn"), &[input], false)
            .unwrap();
        assert_eq!(link.parent_tool_call_id.as_deref(), Some("second"));
        assert_eq!(link.child_turn_id.as_deref(), Some("second-turn"));
        let state = linker.serialize();
        assert!(!state.to_string().contains("secret second task"));
        let mut restored = CodexSubagentLinker::new();
        restored.restore(&state);
        let link2 = restored
            .resolve_work(&meta(), &turn("second-turn"), &[], false)
            .unwrap();
        assert_eq!(link.invocation_id, link2.invocation_id);
        assert_eq!(link2.child_thread_id, "child");
    }

    #[test]
    fn direct_user_input_ambiguous_messages_and_notifications_never_claim_latest_work() {
        let mut linker = CodexSubagentLinker::new();
        linker.register_spawns(&[spawn("first", "same text"), spawn("second", "other")]);
        let direct = source(
            json!({"type":"message","role":"user","content":[{"type":"input_text","text":"same text"}]}),
        );
        assert!(linker
            .resolve_work(&meta(), &turn("direct"), &[direct], false)
            .is_none());
        let messages = [
            source(json!({"type":"agent_message","content":[{"text":"same text"}]})),
            source(json!({"type":"agent_message","content":[{"text":"other"}]})),
        ];
        assert!(linker
            .resolve_work(&meta(), &turn("ambiguous"), &messages, false)
            .is_none());
        let notification = source(
            json!({"type":"agent_message","content":[{"text":"Message Type: FINAL_ANSWER\nPayload:\nsame text"}]}),
        );
        assert!(linker
            .resolve_work(&meta(), &turn("result"), &[notification], false)
            .is_none());
    }

    #[test]
    fn exact_result_fingerprints_and_terminal_facts_are_persisted_separately() {
        let mut linker = CodexSubagentLinker::new();
        let mut work = spawn("call", "task");
        work.child_turn_id = Some("native-turn".into());
        linker.register_spawns(&[work]);
        let turn = turn("native-turn");
        let link = linker.resolve_work(&meta(), &turn, &[], false).unwrap();
        let terminal = CodexTranscriptSourceRecord {
            start_offset: 10,
            end_offset: 50,
            record:
                json!({"type":"event_msg","payload":{"type":"task_complete","turn_id":"native-turn",
                "last_agent_message":"exact result\n"}})
                .as_object()
                .unwrap()
                .clone(),
        };
        linker.register_completed_work_result(&link, &turn, &[terminal]);
        let descriptor = &linker.get_delegations_for_parent("parent")[0];
        assert!(descriptor.result_terminal_observed && descriptor.result_outcome_known);
        assert_eq!(
            descriptor.result_fingerprints,
            [digest("exact result\n", 64)]
        );
        assert_ne!(descriptor.result_fingerprints, [digest("exact result", 64)]);
        let state = linker.serialize();
        assert!(!state.to_string().contains("exact result"));
        assert!(state["spawns"][0].get("resultFingerprints").is_none());
        let mut restored = CodexSubagentLinker::new();
        restored.restore(&state);
        assert_eq!(
            restored.get_delegations_for_parent("parent")[0].result_fingerprints,
            descriptor.result_fingerprints
        );
    }

    #[test]
    fn batched_dispatch_positions_and_failed_or_notification_inputs_are_preserved() {
        let mut parent = turn("parent-turn");
        parent.session_id = "parent".into();
        parent.steps = vec![CodexTranscriptStep {
            started_at_ms: 1.0,
            response_at_ms: 2.0,
            has_response_evidence: true,
            completed_at_ms: 3.0,
            response_id: None,
            input_messages: None,
            reasoning: Vec::new(),
            token_usage: None,
            final_text: None,
            tools: vec![
                super::super::super::types::CodexTranscriptTool {
                    call_id: "batch".into(),
                    name: "collaboration.spawn_agent".into(),
                    input: Some(json!({"tasks":[
                        {"task_name":"one","message":"private one"},null,{"task_name":"three","message":"private three"}]})),
                    started_at_ms: 1.0,
                    output: Some(json!({"tasks":[{"agent_id":"c1"},null,{"agent_id":"c3"}]})),
                    completed_at_ms: Some(3.0),
                },
                super::super::super::types::CodexTranscriptTool {
                    call_id: "notify".into(),
                    name: "send_input".into(),
                    input: Some(json!({"id":"c1","message":"notify","notify_only":true})),
                    started_at_ms: 4.0,
                    output: None,
                    completed_at_ms: None,
                },
            ],
        }];
        let dispatches = extract_dispatches(&parent, &[], "0123456789abcdef0123456789abcdef");
        assert_eq!(dispatches.len(), 3);
        assert_eq!(dispatches[2].work_index, 2);
        assert_eq!(dispatches[2].child_thread_id.as_deref(), Some("c3"));
        assert!(dispatches[1].child_thread_id.is_none());
        assert_ne!(dispatches[0].invocation_id, dispatches[1].invocation_id);
        assert!(!serde_json::to_string(&dispatches)
            .unwrap()
            .contains("private"));
    }
}
