//! Depth-one Codex subagent linker. Port of `codex-subagent-linker.ts`.

use indexmap::IndexMap;

use super::types::{
    CodexExtractedTranscriptTurn, CodexTranscriptMeta, CodexTranscriptSourceRecord,
    CodexTranscriptTool,
};
use super::utils::{as_record, number_value, string_value};

pub const MAX_LINK_DESCRIPTORS: usize = 10_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CodexSubagentLinkConfidence {
    ExplicitId,
    AgentPath,
    TimeOrder,
    Orphan,
}

impl CodexSubagentLinkConfidence {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ExplicitId => "explicit_id",
            Self::AgentPath => "agent_path",
            Self::TimeOrder => "time_order",
            Self::Orphan => "orphan",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrphanReason {
    ParentNotFound,
    AmbiguousExplicitId,
    AmbiguousAgentPath,
    NoCandidate,
}

impl OrphanReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ParentNotFound => "parent_not_found",
            Self::AmbiguousExplicitId => "ambiguous_explicit_id",
            Self::AmbiguousAgentPath => "ambiguous_agent_path",
            Self::NoCandidate => "no_candidate",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct CodexSpawnDescriptor {
    pub parent_thread_id: String,
    pub parent_turn_id: String,
    pub parent_trace_id: String,
    pub parent_tool_call_id: String,
    pub spawned_at_ms: f64,
    pub task_name: Option<String>,
    pub agent_path: Option<String>,
    pub child_thread_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CodexChildThreadDescriptor {
    pub child_thread_id: String,
    pub parent_thread_id: String,
    pub root_session_id: String,
    pub depth: i64,
    pub created_at_ms: f64,
    pub agent_path: Option<String>,
    pub agent_nickname: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CodexSubagentLink {
    pub parent_thread_id: String,
    pub parent_turn_id: Option<String>,
    pub parent_trace_id: Option<String>,
    pub parent_tool_call_id: Option<String>,
    pub child_thread_id: String,
    pub agent_path: Option<String>,
    pub confidence: CodexSubagentLinkConfidence,
    pub orphan_reason: Option<OrphanReason>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CodexSubagentLinkSnapshot {
    pub detected_children: usize,
    pub detected_spawns: usize,
    pub linked_children: usize,
    pub orphan_children: usize,
    pub links: Vec<CodexSubagentLink>,
}

struct CandidateSelection {
    spawn: Option<CodexSpawnDescriptor>,
    confidence: Option<CodexSubagentLinkConfidence>,
    orphan_reason: Option<OrphanReason>,
}

#[derive(Debug, Default)]
pub struct CodexSubagentLinker {
    spawns: IndexMap<String, CodexSpawnDescriptor>,
    children: IndexMap<String, CodexChildThreadDescriptor>,
}

impl CodexSubagentLinker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_spawns(&mut self, descriptors: &[CodexSpawnDescriptor]) {
        for descriptor in descriptors {
            let key = spawn_key(descriptor);
            let merged = match self.spawns.get(&key) {
                Some(previous) => merge_spawn(previous, descriptor),
                None => descriptor.clone(),
            };
            self.spawns.insert(key, merged);
            trim_oldest(&mut self.spawns, MAX_LINK_DESCRIPTORS);
        }
    }

    pub fn register_child(&mut self, meta: &CodexTranscriptMeta) {
        if meta.thread_source != super::types::CodexThreadSource::Subagent
            || meta.depth != 1
            || meta.parent_thread_id.is_none()
        {
            return;
        }
        let parent = meta.parent_thread_id.clone().unwrap_or_default();
        self.children.insert(
            meta.thread_id.clone(),
            CodexChildThreadDescriptor {
                child_thread_id: meta.thread_id.clone(),
                parent_thread_id: parent,
                root_session_id: meta.root_session_id.clone(),
                depth: meta.depth,
                created_at_ms: meta.created_at_ms.unwrap_or(0.0),
                agent_path: meta.agent_path.clone(),
                agent_nickname: meta.agent_nickname.clone(),
            },
        );
        trim_oldest(&mut self.children, MAX_LINK_DESCRIPTORS);
    }

    pub fn has_thread(&self, thread_id: &str) -> bool {
        self.children.contains_key(thread_id)
    }

    pub fn snapshot(&self) -> CodexSubagentLinkSnapshot {
        let spawns: Vec<CodexSpawnDescriptor> = self.spawns.values().cloned().collect();
        let mut children: Vec<CodexChildThreadDescriptor> =
            self.children.values().cloned().collect();
        children.sort_by(|left, right| {
            left.created_at_ms
                .partial_cmp(&right.created_at_ms)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.child_thread_id.cmp(&right.child_thread_id))
        });
        let mut used = std::collections::HashSet::new();
        let mut links_by_child = IndexMap::<String, CodexSubagentLink>::new();
        assign_links(
            &children,
            &spawns,
            &mut used,
            &mut links_by_child,
            CodexSubagentLinkConfidence::ExplicitId,
        );
        assign_links(
            &children,
            &spawns,
            &mut used,
            &mut links_by_child,
            CodexSubagentLinkConfidence::AgentPath,
        );
        assign_links(
            &children,
            &spawns,
            &mut used,
            &mut links_by_child,
            CodexSubagentLinkConfidence::TimeOrder,
        );
        for child in &children {
            if links_by_child.contains_key(&child.child_thread_id) {
                continue;
            }
            let has_parent_spawn = spawns
                .iter()
                .any(|spawn| spawn.parent_thread_id == child.parent_thread_id);
            links_by_child.insert(
                child.child_thread_id.clone(),
                orphan_link(
                    child,
                    if has_parent_spawn {
                        OrphanReason::NoCandidate
                    } else {
                        OrphanReason::ParentNotFound
                    },
                ),
            );
        }
        let links: Vec<CodexSubagentLink> = children
            .iter()
            .filter_map(|child| links_by_child.get(&child.child_thread_id).cloned())
            .collect();
        let linked_children = links
            .iter()
            .filter(|link| link.confidence != CodexSubagentLinkConfidence::Orphan)
            .count();
        CodexSubagentLinkSnapshot {
            detected_children: children.len(),
            detected_spawns: spawns.len(),
            linked_children,
            orphan_children: links.len() - linked_children,
            links,
        }
    }
}

fn assign_links(
    children: &[CodexChildThreadDescriptor],
    spawns: &[CodexSpawnDescriptor],
    used: &mut std::collections::HashSet<String>,
    links_by_child: &mut IndexMap<String, CodexSubagentLink>,
    phase: CodexSubagentLinkConfidence,
) {
    for child in children {
        if links_by_child.contains_key(&child.child_thread_id) {
            continue;
        }
        let candidates: Vec<CodexSpawnDescriptor> = spawns
            .iter()
            .filter(|spawn| {
                spawn.parent_thread_id == child.parent_thread_id
                    && !used.contains(&spawn_key(spawn))
            })
            .cloned()
            .collect();
        let selection = select_candidate_for_phase(child, &candidates, phase);
        if let Some(reason) = selection.orphan_reason {
            links_by_child.insert(child.child_thread_id.clone(), orphan_link(child, reason));
            continue;
        }
        let (Some(spawn), Some(confidence)) = (selection.spawn, selection.confidence) else {
            continue;
        };
        used.insert(spawn_key(&spawn));
        links_by_child.insert(
            child.child_thread_id.clone(),
            linked_child(child, &spawn, confidence),
        );
    }
}

pub fn extract_codex_spawn_descriptors(
    turn: &CodexExtractedTranscriptTurn,
    source_records: &[CodexTranscriptSourceRecord],
    parent_trace_id: &str,
) -> Vec<CodexSpawnDescriptor> {
    let mut activities: IndexMap<String, (Option<String>, Option<String>, Option<f64>)> =
        IndexMap::new();
    for source in source_records {
        if source.record.get("type").and_then(|value| value.as_str()) != Some("event_msg") {
            continue;
        }
        let Some(payload) = source.record.get("payload").and_then(as_record) else {
            continue;
        };
        if payload.get("type").and_then(|value| value.as_str()) != Some("sub_agent_activity")
            || payload.get("kind").and_then(|value| value.as_str()) != Some("started")
        {
            continue;
        }
        let Some(call_id) = string_value(payload.get("event_id")) else {
            continue;
        };
        activities.insert(
            call_id.to_owned(),
            (
                string_value(payload.get("agent_thread_id")).map(str::to_owned),
                string_value(payload.get("agent_path")).map(str::to_owned),
                number_value(payload.get("occurred_at_ms")),
            ),
        );
    }

    let mut descriptors = Vec::new();
    for step in &turn.steps {
        for tool in &step.tools {
            if tool.name != "spawn_agent" {
                continue;
            }
            let activity = activities.get(&tool.call_id);
            let output = tool.output.as_ref().and_then(as_record);
            let result_agent_path = output
                .and_then(|record| string_value(record.get("agent_path")))
                .or_else(|| output.and_then(|record| string_value(record.get("task_name"))));
            if activity.is_none() && result_agent_path.is_none() {
                continue;
            }
            let task_name = spawn_task_name(tool);
            descriptors.push(CodexSpawnDescriptor {
                parent_thread_id: turn.session_id.clone(),
                parent_turn_id: turn.transcript_turn_id.clone(),
                parent_trace_id: parent_trace_id.to_owned(),
                parent_tool_call_id: tool.call_id.clone(),
                spawned_at_ms: activity
                    .and_then(|item| item.2)
                    .unwrap_or(tool.started_at_ms),
                task_name,
                agent_path: activity.and_then(|item| item.1.clone()),
                child_thread_id: activity.and_then(|item| item.0.clone()),
            });
        }
    }
    descriptors
}

fn select_candidate_for_phase(
    child: &CodexChildThreadDescriptor,
    candidates: &[CodexSpawnDescriptor],
    phase: CodexSubagentLinkConfidence,
) -> CandidateSelection {
    if phase == CodexSubagentLinkConfidence::ExplicitId {
        let explicit: Vec<&CodexSpawnDescriptor> = candidates
            .iter()
            .filter(|spawn| {
                spawn.child_thread_id.as_deref() == Some(child.child_thread_id.as_str())
            })
            .collect();
        if explicit.len() == 1 {
            return CandidateSelection {
                spawn: Some(explicit[0].clone()),
                confidence: Some(CodexSubagentLinkConfidence::ExplicitId),
                orphan_reason: None,
            };
        }
        if explicit.len() > 1 {
            return CandidateSelection {
                spawn: None,
                confidence: None,
                orphan_reason: Some(OrphanReason::AmbiguousExplicitId),
            };
        }
        return CandidateSelection {
            spawn: None,
            confidence: None,
            orphan_reason: None,
        };
    }
    if phase == CodexSubagentLinkConfidence::AgentPath {
        let Some(child_path) = &child.agent_path else {
            return CandidateSelection {
                spawn: None,
                confidence: None,
                orphan_reason: None,
            };
        };
        let exact: Vec<&CodexSpawnDescriptor> = candidates
            .iter()
            .filter(|spawn| {
                let candidate = spawn.agent_path.as_deref().or(spawn.task_name.as_deref());
                candidate.is_some_and(|path| {
                    normalize_agent_path(path) == normalize_agent_path(child_path)
                })
            })
            .collect();
        if exact.len() == 1 {
            return CandidateSelection {
                spawn: Some(exact[0].clone()),
                confidence: Some(CodexSubagentLinkConfidence::AgentPath),
                orphan_reason: None,
            };
        }
        if exact.len() > 1 {
            return CandidateSelection {
                spawn: None,
                confidence: None,
                orphan_reason: Some(OrphanReason::AmbiguousAgentPath),
            };
        }
        return CandidateSelection {
            spawn: None,
            confidence: None,
            orphan_reason: None,
        };
    }

    let mut preceding: Vec<&CodexSpawnDescriptor> = candidates
        .iter()
        .filter(|spawn| child.created_at_ms <= 0.0 || spawn.spawned_at_ms <= child.created_at_ms)
        .collect();
    preceding.sort_by(|left, right| {
        right
            .spawned_at_ms
            .partial_cmp(&left.spawned_at_ms)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| spawn_key(left).cmp(&spawn_key(right)))
    });
    if preceding.is_empty() {
        return CandidateSelection {
            spawn: None,
            confidence: None,
            orphan_reason: if candidates.is_empty() {
                None
            } else {
                Some(OrphanReason::NoCandidate)
            },
        };
    }
    if preceding.len() > 1 && preceding[0].spawned_at_ms == preceding[1].spawned_at_ms {
        return CandidateSelection {
            spawn: None,
            confidence: None,
            orphan_reason: Some(OrphanReason::NoCandidate),
        };
    }
    CandidateSelection {
        spawn: Some(preceding[0].clone()),
        confidence: Some(CodexSubagentLinkConfidence::TimeOrder),
        orphan_reason: None,
    }
}

fn linked_child(
    child: &CodexChildThreadDescriptor,
    spawn: &CodexSpawnDescriptor,
    confidence: CodexSubagentLinkConfidence,
) -> CodexSubagentLink {
    CodexSubagentLink {
        parent_thread_id: child.parent_thread_id.clone(),
        parent_turn_id: Some(spawn.parent_turn_id.clone()),
        parent_trace_id: Some(spawn.parent_trace_id.clone()),
        parent_tool_call_id: Some(spawn.parent_tool_call_id.clone()),
        child_thread_id: child.child_thread_id.clone(),
        agent_path: child.agent_path.clone(),
        confidence,
        orphan_reason: None,
    }
}

fn orphan_link(child: &CodexChildThreadDescriptor, reason: OrphanReason) -> CodexSubagentLink {
    CodexSubagentLink {
        parent_thread_id: child.parent_thread_id.clone(),
        parent_turn_id: None,
        parent_trace_id: None,
        parent_tool_call_id: None,
        child_thread_id: child.child_thread_id.clone(),
        agent_path: child.agent_path.clone(),
        confidence: CodexSubagentLinkConfidence::Orphan,
        orphan_reason: Some(reason),
    }
}

fn spawn_task_name(tool: &CodexTranscriptTool) -> Option<String> {
    let output = tool.output.as_ref().and_then(as_record);
    let input = tool.input.as_ref().and_then(as_record);
    string_value(output.and_then(|record| record.get("task_name")))
        .or_else(|| string_value(output.and_then(|record| record.get("agent_path"))))
        .or_else(|| string_value(input.and_then(|record| record.get("task_name"))))
        .or_else(|| string_value(input.and_then(|record| record.get("agent_path"))))
        .map(str::to_owned)
}

fn merge_spawn(
    previous: &CodexSpawnDescriptor,
    next: &CodexSpawnDescriptor,
) -> CodexSpawnDescriptor {
    CodexSpawnDescriptor {
        parent_thread_id: next.parent_thread_id.clone(),
        parent_turn_id: next.parent_turn_id.clone(),
        parent_trace_id: next.parent_trace_id.clone(),
        parent_tool_call_id: next.parent_tool_call_id.clone(),
        spawned_at_ms: next.spawned_at_ms,
        task_name: next
            .task_name
            .clone()
            .or_else(|| previous.task_name.clone()),
        agent_path: next
            .agent_path
            .clone()
            .or_else(|| previous.agent_path.clone()),
        child_thread_id: next
            .child_thread_id
            .clone()
            .or_else(|| previous.child_thread_id.clone()),
    }
}

fn spawn_key(spawn: &CodexSpawnDescriptor) -> String {
    format!("{}:{}", spawn.parent_thread_id, spawn.parent_tool_call_id)
}

pub fn normalize_agent_path(value: &str) -> String {
    let trimmed = value.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return trimmed.to_owned();
    }
    if trimmed.starts_with('/') {
        trimmed.to_owned()
    } else {
        let rest = trimmed.strip_prefix("root/").unwrap_or(trimmed);
        format!("/root/{rest}")
    }
}

fn trim_oldest<T>(values: &mut IndexMap<String, T>, max_size: usize) {
    while values.len() > max_size {
        values.shift_remove_index(0);
    }
}
