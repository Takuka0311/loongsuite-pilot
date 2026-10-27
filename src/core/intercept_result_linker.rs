//! Joins interceptor tool verdicts onto tool.call / tool.result entries.
//! Port of `intercept-result-linker.ts`.

use std::sync::Arc;

use crate::interceptor::{ToolInterceptPhase, ToolVerdictKey, ToolVerdictStore};
use crate::types::{AgentActivityEntry, AgentEventName, ClientType, GuardrailAction};

pub struct InterceptResultLinker {
    verdict_store: Arc<ToolVerdictStore>,
    enabled: bool,
}

impl InterceptResultLinker {
    pub fn new(verdict_store: Arc<ToolVerdictStore>, enabled: bool) -> Self {
        Self {
            verdict_store,
            enabled,
        }
    }

    pub fn enrich(&self, entries: &mut [AgentActivityEntry]) {
        if !self.enabled {
            return;
        }
        let now = chrono::Utc::now().timestamp_millis();
        for entry in entries {
            let phase = match entry.event_name {
                Some(AgentEventName::ToolCall) => ToolInterceptPhase::PreToolUse,
                Some(AgentEventName::ToolResult) => ToolInterceptPhase::PostToolUse,
                _ => continue,
            };
            if !is_interceptor_agent(&entry.agent_type) {
                continue;
            }
            let Some(tool_use_id) = entry.tool_call_id.clone().filter(|value| !value.is_empty())
            else {
                continue;
            };
            let session_id = if entry.session_id.is_empty() {
                None
            } else {
                Some(entry.session_id.clone())
            };
            let action = self.verdict_store.get(
                &ToolVerdictKey {
                    session_id,
                    tool_use_id,
                    phase,
                },
                now,
            );
            let Some(action) = action else { continue };
            entry.guardrail_triggered = Some(true);
            entry.guardrail_action = Some(match action {
                crate::interceptor::ToolVerdictAction::Allow => GuardrailAction::Allow,
                crate::interceptor::ToolVerdictAction::Block => GuardrailAction::Block,
            });
        }
    }
}

fn is_interceptor_agent(agent_type: &str) -> bool {
    const QODER: &[&str] = &[
        "qoder",
        "qoder-cli",
        "qodercli",
        "qoder-idea",
        "qoder-jetbrains",
    ];
    let lower = agent_type.to_ascii_lowercase();
    QODER.contains(&lower.as_str())
        || agent_type == ClientType::OpenClaw.as_str()
        || agent_type == ClientType::QwenWorkCN.as_str()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::interceptor::ToolVerdictAction;

    #[test]
    fn stamps_qoder_tool_call_and_skips_other_agents() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(ToolVerdictStore::new(dir.path().join("verdicts.json")));
        let now = chrono::Utc::now().timestamp_millis();
        store.put(
            &ToolVerdictKey {
                session_id: Some("s1".into()),
                tool_use_id: "tool-1".into(),
                phase: ToolInterceptPhase::PreToolUse,
            },
            ToolVerdictAction::Block,
            now,
        );
        let linker = InterceptResultLinker::new(store, true);
        let mut entries = vec![
            AgentActivityEntry {
                event_name: Some(AgentEventName::ToolCall),
                agent_type: "Qoder-CLI".into(),
                session_id: "s1".into(),
                tool_call_id: Some("tool-1".into()),
                ..AgentActivityEntry::default()
            },
            AgentActivityEntry {
                event_name: Some(AgentEventName::ToolCall),
                agent_type: "cursor".into(),
                session_id: "s1".into(),
                tool_call_id: Some("tool-1".into()),
                ..AgentActivityEntry::default()
            },
        ];
        linker.enrich(&mut entries);
        assert_eq!(entries[0].guardrail_action, Some(GuardrailAction::Block));
        assert_eq!(entries[0].guardrail_triggered, Some(true));
        assert!(entries[1].guardrail_action.is_none());
    }

    #[test]
    fn disabled_linker_leaves_entries_alone() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(ToolVerdictStore::new(dir.path().join("verdicts.json")));
        let linker = InterceptResultLinker::new(store, false);
        let mut entries = vec![AgentActivityEntry {
            event_name: Some(AgentEventName::ToolResult),
            agent_type: ClientType::OpenClaw.as_str().into(),
            tool_call_id: Some("t".into()),
            ..AgentActivityEntry::default()
        }];
        linker.enrich(&mut entries);
        assert!(entries[0].guardrail_triggered.is_none());
    }
}
