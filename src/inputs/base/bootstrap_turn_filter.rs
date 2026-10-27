//! Port of `src/inputs/base/bootstrap-turn-filter.ts`.

use crate::types::AgentActivityEntry;
use serde_json::Value;
use std::collections::HashMap;

pub fn filter_bootstrap_history_turns(entries: Vec<AgentActivityEntry>) -> Vec<AgentActivityEntry> {
    let mut latest_turn_by_batch: HashMap<String, String> = HashMap::new();
    for entry in &entries {
        if entry_str(entry, "agent.transcript.cursor_mode") != Some("bootstrap") {
            continue;
        }
        let Some(batch_id) = non_empty(entry_str(entry, "agent.transcript.cursor_batch_id")) else {
            continue;
        };
        let Some(turn_id) = non_empty(entry.turn_id.as_deref()) else {
            continue;
        };
        latest_turn_by_batch.insert(batch_id.to_owned(), turn_id.to_owned());
    }
    if latest_turn_by_batch.is_empty() {
        return entries;
    }
    entries
        .into_iter()
        .filter(|entry| {
            if entry_str(entry, "agent.transcript.cursor_mode") != Some("bootstrap") {
                return true;
            }
            let batch_id = non_empty(entry_str(entry, "agent.transcript.cursor_batch_id"));
            let turn_id = non_empty(entry.turn_id.as_deref());
            let (Some(batch_id), Some(turn_id)) = (batch_id, turn_id) else {
                return true;
            };
            latest_turn_by_batch.get(batch_id) == Some(&turn_id.to_owned())
        })
        .collect()
}

fn entry_str<'a>(entry: &'a AgentActivityEntry, key: &str) -> Option<&'a str> {
    entry.extra.get(key).and_then(Value::as_str)
}

fn non_empty(value: Option<&str>) -> Option<&str> {
    value.filter(|text| !text.is_empty())
}
