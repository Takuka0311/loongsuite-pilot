//! Bag-of-fields view of [`AgentActivityEntry`].
//!
//! TypeScript treats an entry as a plain object with dotted keys. Round-tripping
//! through the normalization JSON map keeps those keys on the typed fields when
//! they belong there and in `extra` otherwise.

use crate::normalization::{entry_from_map, entry_to_object};
use crate::types::{AgentActivityEntry, AgentEventName};
use serde_json::{Map, Value};

pub fn entry_map(entry: &AgentActivityEntry) -> Map<String, Value> {
    entry_to_object(entry)
}

pub fn entry_from(map: Map<String, Value>) -> AgentActivityEntry {
    entry_from_map(map)
}

pub fn update_entry(entry: &mut AgentActivityEntry, edit: impl FnOnce(&mut Map<String, Value>)) {
    let mut map = entry_map(entry);
    edit(&mut map);
    *entry = entry_from(map);
}

pub fn entry_get<'a>(map: &'a Map<String, Value>, key: &str) -> Option<&'a Value> {
    map.get(key)
}

pub fn string_field(entry: &AgentActivityEntry, key: &str) -> Option<String> {
    entry_map(entry)
        .get(key)
        .and_then(Value::as_str)
        .map(str::to_owned)
}

pub fn event_name(entry: &AgentActivityEntry) -> Option<&'static str> {
    entry.event_name.map(AgentEventName::as_str)
}

pub fn is_event(entry: &AgentActivityEntry, name: &str) -> bool {
    event_name(entry) == Some(name)
}

pub fn non_empty_string(value: Option<&Value>) -> Option<&str> {
    match value {
        Some(Value::String(text)) if !text.is_empty() => Some(text.as_str()),
        _ => None,
    }
}

pub fn finite_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64().filter(|n| n.is_finite()),
        Value::String(text) => text.parse::<f64>().ok().filter(|n| n.is_finite()),
        _ => None,
    }
}

pub fn positive_number(value: Option<&Value>) -> bool {
    value.and_then(finite_f64).is_some_and(|n| n > 0.0)
}

/// `buildTestEntry` stores the legacy `filePath` override on the object.
pub fn legacy_file_path(entry: &AgentActivityEntry) -> Option<&str> {
    entry
        .extra
        .get("filePath")
        .and_then(Value::as_str)
        .or(entry.get("agent.file_path").and_then(Value::as_str))
}
