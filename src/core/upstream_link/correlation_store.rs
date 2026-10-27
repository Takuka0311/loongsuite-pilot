//! Reads `${dataDir}/acp-correlate/<sessionId>.jsonl`.
//! Port of `correlation-store.ts`.

use std::collections::{HashMap, HashSet};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::Value;

use crate::utils::content_hash;

use super::super::{fields, logger};

struct TurnRecord {
    content_hash: Option<String>,
    content_prefix: Option<String>,
    traceparent: String,
}

struct SessionState {
    mtime_ms: u128,
    turns: Vec<TurnRecord>,
    sessions: Vec<String>,
    consumed_turns: HashSet<usize>,
    session_consumed: bool,
    hash_index: HashMap<String, Vec<usize>>,
    hash_cursor: HashMap<String, usize>,
    last_access_ms: u128,
}

pub struct CorrelationStore {
    dir: PathBuf,
    states: std::sync::Mutex<HashMap<String, SessionState>>,
}

impl CorrelationStore {
    pub fn new(correlate_dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: correlate_dir.into(),
            states: std::sync::Mutex::new(HashMap::new()),
        }
    }

    pub fn resolve_turn(&self, session_id: &str, collected_text: &str) -> Option<String> {
        let dir = self.dir.clone();
        let mut states = self.states.lock().expect("states");
        let state = load_state(&mut states, &dir, session_id)?;
        if state.turns.is_empty() {
            return None;
        }
        let hash = content_hash(collected_text);
        if let Some(bucket) = state.hash_index.get(&hash).cloned() {
            let mut cursor = state.hash_cursor.get(&hash).copied().unwrap_or(0);
            while cursor < bucket.len() && state.consumed_turns.contains(&bucket[cursor]) {
                cursor += 1;
            }
            if cursor < bucket.len() {
                let index = bucket[cursor];
                state.consumed_turns.insert(index);
                state.hash_cursor.insert(hash, cursor + 1);
                return Some(state.turns[index].traceparent.clone());
            }
            state.hash_cursor.insert(hash, cursor);
        }
        for index in 0..state.turns.len() {
            if state.consumed_turns.contains(&index) {
                continue;
            }
            if let Some(prefix) = state.turns[index].content_prefix.clone() {
                if !prefix.is_empty() && collected_text.starts_with(&prefix) {
                    state.consumed_turns.insert(index);
                    return Some(state.turns[index].traceparent.clone());
                }
            }
        }
        None
    }

    pub fn resolve_session_first(&self, session_id: &str) -> Option<String> {
        let dir = self.dir.clone();
        let traceparent = {
            let mut states = self.states.lock().expect("states");
            let state = load_state(&mut states, &dir, session_id)?;
            if state.sessions.is_empty() || state.session_consumed {
                return None;
            }
            state.session_consumed = true;
            state.sessions[0].clone()
        };
        if !self.claim_session(session_id) {
            return None;
        }
        Some(traceparent)
    }

    pub fn mark_session_consumed(&self, session_id: &str) {
        let dir = self.dir.clone();
        let should_claim = {
            let mut states = self.states.lock().expect("states");
            let Some(state) = load_state(&mut states, &dir, session_id) else {
                return;
            };
            if state.sessions.is_empty() || state.session_consumed {
                return;
            }
            state.session_consumed = true;
            true
        };
        if should_claim {
            let _ = self.claim_session(session_id);
        }
    }

    pub fn has_session(&self, session_id: &str) -> bool {
        self.session_path(session_id).is_file()
    }

    pub fn prune_idle(&self, cutoff_ms: u128) -> usize {
        let mut states = self.states.lock().expect("states");
        let before = states.len();
        states.retain(|_, state| state.last_access_ms >= cutoff_ms);
        before - states.len()
    }
}

fn load_state<'a>(
    states: &'a mut HashMap<String, SessionState>,
    dir: &Path,
    session_id: &str,
) -> Option<&'a mut SessionState> {
    let path = dir.join(format!("{}.jsonl", safe_name(session_id)));
    let meta = std::fs::metadata(&path).ok()?;
    let mtime = mtime_ms(&meta);
    if states
        .get(session_id)
        .is_some_and(|existing| existing.mtime_ms == mtime)
    {
        states.get_mut(session_id).expect("state").last_access_ms = now_ms();
        return states.get_mut(session_id);
    }
    let raw = std::fs::read_to_string(&path).unwrap_or_default();
    let mut turns = Vec::new();
    let mut sessions = Vec::new();
    for line in raw.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let traceparent = value
            .get("traceparent")
            .and_then(Value::as_str)
            .map(str::to_string);
        let Some(traceparent) = traceparent else {
            continue;
        };
        match value.get("type").and_then(Value::as_str) {
            Some("turn") => turns.push(TurnRecord {
                content_hash: value
                    .get("contentHash")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                content_prefix: value
                    .get("contentPrefix")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                traceparent,
            }),
            Some("session") => sessions.push(traceparent),
            _ => {}
        }
    }
    let previous = states.remove(session_id);
    let mut hash_index = HashMap::new();
    for (index, turn) in turns.iter().enumerate() {
        if let Some(hash) = &turn.content_hash {
            hash_index
                .entry(hash.clone())
                .or_insert_with(Vec::new)
                .push(index);
        }
    }
    states.insert(
        session_id.to_string(),
        SessionState {
            mtime_ms: mtime,
            turns,
            sessions,
            consumed_turns: previous
                .as_ref()
                .map(|state| state.consumed_turns.clone())
                .unwrap_or_default(),
            session_consumed: previous
                .as_ref()
                .map(|state| state.session_consumed)
                .unwrap_or_else(|| {
                    dir.join(format!("{}.session-context.done", safe_name(session_id)))
                        .is_file()
                }),
            hash_index,
            hash_cursor: HashMap::new(),
            last_access_ms: now_ms(),
        },
    );
    states.get_mut(session_id)
}

impl CorrelationStore {
    fn claim_session(&self, session_id: &str) -> bool {
        let path = self.session_done_path(session_id);
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(mut file) => {
                let _ = file.write_all(serde_json::json!({ "sessionId": session_id, "ts": chrono::Utc::now().to_rfc3339() }).to_string().as_bytes());
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
                }
                true
            }
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => false,
            Err(err) => {
                logger("correlation-store").warn(
                    "failed to persist session context consumption",
                    Some(&fields(
                        serde_json::json!({ "sessionId": session_id, "error": err.to_string() }),
                    )),
                );
                true
            }
        }
    }

    fn session_path(&self, session_id: &str) -> PathBuf {
        self.dir.join(format!("{}.jsonl", safe_name(session_id)))
    }

    fn session_done_path(&self, session_id: &str) -> PathBuf {
        self.dir
            .join(format!("{}.session-context.done", safe_name(session_id)))
    }
}

fn safe_name(value: &str) -> String {
    let base = Path::new(value)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(value);
    let cleaned: String = base
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
                ch
            } else {
                '_'
            }
        })
        .collect();
    if cleaned.is_empty() {
        "unknown".into()
    } else {
        cleaned
    }
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0)
}

fn mtime_ms(meta: &std::fs::Metadata) -> u128 {
    meta.modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_millis())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_hash_is_consumed_once_and_prefix_is_the_fallback() {
        let dir = tempfile::tempdir().unwrap();
        let text = "hello upstream";
        let hash = content_hash(text);
        let path = dir.path().join("sess.jsonl");
        std::fs::write(&path, format!(
            "{}\n{}\n",
            serde_json::json!({ "type": "turn", "contentHash": hash, "traceparent": "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01" }),
            serde_json::json!({ "type": "turn", "contentPrefix": "rewrite", "traceparent": "00-cccccccccccccccccccccccccccccccc-dddddddddddddddd-01" }),
        )).unwrap();
        let store = CorrelationStore::new(dir.path());
        assert_eq!(
            store.resolve_turn("sess", text).as_deref(),
            Some("00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01")
        );
        assert!(store.resolve_turn("sess", text).is_none());
        assert_eq!(
            store.resolve_turn("sess", "rewrite me").as_deref(),
            Some("00-cccccccccccccccccccccccccccccccc-dddddddddddddddd-01")
        );
        assert!(store.has_session("sess"));
    }
}
