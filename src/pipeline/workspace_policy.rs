//! Workspace exclusion policy used by file and Qoder API pipelines.
//!
//! The canonical JavaScript class still lives in
//! `assets/hooks/shared/workspace-policy.mjs` (core is not ported). This copy
//! matches the methods the pipeline calls: `enabled`, `allows`, and `filter`.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

#[derive(Debug)]
pub struct WorkspacePolicy {
    state_directory: Option<String>,
    roots: Vec<String>,
    blocked: HashSet<String>,
    fingerprint: String,
    pending_markers: HashSet<String>,
    next_retry_at: i64,
    next_warning_at: i64,
}

impl WorkspacePolicy {
    pub fn new(excluded: &[String], state_directory: Option<String>) -> Result<Self, String> {
        let validated = validate_excluded_workspaces(excluded)?;
        let roots: Vec<String> = validated
            .iter()
            .filter_map(|value| normalize_path(value))
            .collect();
        let mut sorted = roots.clone();
        sorted.sort();
        let fingerprint =
            sha256_hex(&serde_json::to_string(&sorted).unwrap_or_else(|_| "[]".into()));
        Ok(Self {
            state_directory,
            roots,
            blocked: HashSet::new(),
            fingerprint,
            pending_markers: HashSet::new(),
            next_retry_at: 0,
            next_warning_at: 0,
        })
    }

    pub fn enabled(&self) -> bool {
        !self.roots.is_empty()
    }

    pub fn allows(&mut self, record: &Map<String, Value>, fallback_agent: &str) -> bool {
        if self.roots.is_empty() {
            return true;
        }
        self.retry_pending_markers();
        let agent = record
            .get("gen_ai.agent.type")
            .and_then(Value::as_str)
            .unwrap_or(fallback_agent);
        let session = record
            .get("gen_ai.session.id")
            .or_else(|| record.get("session_id"))
            .or_else(|| record.get("sessionId"))
            .or_else(|| record.get("conversation_id"));
        let key = session
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(|session| sha256_hex(&format!("{agent}\0{session}")));
        let marker = key.as_ref().and_then(|key| {
            self.state_directory
                .as_ref()
                .map(|dir| format!("{dir}/{}/{key}", self.fingerprint))
        });
        if key.as_ref().is_some_and(|key| self.blocked.contains(key))
            || marker.as_ref().is_some_and(|path| Path::new(path).exists())
        {
            return false;
        }
        let mut directories = Vec::new();
        directories.extend(strings(record.get("workspace.path")));
        directories.extend(strings(record.get("cwd")));
        directories.extend(strings(record.get("workspace_roots")));
        for (name, value) in record {
            if agent_cwd_key(name) {
                directories.extend(strings(Some(value)));
            }
        }
        if !directories.iter().any(|directory| self.excludes(directory)) {
            return true;
        }
        if let Some(key) = key {
            self.blocked.insert(key);
            if let Some(marker) = marker {
                self.persist_marker(&marker);
            }
        }
        false
    }

    pub fn filter(
        &mut self,
        records: &[Map<String, Value>],
        agent: &str,
    ) -> Vec<Map<String, Value>> {
        if self.roots.is_empty() {
            return records.to_vec();
        }
        for record in records {
            let _ = self.allows(record, agent);
        }
        records
            .iter()
            .filter(|record| self.allows(record, agent))
            .cloned()
            .collect()
    }

    pub fn filter_string_rows(
        &mut self,
        rows: Vec<HashMap<String, String>>,
        agent: &str,
    ) -> Vec<HashMap<String, String>> {
        let maps: Vec<Map<String, Value>> = rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|(key, value)| (key.clone(), Value::String(value.clone())))
                    .collect()
            })
            .collect();
        for record in &maps {
            let _ = self.allows(record, agent);
        }
        rows.into_iter()
            .zip(maps)
            .filter(|(_, record)| self.allows(record, agent))
            .map(|(row, _)| row)
            .collect()
    }

    fn excludes(&self, directory: &str) -> bool {
        let Some(candidate) = normalize_path(directory) else {
            return false;
        };
        self.roots.iter().any(|root| {
            within(&candidate, root) || within(&real_path(&candidate), &real_path(root))
        })
    }

    fn persist_marker(&mut self, marker: &str) {
        let path = Path::new(marker);
        if let Some(parent) = path.parent() {
            if std::fs::create_dir_all(parent).is_ok() && std::fs::write(path, b"").is_ok() {
                self.pending_markers.remove(marker);
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
                }
                return;
            }
        }
        self.pending_markers.insert(marker.to_string());
        let now = now_ms();
        self.next_retry_at = now + 5_000;
        if now >= self.next_warning_at {
            self.next_warning_at = now + 60_000;
            let _ = std::io::Write::write_all(
                &mut std::io::stderr(),
                b"[workspace-privacy] Cannot persist session exclusions; blocked in this process, restart/other-process protection may be unavailable. Retrying on subsequent events.\n",
            );
        }
    }

    fn retry_pending_markers(&mut self) {
        if self.pending_markers.is_empty() || now_ms() < self.next_retry_at {
            return;
        }
        self.next_retry_at = now_ms() + 5_000;
        let batch: Vec<String> = self.pending_markers.iter().take(8).cloned().collect();
        for marker in batch {
            self.pending_markers.remove(&marker);
            self.persist_marker(&marker);
        }
    }
}

pub fn validate_excluded_workspaces(value: &[String]) -> Result<Vec<String>, String> {
    if value.iter().any(|path| !Path::new(path).is_absolute()) {
        return Err(
            "privacy.excludeWorkspaces must be an array of absolute local directories".to_string(),
        );
    }
    let mut unique = Vec::new();
    for path in value {
        if !unique.iter().any(|existing: &String| existing == path) {
            unique.push(path.clone());
        }
    }
    Ok(unique)
}

fn strings(value: Option<&Value>) -> Vec<String> {
    let Some(value) = value else {
        return Vec::new();
    };
    match value {
        Value::Array(items) => items
            .iter()
            .filter_map(|item| item.as_str().map(str::to_string))
            .collect(),
        Value::String(text) if text.is_empty() => Vec::new(),
        Value::String(text) if text.starts_with('[') => serde_json::from_str::<Value>(text)
            .ok()
            .map(|parsed| strings(Some(&parsed)))
            .unwrap_or_default(),
        Value::String(text) => vec![text.clone()],
        _ => Vec::new(),
    }
}

fn agent_cwd_key(name: &str) -> bool {
    let mut parts = name.split('.');
    parts.next() == Some("agent")
        && parts
            .next()
            .is_some_and(|part| !part.is_empty() && !part.contains('.'))
        && matches!(parts.next(), Some("cwd") | Some("workspace_roots"))
        && parts.next().is_none()
}

fn normalize_path(value: &str) -> Option<String> {
    let expanded = if let Some(rest) = value.strip_prefix("~/") {
        let home = std::env::var("HOME").unwrap_or_default();
        format!("{home}/{rest}")
    } else {
        value.to_string()
    };
    if !Path::new(&expanded).is_absolute() {
        return None;
    }
    Some(normalize_lexical(&expanded))
}

fn normalize_lexical(path: &str) -> String {
    let absolute = path.starts_with('/');
    let mut stack = Vec::new();
    for part in path.split(['/', '\\']) {
        if part.is_empty() || part == "." {
            continue;
        }
        if part == ".." {
            stack.pop();
            continue;
        }
        stack.push(part);
    }
    let joined = stack.join("/");
    if absolute {
        if joined.is_empty() {
            "/".into()
        } else {
            format!("/{joined}")
        }
    } else {
        joined
    }
}

fn real_path(value: &str) -> String {
    match std::fs::canonicalize(value) {
        Ok(path) => normalize_lexical(&path.to_string_lossy()),
        Err(_) => {
            let parent = Path::new(value)
                .parent()
                .map(|path| path.to_string_lossy().to_string())
                .unwrap_or_default();
            if parent == value || parent.is_empty() {
                value.to_string()
            } else {
                let base = Path::new(value)
                    .file_name()
                    .map(|name| name.to_string_lossy().to_string())
                    .unwrap_or_default();
                format!("{}/{}", real_path(&parent).trim_end_matches('/'), base)
            }
        }
    }
}

fn within(candidate: &str, root: &str) -> bool {
    candidate == root || candidate.starts_with(&format!("{root}/"))
}

fn sha256_hex(value: &str) -> String {
    hex::encode(Sha256::digest(value.as_bytes()))
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
