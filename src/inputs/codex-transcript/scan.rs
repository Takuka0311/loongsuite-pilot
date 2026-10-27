//! Byte-offset JSONL scanning. Port of the readers in `codex-transcript-input.ts`.

use std::io::SeekFrom;

use serde_json::{Map, Value};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use crate::inputs::base::{CommittedInputBatch, InputRuntime, InputRuntimeAccumulator};

use super::extractor::extract_codex_transcript_meta;
use super::utils::string_value;

pub const READ_CHUNK_SIZE: usize = 1024 * 1024;

#[derive(Clone, Debug)]
pub struct JsonLine {
    pub start_offset: i64,
    pub end_offset: i64,
    pub record: Map<String, Value>,
}

#[derive(Clone, Debug)]
pub struct ScanStats {
    pub next_offset: i64,
    pub records: u64,
    pub parse_success_records: u64,
    pub parse_failed_records: u64,
    pub max_record_bytes: u64,
}

pub struct ReadLines {
    pub items: Vec<JsonLine>,
    pub next_offset: i64,
}

fn observe_read(runtime: Option<&InputRuntime>, bytes_read: u64, requested: u64, started: f64) {
    let Some(runtime) = runtime else {
        return;
    };
    let duration = InputRuntimeAccumulator::now() - started;
    runtime.with_accumulator(|accumulator| {
        accumulator.observe_read(bytes_read, requested, duration);
    });
}

pub fn observe_backlog(runtime: &InputRuntime, bytes: u64) {
    runtime.with_accumulator(|accumulator| accumulator.observe_backlog(bytes));
}

pub fn observe_batch(runtime: &InputRuntime, batch: CommittedInputBatch) {
    runtime.with_accumulator(|accumulator| accumulator.observe_committed_batch(batch));
}

/// `false` from `on_line` stops the scan at that line, matching the TypeScript scanner.
pub async fn scan_json_lines(
    file_path: &str,
    start_offset: i64,
    end_offset: i64,
    runtime: Option<&InputRuntime>,
    mut on_line: impl FnMut(JsonLine) -> bool,
) -> anyhow::Result<ScanStats> {
    if end_offset <= start_offset {
        return Ok(ScanStats {
            next_offset: start_offset,
            records: 0,
            parse_success_records: 0,
            parse_failed_records: 0,
            max_record_bytes: 0,
        });
    }
    let mut file = tokio::fs::File::open(file_path).await?;
    let mut next_offset = start_offset;
    let mut position = start_offset;
    let mut pending: Vec<u8> = Vec::new();
    let mut pending_start = start_offset;
    let mut records = 0u64;
    let mut parse_success = 0u64;
    let mut parse_failed = 0u64;
    let mut max_record_bytes = 0u64;

    while position < end_offset {
        let length = ((end_offset - position) as usize).min(READ_CHUNK_SIZE);
        let mut chunk = vec![0u8; length];
        file.seek(SeekFrom::Start(position as u64)).await?;
        let started = InputRuntimeAccumulator::now();
        let bytes_read = file.read(&mut chunk).await?;
        observe_read(runtime, bytes_read as u64, length as u64, started);
        if bytes_read == 0 {
            break;
        }
        position += bytes_read as i64;
        let mut data = if pending.is_empty() {
            chunk[..bytes_read].to_vec()
        } else {
            let mut combined = std::mem::take(&mut pending);
            combined.extend_from_slice(&chunk[..bytes_read]);
            combined
        };
        let data_start = pending_start;
        let mut cursor = 0usize;
        while cursor < data.len() {
            let Some(relative) = data[cursor..].iter().position(|byte| *byte == b'\n') else {
                break;
            };
            let newline = cursor + relative;
            let text = String::from_utf8_lossy(&data[cursor..newline]);
            let text = text.trim();
            if !text.is_empty() {
                records += 1;
                max_record_bytes = max_record_bytes.max((newline - cursor + 1) as u64);
                match serde_json::from_str::<Value>(text) {
                    Ok(Value::Object(record)) => {
                        parse_success += 1;
                        let keep_going = on_line(JsonLine {
                            start_offset: data_start + cursor as i64,
                            end_offset: data_start + newline as i64 + 1,
                            record,
                        });
                        if !keep_going {
                            return Ok(ScanStats {
                                next_offset: data_start + newline as i64 + 1,
                                records,
                                parse_success_records: parse_success,
                                parse_failed_records: parse_failed,
                                max_record_bytes,
                            });
                        }
                    }
                    _ => parse_failed += 1,
                }
            }
            next_offset = data_start + newline as i64 + 1;
            cursor = newline + 1;
        }
        if cursor < data.len() {
            data.drain(..cursor);
            pending = data;
        } else {
            pending.clear();
        }
        pending_start = data_start + cursor as i64;
    }

    Ok(ScanStats {
        next_offset,
        records,
        parse_success_records: parse_success,
        parse_failed_records: parse_failed,
        max_record_bytes,
    })
}

pub async fn read_json_lines(
    file_path: &str,
    start_offset: i64,
    end_offset: i64,
    runtime: Option<&InputRuntime>,
) -> anyhow::Result<ReadLines> {
    if end_offset <= start_offset {
        return Ok(ReadLines {
            items: Vec::new(),
            next_offset: start_offset,
        });
    }
    let mut items = Vec::new();
    let stats = scan_json_lines(file_path, start_offset, end_offset, runtime, |line| {
        items.push(line);
        true
    })
    .await?;
    Ok(ReadLines {
        items,
        next_offset: stats.next_offset,
    })
}

pub async fn last_complete_jsonl_offset(
    file_path: &str,
    file_size: i64,
    runtime: Option<&InputRuntime>,
) -> anyhow::Result<i64> {
    if file_size <= 0 {
        return Ok(0);
    }
    let mut file = tokio::fs::File::open(file_path).await?;
    let mut last = [0u8; 1];
    file.seek(SeekFrom::Start((file_size - 1) as u64)).await?;
    let started = InputRuntimeAccumulator::now();
    let bytes_read = file.read(&mut last).await? as u64;
    observe_read(runtime, bytes_read, 1, started);
    if bytes_read == 1 && last[0] == b'\n' {
        return Ok(file_size);
    }
    let mut position = file_size;
    while position > 0 {
        let length = (position as usize).min(READ_CHUNK_SIZE);
        position -= length as i64;
        let mut chunk = vec![0u8; length];
        file.seek(SeekFrom::Start(position as u64)).await?;
        let started = InputRuntimeAccumulator::now();
        let bytes_read = file.read(&mut chunk).await?;
        observe_read(runtime, bytes_read as u64, length as u64, started);
        if bytes_read == 0 {
            break;
        }
        if let Some(newline) = chunk[..bytes_read].iter().rposition(|byte| *byte == b'\n') {
            return Ok(position + newline as i64 + 1);
        }
    }
    Ok(0)
}

pub async fn read_json_line_at(
    file_path: &str,
    offset: i64,
    runtime: Option<&InputRuntime>,
) -> anyhow::Result<Option<Map<String, Value>>> {
    let mut file = tokio::fs::File::open(file_path).await?;
    let mut chunks: Vec<u8> = Vec::new();
    let mut position = offset.max(0);
    for _ in 0..16 {
        let mut buffer = vec![0u8; 64 * 1024];
        file.seek(SeekFrom::Start(position as u64)).await?;
        let started = InputRuntimeAccumulator::now();
        let bytes_read = file.read(&mut buffer).await?;
        observe_read(runtime, bytes_read as u64, buffer.len() as u64, started);
        if bytes_read == 0 {
            break;
        }
        let newline = buffer[..bytes_read].iter().position(|byte| *byte == b'\n');
        let take = newline.unwrap_or(bytes_read);
        chunks.extend_from_slice(&buffer[..take]);
        if newline.is_some() {
            let text = String::from_utf8_lossy(&chunks);
            return Ok(match serde_json::from_str::<Value>(text.trim()) {
                Ok(Value::Object(record)) => Some(record),
                _ => None,
            });
        }
        position += bytes_read as i64;
    }
    Ok(None)
}

pub async fn find_owner_session_meta_offset(
    file_path: &str,
    end_offset: i64,
    runtime: Option<&InputRuntime>,
) -> anyhow::Result<Option<i64>> {
    let mut owner_offset = None;
    let mut saw_session_meta = false;
    let mut stop = false;
    scan_json_lines(file_path, 0, end_offset, runtime, |line| {
        if stop {
            return false;
        }
        if line.record.get("type").and_then(|value| value.as_str()) != Some("session_meta") {
            if saw_session_meta {
                stop = true;
                return false;
            }
            return true;
        }
        saw_session_meta = true;
        owner_offset = select_owner_session_meta_offset(file_path, owner_offset, &line);
        true
    })
    .await?;
    Ok(owner_offset)
}

pub fn select_owner_session_meta_offset(
    file_path: &str,
    current_offset: Option<i64>,
    line: &JsonLine,
) -> Option<i64> {
    let Some(meta) = extract_codex_transcript_meta(&line.record) else {
        return current_offset;
    };
    let rollout_thread_id = super::extractor::session_id_from_transcript_path(file_path);
    if meta.thread_id == rollout_thread_id {
        Some(line.start_offset)
    } else {
        current_offset.or(Some(line.start_offset))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ForkAnchorKind {
    InitialHook,
    UuidV7,
    TerminalRecovery,
}

impl ForkAnchorKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InitialHook => "initial-hook",
            Self::UuidV7 => "uuidv7",
            Self::TerminalRecovery => "terminal-recovery",
        }
    }
}

pub struct TurnStart {
    pub start_offset: Option<i64>,
    pub next_offset: i64,
    pub turn_id: Option<String>,
    pub anchor_kind: Option<ForkAnchorKind>,
}

pub fn bounded_search_offset(value: i64, file_size: i64) -> i64 {
    if value < 0 || value > file_size {
        0
    } else {
        value
    }
}

pub async fn find_turn_start_offset(
    file_path: &str,
    owner_thread_id: &str,
    initial_turn_id: Option<&str>,
    recovery_turn_id: Option<&str>,
    start_offset: i64,
    file_size: i64,
    runtime: Option<&InputRuntime>,
) -> anyhow::Result<TurnStart> {
    let mut turn_start = None;
    let mut matched = None;
    let mut anchor = None;
    let safe_start = bounded_search_offset(start_offset, file_size);
    let scan_end = file_size.min(safe_start.saturating_add(16 * 1024 * 1024));
    let owner_timestamp = uuid_v7_timestamp_ms(owner_thread_id);
    let stats = scan_json_lines(file_path, safe_start, scan_end, runtime, |line| {
        let Some(payload) = line
            .record
            .get("payload")
            .and_then(serde_json::Value::as_object)
        else {
            return true;
        };
        let Some(candidate) = turn_id_for_start(&line.record, payload) else {
            return true;
        };
        if initial_turn_id.is_some_and(|id| id == candidate) {
            turn_start = Some(line.start_offset);
            matched = Some(candidate);
            anchor = Some(ForkAnchorKind::InitialHook);
            return false;
        }
        if initial_turn_id.is_some() {
            return true;
        }
        let candidate_timestamp = uuid_v7_timestamp_ms(&candidate);
        if owner_timestamp.is_some()
            && candidate_timestamp.is_some()
            && candidate_timestamp >= owner_timestamp
        {
            turn_start = Some(line.start_offset);
            matched = Some(candidate);
            anchor = Some(ForkAnchorKind::UuidV7);
            return false;
        }
        if recovery_turn_id.is_some_and(|id| id == candidate) {
            turn_start = Some(line.start_offset);
            matched = Some(candidate);
            anchor = Some(ForkAnchorKind::TerminalRecovery);
            return false;
        }
        true
    })
    .await?;
    Ok(TurnStart {
        start_offset: turn_start,
        next_offset: stats.next_offset,
        turn_id: matched,
        anchor_kind: anchor,
    })
}

pub fn turn_id_for_start(
    record: &Map<String, Value>,
    payload: &Map<String, Value>,
) -> Option<String> {
    let record_type = record.get("type").and_then(|value| value.as_str());
    let payload_type = payload.get("type").and_then(|value| value.as_str());
    if record_type != Some("turn_context")
        && !(record_type == Some("event_msg") && payload_type == Some("task_started"))
    {
        return None;
    }
    string_value(payload.get("turn_id")).map(str::to_owned)
}

pub fn terminal_turn_id_for(
    record: &Map<String, Value>,
    payload: &Map<String, Value>,
) -> Option<String> {
    if record.get("type").and_then(|value| value.as_str()) != Some("event_msg") {
        return None;
    }
    let kind = payload.get("type").and_then(|value| value.as_str());
    if kind != Some("task_complete") && kind != Some("turn_aborted") {
        return None;
    }
    string_value(payload.get("turn_id")).map(str::to_owned)
}

static UUID_V7_RE: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
    regex::Regex::new(r"(?i)^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")
        .expect("uuid v7")
});

pub fn uuid_v7_timestamp_ms(value: &str) -> Option<f64> {
    if !UUID_V7_RE.is_match(value) {
        return None;
    }
    let hex = value.replace('-', "");
    let prefix = hex.get(..12)?;
    let timestamp = u64::from_str_radix(prefix, 16).ok()?;
    Some(timestamp as f64)
}

pub fn file_meta_snapshot(meta: &std::fs::Metadata) -> (u64, i64, i64) {
    let ino = {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            meta.ino()
        }
        #[cfg(not(unix))]
        {
            0
        }
    };
    let size = i64::try_from(meta.len()).unwrap_or(i64::MAX);
    let mtime = meta
        .modified()
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0);
    (ino, size, mtime)
}
