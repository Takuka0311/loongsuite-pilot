//! Port of `src/flushers/base-flusher.ts`.

use async_trait::async_trait;
use serde_json::{Map, Value};

use crate::types::AgentActivityEntry;

#[derive(Debug)]
pub struct FlushError {
    pub message: String,
}

impl FlushError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for FlushError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for FlushError {}

/// Process-lifetime totals. Removal means leaving the pending buffer.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TraceRuntimeCounters {
    pub removed_buffers_total: u64,
    pub removed_logical_bytes_total: f64,
    pub removed_unmeasured_records_total: u64,
    pub converter_calls_total: u64,
    pub converter_duration_ms_total: f64,
    pub converter_failed_total: u64,
}

/// A periodic view of existing buffers only.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TraceRuntimeSnapshot {
    pub removed_buffers_total: u64,
    pub removed_logical_bytes_total: f64,
    pub removed_unmeasured_records_total: u64,
    pub converter_calls_total: u64,
    pub converter_duration_ms_total: f64,
    pub converter_failed_total: u64,
    pub agent_type: String,
    pub pending_buffers: u64,
    pub pending_records: u64,
    pub pending_logical_bytes: f64,
    pub pending_unmeasured_records: u64,
    pub largest_buffer_logical_bytes: f64,
    pub largest_buffer_records: u64,
    pub largest_buffer_age_ms: u64,
    pub largest_buffer_turn_id: Option<String>,
    pub largest_buffer_session_id: Option<String>,
    pub oldest_buffer_age_ms: u64,
}

impl TraceRuntimeSnapshot {
    pub fn from_counters(agent_type: &str, counters: &TraceRuntimeCounters) -> Self {
        Self {
            removed_buffers_total: counters.removed_buffers_total,
            removed_logical_bytes_total: counters.removed_logical_bytes_total,
            removed_unmeasured_records_total: counters.removed_unmeasured_records_total,
            converter_calls_total: counters.converter_calls_total,
            converter_duration_ms_total: counters.converter_duration_ms_total,
            converter_failed_total: counters.converter_failed_total,
            agent_type: agent_type.to_string(),
            ..Self::default()
        }
    }
}

#[async_trait]
pub trait BaseFlusher: Send + Sync {
    fn name(&self) -> &str;

    async fn send(
        &self,
        entry: AgentActivityEntry,
        logical_bytes: Option<f64>,
    ) -> Result<(), FlushError>;

    async fn send_batch(
        &self,
        entries: Vec<AgentActivityEntry>,
        logical_bytes: Option<Vec<f64>>,
    ) -> Result<(), FlushError>;

    async fn flush(&self) -> Result<(), FlushError>;

    async fn shutdown(&self) -> Result<(), FlushError>;

    fn trace_runtime_snapshot(&self) -> Vec<TraceRuntimeSnapshot> {
        Vec::new()
    }

    async fn start(&self) -> Result<(), FlushError> {
        Ok(())
    }

    async fn send_raw(
        &self,
        _topic: &str,
        _payload: &Map<String, Value>,
    ) -> Result<(), FlushError> {
        Ok(())
    }
}
