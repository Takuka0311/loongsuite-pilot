//! Process-lifetime trace buffer counters. Removal means leaving the pending buffer.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct TraceRuntimeCounters {
    pub removed_buffers_total: u64,
    pub removed_logical_bytes_total: u64,
    pub removed_unmeasured_records_total: u64,
    pub converter_calls_total: u64,
    pub converter_duration_ms_total: u64,
    pub converter_failed_total: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TraceRuntimeSnapshot {
    pub agent_type: String,
    pub pending_buffers: u64,
    pub pending_records: u64,
    pub pending_logical_bytes: u64,
    pub pending_unmeasured_records: u64,
    pub largest_buffer_logical_bytes: u64,
    pub largest_buffer_records: u64,
    pub largest_buffer_age_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub largest_buffer_turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub largest_buffer_session_id: Option<String>,
    pub oldest_buffer_age_ms: u64,
    pub removed_buffers_total: u64,
    pub removed_logical_bytes_total: u64,
    pub removed_unmeasured_records_total: u64,
    pub converter_calls_total: u64,
    pub converter_duration_ms_total: u64,
    pub converter_failed_total: u64,
}
