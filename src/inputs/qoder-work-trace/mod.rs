//! Hook JSONL + segment trace input for Qoder Work.
//!
//! Segment FIFO pairing, subagent filtering, and the legacy SDK token fallback
//! follow `qoder-work-trace-input.ts`. The SDK token map and loop-id iteration
//! state machine are the parts most likely to drift; see the module tests and
//! the gap note in the parent report.

mod qoder_work_trace_input;

pub use qoder_work_trace_input::{QoderWorkTraceInput, QoderWorkTraceOptions};
