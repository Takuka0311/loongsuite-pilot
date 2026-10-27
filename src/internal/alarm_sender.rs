//! Open-source alarm transport.
//!
//! The proprietary build aliases this module to an internal sender. That sender
//! is not part of this tree, so these functions stay empty even when the
//! `proprietary` feature is enabled. `sender` still routes here only under that
//! feature; the default build does not call them.

pub fn send_alarm(_topic: &str, _data: &indexmap::IndexMap<String, String>) {}

pub fn send_status(_topic: &str, _data: &indexmap::IndexMap<String, String>) {}

/// Awaitable alarm send used by hook install. The open-source build has nowhere
/// to deliver it and reports that immediately.
pub async fn post_alarm(
    _topic: &str,
    _data: &indexmap::IndexMap<String, String>,
    _timeout_ms: u64,
) -> bool {
    false
}
