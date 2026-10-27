//! Running-status sample. The public sender calls this only when the
//! `proprietary` feature is enabled. The default build does not post it.

use crate::internal::webtracking_post::build_web_tracking_url;
#[cfg(all(feature = "proprietary", not(test)))]
use crate::internal::webtracking_post::post_web_tracking;
#[cfg(any(feature = "proprietary", test))]
use indexmap::IndexMap;
#[cfg(any(feature = "proprietary", test))]
use serde_json::{json, Map, Value};
#[cfg(any(feature = "proprietary", test))]
use std::sync::atomic::{AtomicU64, Ordering};

const ENDPOINT: &str = "https://cn-shanghai.log.aliyuncs.com";
const PROJECT: &str = "loongsuite-community-edition";
const LOGSTORE: &str = "loongsuite-online";

/// L1 is collected every 10 minutes; send once per 12 hours → 72 intervals.
#[cfg(any(feature = "proprietary", test))]
const SEND_INTERVAL_COUNT: u64 = 72;

#[cfg(any(feature = "proprietary", test))]
const SELECTED_FIELDS: &[&str] = &[
    "cpu",
    "mem",
    "version",
    "instance_id",
    "ip",
    "hostname",
    "os_detail",
    "metric_json",
];

#[cfg(any(feature = "proprietary", test))]
static CALL_COUNT: AtomicU64 = AtomicU64::new(0);

pub fn status_url() -> String {
    build_web_tracking_url(ENDPOINT, PROJECT, LOGSTORE)
}

/// Returns the payload that would be posted, or `None` when this call falls
/// between the 12-hour samples.
#[cfg(any(feature = "proprietary", test))]
pub fn next_running_status_payload(data: &IndexMap<String, String>) -> Option<Value> {
    let call = CALL_COUNT.fetch_add(1, Ordering::SeqCst);
    if !call.is_multiple_of(SEND_INTERVAL_COUNT) {
        return None;
    }
    let mut status = Map::new();
    for key in SELECTED_FIELDS {
        if let Some(value) = data.get(*key) {
            status.insert((*key).to_string(), Value::String(value.clone()));
        }
    }
    Some(json!({
        "__topic__": "pilot_running_status",
        "__logs__": [Value::Object(status)],
    }))
}

#[cfg(all(feature = "proprietary", not(test)))]
pub fn send_running_status(data: &IndexMap<String, String>) {
    let Some(body) = next_running_status_payload(data) else {
        return;
    };
    let url = status_url();
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        handle.spawn(async move {
            post_web_tracking(&url, &body, Some("running-status")).await;
        });
        return;
    }
    std::thread::spawn(move || {
        if let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            runtime.block_on(post_web_tracking(&url, &body, Some("running-status")));
        }
    });
}

#[cfg(test)]
pub fn reset_call_count() {
    CALL_COUNT.store(0, Ordering::SeqCst);
}

#[cfg(test)]
mod tests {
    use super::*;
    use indexmap::IndexMap;

    #[test]
    fn sends_the_selected_fields_once_per_72_calls() {
        reset_call_count();
        let mut data = IndexMap::new();
        data.insert("cpu".into(), "1".into());
        data.insert("mem".into(), "2".into());
        data.insert("version".into(), "1.2.0".into());
        data.insert("ignored".into(), "no".into());
        let first = next_running_status_payload(&data).unwrap();
        assert_eq!(first["__topic__"], "pilot_running_status");
        assert_eq!(first["__logs__"][0]["cpu"], "1");
        assert_eq!(first["__logs__"][0]["version"], "1.2.0");
        assert!(first["__logs__"][0].get("ignored").is_none());
        for _ in 1..SEND_INTERVAL_COUNT {
            assert!(next_running_status_payload(&data).is_none());
        }
        assert!(next_running_status_payload(&data).is_some());
        reset_call_count();
    }
}
