//! Port of `src/flushers/multi-flusher.ts`.
//!
//! Child failures are logged and do not fail the fan-out. Each child receives
//! its own clone of the entry. The TypeScript flusher shares one object, so a
//! child that mutates an entry during `send` can be observed by a sibling that
//! has not finished. The Rust port does not share that mutation.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Map, Value};

use super::base_flusher::{BaseFlusher, FlushError, TraceRuntimeSnapshot};
use crate::types::AgentActivityEntry;
use crate::utils::create_logger;

pub struct MultiFlusher {
    flushers: Vec<Arc<dyn BaseFlusher>>,
}

impl MultiFlusher {
    pub fn new(flushers: Vec<Arc<dyn BaseFlusher>>) -> Self {
        Self { flushers }
    }

    pub fn flushers(&self) -> &[Arc<dyn BaseFlusher>] {
        &self.flushers
    }
}

#[async_trait]
impl BaseFlusher for MultiFlusher {
    fn name(&self) -> &str {
        "multi"
    }

    async fn send(
        &self,
        entry: AgentActivityEntry,
        logical_bytes: Option<f64>,
    ) -> Result<(), FlushError> {
        let mut set = tokio::task::JoinSet::new();
        for (index, flusher) in self.flushers.iter().cloned().enumerate() {
            let entry = entry.clone();
            set.spawn(async move {
                let name = flusher.name().to_string();
                (index, name, flusher.send(entry, logical_bytes).await)
            });
        }
        let logger = create_logger("MultiFlusher");
        while let Some(joined) = set.join_next().await {
            match joined {
                Ok((_, name, Err(error))) => {
                    logger.error(
                        "flusher send failed",
                        Some(&meta(&[
                            ("flusher", Value::String(name)),
                            ("error", Value::String(error.to_string())),
                        ])),
                    );
                }
                Ok((_, _, Ok(()))) => {}
                Err(error) => {
                    logger.error(
                        "flusher send failed",
                        Some(&meta(&[("error", Value::String(error.to_string()))])),
                    );
                }
            }
        }
        Ok(())
    }

    async fn send_batch(
        &self,
        entries: Vec<AgentActivityEntry>,
        logical_bytes: Option<Vec<f64>>,
    ) -> Result<(), FlushError> {
        let mut set = tokio::task::JoinSet::new();
        for flusher in &self.flushers {
            let flusher = Arc::clone(flusher);
            let entries = entries.clone();
            let logical_bytes = logical_bytes.clone();
            set.spawn(async move {
                let name = flusher.name().to_string();
                (name, flusher.send_batch(entries, logical_bytes).await)
            });
        }
        let logger = create_logger("MultiFlusher");
        while let Some(joined) = set.join_next().await {
            if let Ok((name, Err(error))) = joined {
                logger.error(
                    "flusher sendBatch failed",
                    Some(&meta(&[
                        ("flusher", Value::String(name)),
                        ("error", Value::String(error.to_string())),
                    ])),
                );
            }
        }
        Ok(())
    }

    async fn flush(&self) -> Result<(), FlushError> {
        let mut set = tokio::task::JoinSet::new();
        for flusher in &self.flushers {
            let flusher = Arc::clone(flusher);
            set.spawn(async move { flusher.flush().await });
        }
        while set.join_next().await.is_some() {}
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), FlushError> {
        let mut set = tokio::task::JoinSet::new();
        for flusher in &self.flushers {
            let flusher = Arc::clone(flusher);
            set.spawn(async move { flusher.shutdown().await });
        }
        while set.join_next().await.is_some() {}
        Ok(())
    }

    fn trace_runtime_snapshot(&self) -> Vec<TraceRuntimeSnapshot> {
        self.flushers
            .iter()
            .flat_map(|flusher| flusher.trace_runtime_snapshot())
            .collect()
    }

    async fn send_raw(&self, topic: &str, payload: &Map<String, Value>) -> Result<(), FlushError> {
        let mut set = tokio::task::JoinSet::new();
        for flusher in &self.flushers {
            let flusher = Arc::clone(flusher);
            let topic = topic.to_string();
            let payload = payload.clone();
            set.spawn(async move { flusher.send_raw(&topic, &payload).await });
        }
        while set.join_next().await.is_some() {}
        Ok(())
    }
}

fn meta(pairs: &[(&str, Value)]) -> Map<String, Value> {
    pairs
        .iter()
        .map(|(key, value)| ((*key).to_string(), value.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{BaseFlusher, FlushError, MultiFlusher};
    use crate::types::AgentActivityEntry;
    use async_trait::async_trait;
    use serde_json::{json, Map, Value};
    use std::sync::{Arc, Mutex};

    struct MockFlusher {
        name: String,
        should_fail: Mutex<bool>,
        sends: Mutex<usize>,
        batches: Mutex<Vec<usize>>,
        raw: Mutex<Vec<(String, Map<String, Value>)>>,
        flushes: Mutex<usize>,
        shutdowns: Mutex<usize>,
        batch_bytes: Mutex<Vec<Option<Vec<f64>>>>,
    }

    impl MockFlusher {
        fn new(name: &str) -> Arc<Self> {
            Arc::new(Self {
                name: name.to_string(),
                should_fail: Mutex::new(false),
                sends: Mutex::new(0),
                batches: Mutex::new(Vec::new()),
                raw: Mutex::new(Vec::new()),
                flushes: Mutex::new(0),
                shutdowns: Mutex::new(0),
                batch_bytes: Mutex::new(Vec::new()),
            })
        }
    }

    #[async_trait]
    impl BaseFlusher for MockFlusher {
        fn name(&self) -> &str {
            &self.name
        }

        async fn send(
            &self,
            _entry: AgentActivityEntry,
            _logical_bytes: Option<f64>,
        ) -> Result<(), FlushError> {
            *self.sends.lock().unwrap() += 1;
            if *self.should_fail.lock().unwrap() {
                Err(FlushError::new("fail"))
            } else {
                Ok(())
            }
        }

        async fn send_batch(
            &self,
            entries: Vec<AgentActivityEntry>,
            logical_bytes: Option<Vec<f64>>,
        ) -> Result<(), FlushError> {
            self.batches.lock().unwrap().push(entries.len());
            self.batch_bytes.lock().unwrap().push(logical_bytes);
            if *self.should_fail.lock().unwrap() {
                Err(FlushError::new("fail"))
            } else {
                Ok(())
            }
        }

        async fn flush(&self) -> Result<(), FlushError> {
            *self.flushes.lock().unwrap() += 1;
            Ok(())
        }

        async fn shutdown(&self) -> Result<(), FlushError> {
            *self.shutdowns.lock().unwrap() += 1;
            if *self.should_fail.lock().unwrap() {
                Err(FlushError::new("fail"))
            } else {
                Ok(())
            }
        }

        async fn send_raw(
            &self,
            topic: &str,
            payload: &Map<String, Value>,
        ) -> Result<(), FlushError> {
            self.raw
                .lock()
                .unwrap()
                .push((topic.to_string(), payload.clone()));
            if *self.should_fail.lock().unwrap() {
                Err(FlushError::new("fail"))
            } else {
                Ok(())
            }
        }
    }

    #[tokio::test]
    async fn fans_out_batches_and_isolates_failures() {
        let first = MockFlusher::new("f1");
        let second = MockFlusher::new("f2");
        let multi = MultiFlusher::new(vec![first.clone(), second.clone()]);
        let entries = vec![crate::normalization::build_test_entry(&json!({}))];
        multi
            .send_batch(entries.clone(), Some(vec![123.0]))
            .await
            .unwrap();
        assert_eq!(
            first.batch_bytes.lock().unwrap()[0].as_ref().unwrap(),
            &vec![123.0]
        );
        assert!(multi.trace_runtime_snapshot().is_empty());

        multi
            .send_batch(
                vec![
                    crate::normalization::build_test_entry(&json!({})),
                    crate::normalization::build_test_entry(&json!({})),
                ],
                None,
            )
            .await
            .unwrap();
        assert_eq!(first.batches.lock().unwrap()[1], 2);
        assert_eq!(second.batches.lock().unwrap()[1], 2);

        *first.should_fail.lock().unwrap() = true;
        multi.send_batch(entries, None).await.unwrap();
        assert_eq!(second.batches.lock().unwrap().len(), 3);
        *first.should_fail.lock().unwrap() = false;
        *second.should_fail.lock().unwrap() = true;
        multi
            .send_batch(
                vec![crate::normalization::build_test_entry(&json!({}))],
                None,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn send_raw_shutdown_and_flush_reach_every_child() {
        let first = MockFlusher::new("f1");
        let second = MockFlusher::new("f2");
        let multi = MultiFlusher::new(vec![first.clone(), second.clone()]);
        multi
            .send(crate::normalization::build_test_entry(&json!({})), None)
            .await
            .unwrap();
        assert_eq!(*first.sends.lock().unwrap(), 1);
        assert_eq!(*second.sends.lock().unwrap(), 1);

        let mut payload = Map::new();
        payload.insert("data".to_string(), Value::String("payload".to_string()));
        multi.send_raw("topic-x", &payload).await.unwrap();
        assert_eq!(first.raw.lock().unwrap()[0].0, "topic-x");
        assert_eq!(
            first.raw.lock().unwrap()[0].1.get("data").unwrap(),
            "payload"
        );
        assert_eq!(second.raw.lock().unwrap().len(), 1);

        *first.should_fail.lock().unwrap() = true;
        multi.send_raw("topic", &payload).await.unwrap();
        assert_eq!(second.raw.lock().unwrap().len(), 2);

        multi.shutdown().await.unwrap();
        assert_eq!(*second.shutdowns.lock().unwrap(), 1);
        *first.should_fail.lock().unwrap() = false;
        multi.flush().await.unwrap();
        assert_eq!(*first.flushes.lock().unwrap(), 1);
        assert_eq!(*second.flushes.lock().unwrap(), 1);
    }
}
