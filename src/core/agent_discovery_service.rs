//! Agent discovery. Port of `agent-discovery-service.ts`.
//!
//! Each entry moves Idle → Starting → Running → Stopping → Idle. Watch bursts
//! coalesce onto one in-flight refresh. `notify` is not a crate dependency, so
//! an existing watch path is observed by mtime polling (1s) and a missing path
//! falls back to the entry interval. `LOONGSUITE_PILOT_FORCE_POLLING=true`
//! skips the mtime watcher, matching the TypeScript force-polling switch.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Mutex as AsyncMutex;
use tokio::task::JoinHandle;

use crate::types::{AgentDetection, AgentStopReason, EntryState};

use super::{fields, logger};

const DEFAULT_POLL_MS: u64 = 300_000;
const WATCH_POLL_MS: u64 = 1_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentDiscoveryEvent {
    Started(String),
    Stopped(String, AgentStopReason),
}

struct EntryRuntime {
    entry: Arc<dyn AgentDetection>,
    state: EntryState,
    consecutive_unavailable: u32,
    refresh_pending: bool,
    poll: Option<JoinHandle<()>>,
    watch: Option<JoinHandle<()>>,
}

pub struct AgentDiscoveryService {
    runtimes: Mutex<HashMap<String, EntryRuntime>>,
    gates: HashMap<String, AsyncMutex<()>>,
    stopping: AtomicBool,
    global_poll: Mutex<Option<JoinHandle<()>>>,
    listeners: Mutex<Vec<Arc<dyn Fn(AgentDiscoveryEvent) + Send + Sync>>>,
}

impl AgentDiscoveryService {
    pub fn new(entries: Vec<Arc<dyn AgentDetection>>) -> Self {
        let mut runtimes = HashMap::new();
        let mut gates = HashMap::new();
        for entry in entries {
            let id = entry.id().to_string();
            gates.insert(id.clone(), AsyncMutex::new(()));
            runtimes.insert(
                id,
                EntryRuntime {
                    entry,
                    state: EntryState::Idle,
                    consecutive_unavailable: 0,
                    refresh_pending: false,
                    poll: None,
                    watch: None,
                },
            );
        }
        Self {
            runtimes: Mutex::new(runtimes),
            gates,
            stopping: AtomicBool::new(false),
            global_poll: Mutex::new(None),
            listeners: Mutex::new(Vec::new()),
        }
    }

    pub fn on_event(&self, listener: impl Fn(AgentDiscoveryEvent) + Send + Sync + 'static) {
        self.listeners
            .lock()
            .expect("listeners")
            .push(Arc::new(listener));
    }

    pub async fn start(self: &Arc<Self>) {
        self.stopping.store(false, Ordering::SeqCst);
        let ids: Vec<String> = self
            .runtimes
            .lock()
            .expect("runtimes")
            .keys()
            .cloned()
            .collect();
        for id in &ids {
            self.setup_watcher(id);
        }
        self.refresh("startup").await;
        if self.stopping.load(Ordering::SeqCst) {
            return;
        }
        let interval = std::env::var("LOONGSUITE_PILOT_DISCOVERY_INTERVAL_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_POLL_MS);
        let this = Arc::clone(self);
        *self.global_poll.lock().expect("global poll") = Some(tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_millis(interval));
            ticker.tick().await;
            loop {
                ticker.tick().await;
                if this.stopping.load(Ordering::SeqCst) {
                    break;
                }
                this.refresh("poll").await;
            }
        }));
    }

    pub async fn stop(&self) {
        self.stopping.store(true, Ordering::SeqCst);
        if let Some(task) = self.global_poll.lock().expect("global poll").take() {
            task.abort();
        }
        let ids: Vec<String> = self
            .runtimes
            .lock()
            .expect("runtimes")
            .keys()
            .cloned()
            .collect();
        for id in ids {
            {
                let mut runtimes = self.runtimes.lock().expect("runtimes");
                if let Some(rt) = runtimes.get_mut(&id) {
                    if let Some(task) = rt.watch.take() {
                        task.abort();
                    }
                    if let Some(task) = rt.poll.take() {
                        task.abort();
                    }
                }
            }
            if let Some(gate) = self.gates.get(&id) {
                let _guard = gate.lock().await;
            }
            let should_stop = self
                .runtimes
                .lock()
                .expect("runtimes")
                .get(&id)
                .map(|rt| matches!(rt.state, EntryState::Running | EntryState::Starting))
                .unwrap_or(false);
            if should_stop {
                self.stop_entry(&id, AgentStopReason::Shutdown).await;
            }
        }
    }

    pub async fn refresh(&self, trigger: &str) {
        logger("AgentDiscoveryService").debug(
            "refresh triggered",
            Some(&fields(serde_json::json!({ "trigger": trigger }))),
        );
        let ids: Vec<String> = self
            .runtimes
            .lock()
            .expect("runtimes")
            .keys()
            .cloned()
            .collect();
        for id in ids {
            self.process_entry(&id).await;
        }
    }

    pub fn get_states(&self) -> HashMap<String, EntryState> {
        self.runtimes
            .lock()
            .expect("runtimes")
            .iter()
            .map(|(id, rt)| (id.clone(), rt.state))
            .collect()
    }

    async fn process_entry(&self, id: &str) {
        if self.stopping.load(Ordering::SeqCst) {
            return;
        }
        {
            let mut runtimes = self.runtimes.lock().expect("runtimes");
            let Some(rt) = runtimes.get_mut(id) else {
                return;
            };
            rt.refresh_pending = true;
        }
        let Some(gate) = self.gates.get(id) else {
            return;
        };
        let _guard = gate.lock().await;
        loop {
            let pending = {
                let mut runtimes = self.runtimes.lock().expect("runtimes");
                let Some(rt) = runtimes.get_mut(id) else {
                    return;
                };
                let pending = rt.refresh_pending;
                rt.refresh_pending = false;
                pending
            };
            if !pending || self.stopping.load(Ordering::SeqCst) {
                break;
            }
            self.process_entry_once(id).await;
        }
    }

    async fn process_entry_once(&self, id: &str) {
        let entry = {
            let runtimes = self.runtimes.lock().expect("runtimes");
            let Some(rt) = runtimes.get(id) else { return };
            Arc::clone(&rt.entry)
        };
        let state = self
            .runtimes
            .lock()
            .expect("runtimes")
            .get(id)
            .map(|rt| rt.state)
            .unwrap_or(EntryState::Idle);
        let outcome = async {
            let enabled = entry.enabled();
            let available = if enabled {
                entry.is_available().await
            } else {
                false
            };
            if self.stopping.load(Ordering::SeqCst) {
                return;
            }
            let should_run = enabled && available;
            if !should_run && state == EntryState::Idle {
                logger("AgentDiscoveryService").debug(
                    "agent skipped",
                    Some(&fields(serde_json::json!({ "id": entry.id(), "enabled": enabled, "available": available }))),
                );
            }
            if should_run && state != EntryState::Running {
                if let Some(rt) = self.runtimes.lock().expect("runtimes").get_mut(id) {
                    rt.consecutive_unavailable = 0;
                    rt.state = EntryState::Starting;
                }
                logger("AgentDiscoveryService").info(
                    "starting agent",
                    Some(&fields(serde_json::json!({ "id": entry.id() }))),
                );
                if let Err(err) = entry.start().await {
                    self.fail_entry(id, &err.to_string());
                    return;
                }
                if self.stopping.load(Ordering::SeqCst) {
                    return;
                }
                if let Some(rt) = self.runtimes.lock().expect("runtimes").get_mut(id) {
                    rt.state = EntryState::Running;
                }
                self.emit(AgentDiscoveryEvent::Started(entry.id().to_string()));
            } else if !should_run && matches!(state, EntryState::Running | EntryState::Starting) {
                if !enabled {
                    if let Some(rt) = self.runtimes.lock().expect("runtimes").get_mut(id) {
                        rt.consecutive_unavailable = 0;
                    }
                    self.stop_entry(id, AgentStopReason::Disabled).await;
                } else {
                    let threshold = entry.unavailable_threshold();
                    let count = {
                        let mut runtimes = self.runtimes.lock().expect("runtimes");
                        let Some(rt) = runtimes.get_mut(id) else {
                            return;
                        };
                        rt.consecutive_unavailable += 1;
                        rt.consecutive_unavailable
                    };
                    if count >= threshold {
                        if let Some(rt) = self.runtimes.lock().expect("runtimes").get_mut(id) {
                            rt.consecutive_unavailable = 0;
                        }
                        self.stop_entry(id, AgentStopReason::Unavailable).await;
                    } else {
                        logger("AgentDiscoveryService").debug(
                            "agent unavailable, debouncing",
                            Some(&fields(serde_json::json!({
                                "id": entry.id(),
                                "consecutiveUnavailable": count,
                                "threshold": threshold,
                            }))),
                        );
                    }
                }
            } else if should_run && state == EntryState::Running && entry.run_on_active() {
                if let Some(rt) = self.runtimes.lock().expect("runtimes").get_mut(id) {
                    rt.consecutive_unavailable = 0;
                }
                let _ = entry.start().await;
            } else if should_run && state == EntryState::Running {
                if let Some(rt) = self.runtimes.lock().expect("runtimes").get_mut(id) {
                    rt.consecutive_unavailable = 0;
                }
            }
        };
        outcome.await;
    }

    fn fail_entry(&self, id: &str, error: &str) {
        logger("AgentDiscoveryService").error(
            "processEntry failed",
            Some(&fields(serde_json::json!({ "id": id, "error": error }))),
        );
        let was_running = self
            .runtimes
            .lock()
            .expect("runtimes")
            .get(id)
            .map(|rt| rt.state == EntryState::Running)
            .unwrap_or(false);
        if let Some(rt) = self.runtimes.lock().expect("runtimes").get_mut(id) {
            rt.state = EntryState::Idle;
        }
        if was_running {
            self.emit(AgentDiscoveryEvent::Stopped(
                id.to_string(),
                AgentStopReason::Unexpected,
            ));
        }
    }

    async fn stop_entry(&self, id: &str, reason: AgentStopReason) {
        let entry = {
            let mut runtimes = self.runtimes.lock().expect("runtimes");
            let Some(rt) = runtimes.get_mut(id) else {
                return;
            };
            rt.state = EntryState::Stopping;
            Arc::clone(&rt.entry)
        };
        if let Err(err) = entry.stop().await {
            logger("AgentDiscoveryService").warn(
                "entry stop failed",
                Some(&fields(
                    serde_json::json!({ "id": id, "error": err.to_string() }),
                )),
            );
        }
        if let Some(rt) = self.runtimes.lock().expect("runtimes").get_mut(id) {
            rt.state = EntryState::Idle;
        }
        self.emit(AgentDiscoveryEvent::Stopped(id.to_string(), reason));
    }

    fn emit(&self, event: AgentDiscoveryEvent) {
        for listener in self.listeners.lock().expect("listeners").iter() {
            listener(event.clone());
        }
    }

    fn setup_watcher(self: &Arc<Self>, id: &str) {
        let force = std::env::var("LOONGSUITE_PILOT_FORCE_POLLING")
            .ok()
            .as_deref()
            == Some("true");
        if force {
            self.setup_polling(id);
            return;
        }
        let paths = self
            .runtimes
            .lock()
            .expect("runtimes")
            .get(id)
            .map(|rt| rt.entry.watch_paths().to_vec())
            .unwrap_or_default();
        if let Some(path) = paths
            .into_iter()
            .find(|path| std::path::Path::new(path).exists())
        {
            let this = Arc::clone(self);
            let id = id.to_string();
            let watched = id.clone();
            let task = tokio::spawn(async move {
                let mut previous = signature(&path);
                let mut ticker = tokio::time::interval(Duration::from_millis(WATCH_POLL_MS));
                ticker.tick().await;
                loop {
                    ticker.tick().await;
                    if this.stopping.load(Ordering::SeqCst) {
                        break;
                    }
                    let next = signature(&path);
                    if next != previous {
                        previous = next;
                        this.process_entry(&watched).await;
                    }
                }
            });
            if let Some(rt) = self.runtimes.lock().expect("runtimes").get_mut(&id) {
                rt.watch = Some(task);
            }
            return;
        }
        self.setup_polling(id);
    }

    fn setup_polling(self: &Arc<Self>, id: &str) {
        if self.stopping.load(Ordering::SeqCst) {
            return;
        }
        let already = self
            .runtimes
            .lock()
            .expect("runtimes")
            .get(id)
            .and_then(|rt| rt.poll.as_ref())
            .is_some();
        if already {
            return;
        }
        let interval = self
            .runtimes
            .lock()
            .expect("runtimes")
            .get(id)
            .map(|rt| {
                let value = rt.entry.poll_interval_ms();
                if value == 0 {
                    DEFAULT_POLL_MS
                } else {
                    value
                }
            })
            .unwrap_or(DEFAULT_POLL_MS);
        let this = Arc::clone(self);
        let id_owned = id.to_string();
        let task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_millis(interval));
            ticker.tick().await;
            loop {
                ticker.tick().await;
                if this.stopping.load(Ordering::SeqCst) {
                    break;
                }
                this.process_entry(&id_owned).await;
            }
        });
        if let Some(rt) = self.runtimes.lock().expect("runtimes").get_mut(id) {
            rt.poll = Some(task);
        }
    }
}

fn signature(path: &str) -> String {
    match std::fs::metadata(path) {
        Ok(meta) => format!(
            "{}:{}",
            meta.len(),
            meta.modified()
                .ok()
                .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_millis())
                .unwrap_or(0)
        ),
        Err(_) => "missing".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::atomic::AtomicU32;

    struct Fake {
        id: String,
        enabled: AtomicBool,
        available: AtomicBool,
        starts: AtomicU32,
        stops: AtomicU32,
        fail_start: AtomicBool,
        threshold: u32,
        watch: Vec<String>,
    }

    #[async_trait]
    impl AgentDetection for Fake {
        fn id(&self) -> &str {
            &self.id
        }
        fn type_name(&self) -> &str {
            "test"
        }
        async fn is_available(&self) -> bool {
            self.available.load(Ordering::SeqCst)
        }
        fn watch_paths(&self) -> &[String] {
            &self.watch
        }
        fn enabled(&self) -> bool {
            self.enabled.load(Ordering::SeqCst)
        }
        async fn start(&self) -> anyhow::Result<()> {
            self.starts.fetch_add(1, Ordering::SeqCst);
            if self.fail_start.load(Ordering::SeqCst) {
                anyhow::bail!("start failed");
            }
            Ok(())
        }
        async fn stop(&self) -> anyhow::Result<()> {
            self.stops.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        fn poll_interval_ms(&self) -> u64 {
            60_000
        }
        fn unavailable_threshold(&self) -> u32 {
            self.threshold
        }
    }

    fn fake(id: &str) -> Arc<Fake> {
        Arc::new(Fake {
            id: id.into(),
            enabled: AtomicBool::new(true),
            available: AtomicBool::new(true),
            starts: AtomicU32::new(0),
            stops: AtomicU32::new(0),
            fail_start: AtomicBool::new(false),
            threshold: 1,
            watch: Vec::new(),
        })
    }

    #[tokio::test]
    async fn starts_when_enabled_and_available_and_stops_on_disable() {
        let agent = fake("claude");
        let service = Arc::new(AgentDiscoveryService::new(vec![agent.clone()]));
        let events = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&events);
        service.on_event(move |event| seen.lock().unwrap().push(event));
        service.refresh("manual").await;
        assert_eq!(
            service.get_states().get("claude"),
            Some(&EntryState::Running)
        );
        assert_eq!(agent.starts.load(Ordering::SeqCst), 1);
        agent.enabled.store(false, Ordering::SeqCst);
        service.refresh("manual").await;
        assert_eq!(service.get_states().get("claude"), Some(&EntryState::Idle));
        assert_eq!(agent.stops.load(Ordering::SeqCst), 1);
        assert!(events.lock().unwrap().iter().any(|event| matches!(
            event,
            AgentDiscoveryEvent::Stopped(_, AgentStopReason::Disabled)
        )));
    }

    #[tokio::test]
    async fn does_not_start_when_unavailable_and_debounces_threshold() {
        let agent = Arc::new(Fake {
            id: "cursor".into(),
            enabled: AtomicBool::new(true),
            available: AtomicBool::new(false),
            starts: AtomicU32::new(0),
            stops: AtomicU32::new(0),
            fail_start: AtomicBool::new(false),
            threshold: 3,
            watch: Vec::new(),
        });
        let service = Arc::new(AgentDiscoveryService::new(vec![agent.clone()]));
        service.refresh("manual").await;
        assert_eq!(agent.starts.load(Ordering::SeqCst), 0);
        agent.available.store(true, Ordering::SeqCst);
        service.refresh("manual").await;
        agent.available.store(false, Ordering::SeqCst);
        service.refresh("manual").await;
        assert_eq!(
            service.get_states().get("cursor"),
            Some(&EntryState::Running)
        );
        service.refresh("manual").await;
        assert_eq!(
            service.get_states().get("cursor"),
            Some(&EntryState::Running)
        );
        service.refresh("manual").await;
        assert_eq!(service.get_states().get("cursor"), Some(&EntryState::Idle));
    }

    #[tokio::test]
    async fn unexpected_stop_when_start_fails_after_running_is_not_emitted_for_idle() {
        let agent = Arc::new(Fake {
            id: "boom".into(),
            enabled: AtomicBool::new(true),
            available: AtomicBool::new(true),
            starts: AtomicU32::new(0),
            stops: AtomicU32::new(0),
            fail_start: AtomicBool::new(true),
            threshold: 1,
            watch: Vec::new(),
        });
        let service = Arc::new(AgentDiscoveryService::new(vec![agent]));
        let events = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&events);
        service.on_event(move |event| seen.lock().unwrap().push(event));
        service.refresh("manual").await;
        assert_eq!(service.get_states().get("boom"), Some(&EntryState::Idle));
        assert!(events.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn shutdown_emits_shutdown_and_stops_running_entries() {
        let agent = fake("qoder");
        let service = Arc::new(AgentDiscoveryService::new(vec![agent.clone()]));
        service.refresh("manual").await;
        service.stop().await;
        assert_eq!(agent.stops.load(Ordering::SeqCst), 1);
        assert_eq!(service.get_states().get("qoder"), Some(&EntryState::Idle));
    }
}
