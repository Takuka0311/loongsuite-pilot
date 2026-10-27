//! Shared start/stop/poll lifecycle from `src/inputs/base/base-input.ts`.
//!
//! Concrete collectors are [`InputBody`] values hosted by [`HostedInput`].
//! Mutable lifecycle state sits behind mutexes so `start` and `stop` can
//! interleave the way the TypeScript promises do.

use super::runtime_metrics::{InputRuntimeAccumulator, InputRuntimeDelta};
use super::workspace_policy::WorkspacePolicy;
use crate::checkpoints::{InputState, InputStatePatch, StateStore};
use crate::types::{AgentActivityEntry, ClientType, CollectionMethod};
use crate::utils::{
    create_logger, resolve_poll_interval_ms, BoundLogger, DEFAULT_POLL_INTERVAL_MS,
};
use async_trait::async_trait;
use serde_json::{json, Map, Value};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

const OWNERSHIP_WARN_CAP: usize = 512;

#[async_trait]
pub trait InputBody: Send + Sync + 'static {
    fn id(&self) -> &str;
    fn agent_type(&self) -> ClientType;
    fn collection_method(&self) -> CollectionMethod;
    async fn collect(&self, rt: &InputRuntime) -> anyhow::Result<Vec<AgentActivityEntry>>;
    async fn on_start(&self, _rt: &InputRuntime) -> anyhow::Result<()> {
        Ok(())
    }
    async fn on_stop(&self, _rt: &InputRuntime) -> anyhow::Result<()> {
        Ok(())
    }
    fn before_stop(&self) {}
    fn agent_version(&self) -> String {
        String::new()
    }
}

struct Gate {
    result: Mutex<Option<Result<(), String>>>,
    notify: Notify,
}

impl Gate {
    fn new() -> Self {
        Self {
            result: Mutex::new(None),
            notify: Notify::new(),
        }
    }

    async fn wait(&self) -> Result<(), String> {
        loop {
            let notified = self.notify.notified();
            if let Some(result) = self
                .result
                .lock()
                .unwrap_or_else(|err| err.into_inner())
                .clone()
            {
                return result;
            }
            notified.await;
        }
    }

    fn finish(&self, result: Result<(), String>) {
        *self.result.lock().unwrap_or_else(|err| err.into_inner()) = Some(result);
        self.notify.notify_waiters();
    }
}

struct CycleGate {
    done: Mutex<bool>,
    notify: Notify,
}

impl CycleGate {
    fn new() -> Self {
        Self {
            done: Mutex::new(false),
            notify: Notify::new(),
        }
    }

    async fn wait(&self) {
        loop {
            let notified = self.notify.notified();
            if *self.done.lock().unwrap_or_else(|err| err.into_inner()) {
                return;
            }
            notified.await;
        }
    }

    fn finish(&self) {
        *self.done.lock().unwrap_or_else(|err| err.into_inner()) = true;
        self.notify.notify_waiters();
    }
}

struct Lifecycle {
    running: bool,
    start: Option<Arc<Gate>>,
    stop: Option<Arc<Gate>>,
    cycle: Option<Arc<CycleGate>>,
}

pub struct InputRuntime {
    pub id: String,
    pub agent_type: ClientType,
    pub collection_method: CollectionMethod,
    pub logger: BoundLogger,
    pub state_store: Arc<StateStore>,
    poll_interval_ms: AtomicU64,
    lifecycle: Mutex<Lifecycle>,
    timer: Mutex<Option<JoinHandle<()>>>,
    workspace_policy: Mutex<WorkspacePolicy>,
    accumulator: Mutex<Option<InputRuntimeAccumulator>>,
    ownership_warned: Mutex<VecDeque<String>>,
    #[cfg(test)]
    test_warns: Mutex<Vec<String>>,
    #[cfg(test)]
    daemon_uid_override: Mutex<Option<u32>>,
}

impl InputRuntime {
    pub fn state(&self) -> InputState {
        self.state_store.get(&self.id)
    }

    pub fn update_state(&self, patch: InputStatePatch) {
        self.state_store.update(&self.id, patch);
    }

    pub fn set_state_fields(
        &self,
        last_file: Option<String>,
        last_offset: Option<f64>,
        extra: Option<Value>,
    ) {
        self.update_state(InputStatePatch {
            last_file,
            last_offset,
            extra,
            ..InputStatePatch::default()
        });
    }

    pub fn poll_interval_ms(&self) -> u64 {
        self.poll_interval_ms.load(Ordering::SeqCst)
    }

    /// Matches `BaseCliForwarder` assigning `pollIntervalMs` after `super()`.
    pub fn force_poll_interval_ms(&self, millis: u64) {
        self.poll_interval_ms.store(millis, Ordering::SeqCst);
    }

    pub fn is_running(&self) -> bool {
        self.lifecycle
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .running
    }

    pub fn timer_active(&self) -> bool {
        self.timer
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .is_some()
    }

    pub fn set_workspace_policy(&self, policy: WorkspacePolicy) {
        *self
            .workspace_policy
            .lock()
            .unwrap_or_else(|err| err.into_inner()) = policy;
    }

    pub fn with_workspace_policy<R>(&self, f: impl FnOnce(&mut WorkspacePolicy) -> R) -> R {
        let mut policy = self
            .workspace_policy
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        f(&mut policy)
    }

    pub fn with_accumulator(&self, f: impl FnOnce(&mut InputRuntimeAccumulator)) {
        if let Some(acc) = self
            .accumulator
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .as_mut()
        {
            f(acc);
        }
    }

    pub(crate) fn warn(&self, message: &str, meta: Option<&Map<String, Value>>) {
        self.logger.warn(message, meta);
        #[cfg(test)]
        self.test_warns
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .push(message.to_owned());
    }

    pub fn info(&self, message: &str, meta: Option<&Map<String, Value>>) {
        self.logger.info(message, meta);
    }

    pub fn error(&self, message: &str, meta: Option<&Map<String, Value>>) {
        self.logger.error(message, meta);
    }

    pub fn debug(&self, message: &str, meta: Option<&Map<String, Value>>) {
        self.logger.debug(message, meta);
    }

    #[cfg(test)]
    pub fn test_warns(&self) -> Vec<String> {
        self.test_warns
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .clone()
    }

    #[cfg(test)]
    pub fn set_daemon_uid_override(&self, uid: Option<u32>) {
        *self
            .daemon_uid_override
            .lock()
            .unwrap_or_else(|err| err.into_inner()) = uid;
    }

    fn current_uid(&self) -> Option<u32> {
        #[cfg(test)]
        if let Some(uid) = *self
            .daemon_uid_override
            .lock()
            .unwrap_or_else(|err| err.into_inner())
        {
            return Some(uid);
        }
        current_process_uid()
    }

    pub fn is_unreadable_error(err: &std::io::Error) -> bool {
        if err.kind() == std::io::ErrorKind::PermissionDenied {
            return true;
        }
        matches!(err.raw_os_error(), Some(1 | 13))
    }

    pub async fn diagnose_unreadable_path(&self, target_path: &str, kind: &str) {
        let warn_key = format!("{kind}:{target_path}");
        {
            let mut warned = self
                .ownership_warned
                .lock()
                .unwrap_or_else(|err| err.into_inner());
            if warned.iter().any(|existing| existing == &warn_key) {
                return;
            }
            if warned.len() >= OWNERSHIP_WARN_CAP {
                warned.pop_front();
            }
            warned.push_back(warn_key);
        }
        let owner_uid = tokio::fs::metadata(target_path)
            .await
            .ok()
            .map(|meta| file_uid(&meta));
        let daemon_uid = self.current_uid();
        let meta = json!({
            "path": target_path,
            "kind": kind,
            "ownerUid": owner_uid,
            "daemonUid": daemon_uid,
        });
        let meta = meta.as_object();
        if daemon_uid.is_none() {
            self.warn(
                &format!(
                    "ownership-mismatch: cannot read {kind} (EACCES); this platform exposes no process uid, so ensure the process writing agent events runs as the same user as this daemon"
                ),
                meta,
            );
            return;
        }
        let daemon_uid = daemon_uid.unwrap_or(0);
        if owner_uid.is_some_and(|owner| owner != daemon_uid) {
            let owner_uid = owner_uid.unwrap_or(0);
            self.warn(
                &format!(
                    "ownership-mismatch: cannot read {kind} (EACCES): owned by uid {owner_uid}, but this daemon runs as uid {daemon_uid}. A process running as a different uid loaded the pilot plugin and writes event files this daemon cannot read. Fix: run every process that loads the plugin as uid {daemon_uid} (drop the privileges of the uid-{owner_uid} process, e.g. via su or runAsUser)"
                ),
                meta,
            );
            return;
        }
        self.warn(
            &format!(
                "ownership-mismatch: cannot read {kind} (EACCES) despite matching uid {daemon_uid}; check the surrounding directory permissions or security modules (SELinux/AppArmor/ACLs)"
            ),
            meta,
        );
    }
}

fn file_uid(meta: &std::fs::Metadata) -> u32 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        meta.uid()
    }
    #[cfg(not(unix))]
    {
        let _ = meta;
        0
    }
}

fn current_process_uid() -> Option<u32> {
    #[cfg(unix)]
    {
        extern "C" {
            fn getuid() -> u32;
        }
        Some(unsafe { getuid() })
    }
    #[cfg(not(unix))]
    {
        None
    }
}

pub type EntriesHandler = Arc<dyn Fn(&[AgentActivityEntry]) + Send + Sync>;
pub type ErrorHandler = Arc<dyn Fn(&str) + Send + Sync>;
pub type DeltaHandler = Arc<dyn Fn(&InputRuntimeDelta) + Send + Sync>;

pub struct HostedInput<T: InputBody> {
    pub runtime: InputRuntime,
    pub body: T,
    entries_handlers: Mutex<Vec<EntriesHandler>>,
    error_handlers: Mutex<Vec<ErrorHandler>>,
    delta_handlers: Mutex<Vec<DeltaHandler>>,
    running_flag: AtomicBool,
}

impl<T: InputBody> HostedInput<T> {
    pub fn new(
        body: T,
        logger_name: &str,
        state_store: Arc<StateStore>,
        poll_interval_ms: Option<u64>,
    ) -> Arc<Self> {
        let id = body.id().to_owned();
        let agent_type = body.agent_type();
        let collection_method = body.collection_method();
        Arc::new(Self {
            runtime: InputRuntime {
                id,
                agent_type,
                collection_method,
                logger: create_logger(logger_name),
                state_store,
                poll_interval_ms: AtomicU64::new(resolve_poll_interval_ms(
                    poll_interval_ms,
                    DEFAULT_POLL_INTERVAL_MS,
                )),
                lifecycle: Mutex::new(Lifecycle {
                    running: false,
                    start: None,
                    stop: None,
                    cycle: None,
                }),
                timer: Mutex::new(None),
                workspace_policy: Mutex::new(WorkspacePolicy::default()),
                accumulator: Mutex::new(None),
                ownership_warned: Mutex::new(VecDeque::new()),
                #[cfg(test)]
                test_warns: Mutex::new(Vec::new()),
                #[cfg(test)]
                daemon_uid_override: Mutex::new(None),
            },
            body,
            entries_handlers: Mutex::new(Vec::new()),
            error_handlers: Mutex::new(Vec::new()),
            delta_handlers: Mutex::new(Vec::new()),
            running_flag: AtomicBool::new(false),
        })
    }

    pub fn id(&self) -> &str {
        self.body.id()
    }

    pub fn agent_type(&self) -> ClientType {
        self.body.agent_type()
    }

    pub fn collection_method(&self) -> CollectionMethod {
        self.body.collection_method()
    }

    pub fn running(&self) -> bool {
        self.running_flag.load(Ordering::SeqCst)
    }

    pub fn agent_version(&self) -> String {
        self.body.agent_version()
    }

    pub fn on_entries(&self, handler: impl Fn(&[AgentActivityEntry]) + Send + Sync + 'static) {
        self.entries_handlers
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .push(Arc::new(handler));
    }

    pub fn on_collect_error(&self, handler: impl Fn(&str) + Send + Sync + 'static) {
        self.error_handlers
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .push(Arc::new(handler));
    }

    pub fn on_runtime_delta(&self, handler: impl Fn(&InputRuntimeDelta) + Send + Sync + 'static) {
        self.delta_handlers
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .push(Arc::new(handler));
    }

    pub async fn start(self: &Arc<Self>) -> Result<(), String> {
        loop {
            enum Action {
                WaitStop(Arc<Gate>),
                WaitStart(Arc<Gate>),
                Already,
                Lead(Arc<Gate>),
            }
            let action = {
                let mut life = self
                    .runtime
                    .lifecycle
                    .lock()
                    .unwrap_or_else(|err| err.into_inner());
                if let Some(stop) = &life.stop {
                    Action::WaitStop(Arc::clone(stop))
                } else if let Some(start) = &life.start {
                    Action::WaitStart(Arc::clone(start))
                } else if life.running {
                    Action::Already
                } else {
                    life.running = true;
                    self.running_flag.store(true, Ordering::SeqCst);
                    let gate = Arc::new(Gate::new());
                    life.start = Some(Arc::clone(&gate));
                    Action::Lead(gate)
                }
            };
            match action {
                Action::WaitStop(gate) => {
                    gate.wait().await?;
                }
                Action::WaitStart(gate) => return gate.wait().await,
                Action::Already => return Ok(()),
                Action::Lead(gate) => {
                    let result = self.start_once().await;
                    {
                        let mut life = self
                            .runtime
                            .lifecycle
                            .lock()
                            .unwrap_or_else(|err| err.into_inner());
                        life.start = None;
                    }
                    gate.finish(result.clone());
                    return result;
                }
            }
        }
    }

    async fn start_once(self: &Arc<Self>) -> Result<(), String> {
        self.runtime.logger.info("starting", None);
        if let Err(err) = self.body.on_start(&self.runtime).await {
            self.fail_start();
            let stop_in_progress = self
                .runtime
                .lifecycle
                .lock()
                .unwrap_or_else(|err| err.into_inner())
                .stop
                .is_some();
            if !stop_in_progress {
                let _ = self.body.on_stop(&self.runtime).await;
            }
            return Err(err.to_string());
        }
        if !self.runtime.is_running() {
            return Ok(());
        }
        self.run_cycle().await;
        if !self.runtime.is_running() {
            return Ok(());
        }
        self.arm_timer();
        // Let the interval register before returning so paused-time tests can advance it.
        tokio::task::yield_now().await;
        Ok(())
    }

    fn fail_start(&self) {
        let mut life = self
            .runtime
            .lifecycle
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        life.running = false;
        self.running_flag.store(false, Ordering::SeqCst);
    }

    pub async fn stop(self: &Arc<Self>) -> Result<(), String> {
        self.body.before_stop();
        enum Action {
            Wait(Arc<Gate>),
            Idle,
            Lead(Arc<Gate>),
        }
        let action = {
            let mut life = self
                .runtime
                .lifecycle
                .lock()
                .unwrap_or_else(|err| err.into_inner());
            if let Some(stop) = &life.stop {
                Action::Wait(Arc::clone(stop))
            } else if !life.running && life.start.is_none() {
                Action::Idle
            } else {
                life.running = false;
                self.running_flag.store(false, Ordering::SeqCst);
                if let Some(handle) = self
                    .runtime
                    .timer
                    .lock()
                    .unwrap_or_else(|err| err.into_inner())
                    .take()
                {
                    handle.abort();
                }
                let gate = Arc::new(Gate::new());
                life.stop = Some(Arc::clone(&gate));
                Action::Lead(gate)
            }
        };
        match action {
            Action::Wait(gate) => gate.wait().await,
            Action::Idle => Ok(()),
            Action::Lead(gate) => {
                let result = self.stop_once().await;
                {
                    let mut life = self
                        .runtime
                        .lifecycle
                        .lock()
                        .unwrap_or_else(|err| err.into_inner());
                    life.stop = None;
                }
                gate.finish(result.clone());
                result
            }
        }
    }

    async fn stop_once(self: &Arc<Self>) -> Result<(), String> {
        let start = self
            .runtime
            .lifecycle
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .start
            .clone();
        if let Some(start) = start {
            let _ = start.wait().await;
        }
        let cycle = self
            .runtime
            .lifecycle
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .cycle
            .clone();
        if let Some(cycle) = cycle {
            cycle.wait().await;
        }
        let stopped = self
            .body
            .on_stop(&self.runtime)
            .await
            .map_err(|err| err.to_string());
        self.runtime.logger.info("stopped", None);
        stopped
    }

    pub fn request_collection(self: &Arc<Self>) {
        if !self.runtime.is_running() {
            return;
        }
        let this = Arc::clone(self);
        tokio::spawn(async move {
            this.run_cycle().await;
        });
    }

    pub async fn run_cycle(self: &Arc<Self>) {
        enum Action {
            Wait(Arc<CycleGate>),
            Lead(Arc<CycleGate>),
        }
        let action = {
            let mut life = self
                .runtime
                .lifecycle
                .lock()
                .unwrap_or_else(|err| err.into_inner());
            if let Some(cycle) = &life.cycle {
                Action::Wait(Arc::clone(cycle))
            } else {
                let gate = Arc::new(CycleGate::new());
                life.cycle = Some(Arc::clone(&gate));
                Action::Lead(gate)
            }
        };
        match action {
            Action::Wait(gate) => gate.wait().await,
            Action::Lead(gate) => {
                self.run_cycle_once().await;
                {
                    let mut life = self
                        .runtime
                        .lifecycle
                        .lock()
                        .unwrap_or_else(|err| err.into_inner());
                    life.cycle = None;
                }
                gate.finish();
            }
        }
    }

    async fn run_cycle_once(&self) {
        let started = InputRuntimeAccumulator::now();
        *self
            .runtime
            .accumulator
            .lock()
            .unwrap_or_else(|err| err.into_inner()) = Some(InputRuntimeAccumulator::new());
        let cycle_result = async {
            let entries = self.body.collect(&self.runtime).await?;
            let filtered = self.runtime.with_workspace_policy(|policy| {
                policy.filter_entries(&entries, self.runtime.agent_type)
            });
            if !filtered.is_empty() {
                let handlers = self
                    .entries_handlers
                    .lock()
                    .unwrap_or_else(|err| err.into_inner())
                    .clone();
                for handler in handlers {
                    handler(&filtered);
                }
                self.runtime.logger.debug(
                    "cycle produced entries",
                    json!({ "count": filtered.len() }).as_object(),
                );
            }
            self.runtime.state_store.save().await?;
            Ok::<(), anyhow::Error>(())
        }
        .await;
        if let Err(err) = cycle_result {
            let message = err.to_string();
            self.runtime.logger.error(
                "collection cycle failed",
                json!({ "error": message }).as_object(),
            );
            let handlers = self
                .error_handlers
                .lock()
                .unwrap_or_else(|err| err.into_inner())
                .clone();
            for handler in handlers {
                handler(&message);
            }
        }
        let duration = InputRuntimeAccumulator::now() - started;
        let delta = self
            .runtime
            .accumulator
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .take()
            .unwrap_or_default()
            .finish(duration);
        let handlers = self
            .delta_handlers
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .clone();
        for handler in handlers {
            handler(&delta);
        }
    }

    fn arm_timer(self: &Arc<Self>) {
        let this = Arc::clone(self);
        let period = Duration::from_millis(self.runtime.poll_interval_ms().max(1));
        let handle = tokio::spawn(async move {
            let start = tokio::time::Instant::now() + period;
            let mut ticker = tokio::time::interval_at(start, period);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                if !this.runtime.is_running() {
                    break;
                }
                let cycle = Arc::clone(&this);
                tokio::spawn(async move {
                    cycle.run_cycle().await;
                });
            }
        });
        *self
            .runtime
            .timer
            .lock()
            .unwrap_or_else(|err| err.into_inner()) = Some(handle);
    }
}
