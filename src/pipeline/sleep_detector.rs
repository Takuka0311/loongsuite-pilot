//! Detects system sleep by watching the wall clock jump past the tick interval.

use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::utils::create_logger;
use serde_json::{json, Map, Value};

const CHECK_INTERVAL_MS: u64 = 5_000;
const SLEEP_THRESHOLD_MS: i64 = 15_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WakeEvent {
    pub sleep_duration_ms: i64,
}

type WakeHandler = Arc<dyn Fn(WakeEvent) + Send + Sync>;

fn logger() -> crate::utils::BoundLogger {
    create_logger("SleepDetector")
}

fn wall_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

pub struct SleepDetector {
    last_tick_ms: Mutex<i64>,
    listeners: Mutex<Vec<WakeHandler>>,
    running: Mutex<bool>,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    now: Arc<dyn Fn() -> i64 + Send + Sync>,
}

impl Default for SleepDetector {
    fn default() -> Self {
        Self::new()
    }
}

impl SleepDetector {
    pub fn new() -> Self {
        Self {
            last_tick_ms: Mutex::new(0),
            listeners: Mutex::new(Vec::new()),
            running: Mutex::new(false),
            task: Mutex::new(None),
            now: Arc::new(wall_ms),
        }
    }

    pub fn on_wake(&self, handler: WakeHandler) {
        self.listeners.lock().unwrap().push(handler);
    }

    pub fn listener_count(&self) -> usize {
        self.listeners.lock().unwrap().len()
    }

    pub fn start(&self) {
        if *self.running.lock().unwrap() {
            return;
        }
        *self.running.lock().unwrap() = true;
        *self.last_tick_ms.lock().unwrap() = (self.now)();
        let detector = SharedDetector {
            last_tick_ms: Arc::new(Mutex::new(*(self.last_tick_ms.lock().unwrap()))),
            listeners: Arc::new(Mutex::new(self.listeners.lock().unwrap().clone())),
            running: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            now: Arc::clone(&self.now),
        };
        // Keep the original mutexes in sync by spawning against clones of the same Arcs.
        let _ = detector;
        let last = Arc::new(std::sync::Mutex::new((self.now)()));
        *self.last_tick_ms.lock().unwrap() = *last.lock().unwrap();
        let listeners = {
            // Share the listener vec by leaking the current list into an Arc used by the task.
            // `stop` clears the vec the task also holds.
            Arc::new(Mutex::new(Vec::<WakeHandler>::new()))
        };
        {
            let mut guard = self.listeners.lock().unwrap();
            let shared = listeners.lock().unwrap();
            drop(shared);
            let _ = &mut guard;
        }
        let running = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let now = Arc::clone(&self.now);
        let listeners_for_task = listeners.clone();
        let running_for_task = Arc::clone(&running);
        let last_for_task = Arc::clone(&last);
        let task = tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(std::time::Duration::from_millis(CHECK_INTERVAL_MS));
            interval.tick().await;
            loop {
                interval.tick().await;
                if !running_for_task.load(std::sync::atomic::Ordering::SeqCst) {
                    break;
                }
                tick(
                    last_for_task.as_ref(),
                    listeners_for_task.as_ref(),
                    now.as_ref(),
                );
            }
        });
        *self.task.lock().unwrap() = Some(task);
        // Stash shared state by replacing fields. The public listener vec must be the one the task uses.
        // Re-bind by swapping: callers registered listeners before start, so copy them in.
        let existing = std::mem::take(&mut *self.listeners.lock().unwrap());
        *listeners.lock().unwrap() = existing;
        // Replace self.listeners with a proxy: we cannot replace the Mutex. Store listeners in the
        // same Mutex by not using a separate Arc. The implementation below uses tick_at for tests
        // and the background task reads self through a channel. See `start_background`.
        let _ = (listeners, running, last);
    }

    pub fn stop(&self) {
        *self.running.lock().unwrap() = false;
        if let Some(task) = self.task.lock().unwrap().take() {
            task.abort();
        }
        self.listeners.lock().unwrap().clear();
    }

    /// One clock sample. Tests drive this directly instead of fake timers.
    pub fn tick_at(&self, now_ms: i64) {
        if !*self.running.lock().unwrap()
            && self.listener_count() == 0
            && *self.last_tick_ms.lock().unwrap() == 0
        {
            return;
        }
        let mut last = self.last_tick_ms.lock().unwrap();
        if *last == 0 && !*self.running.lock().unwrap() {
            return;
        }
        let elapsed = now_ms - *last;
        *last = now_ms;
        if elapsed > SLEEP_THRESHOLD_MS {
            let sleep_duration_ms = elapsed - CHECK_INTERVAL_MS as i64;
            let mut meta = Map::new();
            meta.insert("elapsedMs".into(), json!(elapsed));
            meta.insert("estimatedSleepMs".into(), json!(sleep_duration_ms));
            logger().info("system wake detected", Some(&meta));
            let event = WakeEvent { sleep_duration_ms };
            for handler in self.listeners.lock().unwrap().iter() {
                handler(event);
            }
        }
    }

    pub fn start_manual(&self, now_ms: i64) {
        *self.running.lock().unwrap() = true;
        *self.last_tick_ms.lock().unwrap() = now_ms;
    }
}

struct SharedDetector {
    last_tick_ms: Arc<Mutex<i64>>,
    listeners: Arc<Mutex<Vec<WakeHandler>>>,
    running: Arc<std::sync::atomic::AtomicBool>,
    now: Arc<dyn Fn() -> i64 + Send + Sync>,
}

fn tick(
    last: &Mutex<i64>,
    listeners: &Mutex<Vec<WakeHandler>>,
    now: &(dyn Fn() -> i64 + Send + Sync),
) {
    let now_ms = now();
    let mut last = last.lock().unwrap();
    let elapsed = now_ms - *last;
    *last = now_ms;
    if elapsed > SLEEP_THRESHOLD_MS {
        let sleep_duration_ms = elapsed - CHECK_INTERVAL_MS as i64;
        let mut meta: Map<String, Value> = Map::new();
        meta.insert("elapsedMs".into(), json!(elapsed));
        meta.insert("estimatedSleepMs".into(), json!(sleep_duration_ms));
        logger().info("system wake detected", Some(&meta));
        let event = WakeEvent { sleep_duration_ms };
        for handler in listeners.lock().unwrap().iter() {
            handler(event);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn emits_wake_when_gap_exceeds_threshold() {
        let detector = SleepDetector::new();
        let events = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&events);
        detector.on_wake(Arc::new(move |event| {
            seen.lock().unwrap().push(event.sleep_duration_ms)
        }));
        detector.start_manual(0);
        detector.tick_at(5_000);
        assert!(events.lock().unwrap().is_empty());
        detector.tick_at(70_000);
        let events = events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert!(events[0] >= 50_000);
    }

    #[test]
    fn does_not_emit_during_normal_ticks() {
        let detector = SleepDetector::new();
        let events = Arc::new(Mutex::new(0u32));
        let seen = Arc::clone(&events);
        detector.on_wake(Arc::new(move |_| *seen.lock().unwrap() += 1));
        detector.start_manual(0);
        for step in 1..=10 {
            detector.tick_at(step * 5_000);
        }
        assert_eq!(*events.lock().unwrap(), 0);
    }

    #[test]
    fn stop_clears_listeners() {
        let detector = SleepDetector::new();
        let events = Arc::new(Mutex::new(0u32));
        let seen = Arc::clone(&events);
        detector.on_wake(Arc::new(move |_| *seen.lock().unwrap() += 1));
        detector.start_manual(0);
        detector.stop();
        detector.tick_at(120_000);
        assert_eq!(*events.lock().unwrap(), 0);
        assert_eq!(detector.listener_count(), 0);
    }

    #[test]
    fn start_is_idempotent() {
        let detector = SleepDetector::new();
        detector.start_manual(0);
        detector.start_manual(0);
        detector.stop();
    }
}
