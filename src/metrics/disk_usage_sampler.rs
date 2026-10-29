//! Bounded metadata walk of mutable Pilot data directories.
//!
//! Only `logs`, `state`, `cache`, `configs`, `agents.d.local`, `acp-correlate`,
//! `sls-failed-logs`, `sls-failed-logs.delete-pending`, and `local-workers` are
//! recursed. Local-worker `bundle`, `.bundle.staging-*`, and `.bundle.backup-*`
//! trees are skipped. Symlinks are not followed. Incomplete scans keep the last
//! complete byte totals.

use crate::utils::create_logger;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub const DISK_USAGE_STALE_MS: i64 = 20 * 60 * 1000;

const SCANNED_DATA_DIRECTORIES: &[&str] = &[
    "logs",
    "state",
    "cache",
    "configs",
    "agents.d.local",
    "acp-correlate",
    "sls-failed-logs",
    "sls-failed-logs.delete-pending",
    "local-workers",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DiskUsageStatus {
    Pending,
    Ok,
    Partial,
    Timeout,
    Error,
    Stale,
}

impl DiskUsageStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Ok => "ok",
            Self::Partial => "partial",
            Self::Timeout => "timeout",
            Self::Error => "error",
            Self::Stale => "stale",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DiskUsageSnapshot {
    pub status: DiskUsageStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sampled_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scan_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logs_bytes: Option<u64>,
}

impl Default for DiskUsageSnapshot {
    fn default() -> Self {
        Self {
            status: DiskUsageStatus::Pending,
            sampled_at: None,
            scan_ms: None,
            data_bytes: None,
            logs_bytes: None,
        }
    }
}

pub struct DiskUsageSamplerOptions {
    pub data_dir: String,
    pub initial_delay_ms: u64,
    pub interval_ms: u64,
    pub batch_size: u64,
    pub batch_pause_ms: u64,
    pub max_depth: usize,
    pub max_entries: u64,
    pub budget_ms: u128,
}

impl DiskUsageSamplerOptions {
    pub fn new(data_dir: impl Into<String>) -> Self {
        Self {
            data_dir: data_dir.into(),
            initial_delay_ms: 30_000,
            interval_ms: 10 * 60_000,
            batch_size: 100,
            batch_pause_ms: 20,
            max_depth: 32,
            max_entries: 200_000,
            budget_ms: 10_000,
        }
    }
}

struct SamplerState {
    snapshot: DiskUsageSnapshot,
    active: bool,
    stopped: bool,
    failures: u32,
    next_attempt_at: i64,
    in_flight: bool,
    opened: Vec<PathBuf>,
}

pub struct DiskUsageSampler {
    options: DiskUsageSamplerOptions,
    state: Arc<Mutex<SamplerState>>,
    generation: Arc<AtomicU64>,
    cancel_pause: Arc<AtomicBool>,
    on_sample: Arc<dyn Fn(DiskUsageSnapshot) + Send + Sync>,
    now_ms: Arc<dyn Fn() -> i64 + Send + Sync>,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl DiskUsageSampler {
    pub fn new(
        options: DiskUsageSamplerOptions,
        on_sample: Arc<dyn Fn(DiskUsageSnapshot) + Send + Sync>,
    ) -> Self {
        Self {
            options,
            state: Arc::new(Mutex::new(SamplerState {
                snapshot: DiskUsageSnapshot::default(),
                active: false,
                stopped: false,
                failures: 0,
                next_attempt_at: 0,
                in_flight: false,
                opened: Vec::new(),
            })),
            generation: Arc::new(AtomicU64::new(0)),
            cancel_pause: Arc::new(AtomicBool::new(false)),
            on_sample,
            now_ms: Arc::new(|| chrono::Utc::now().timestamp_millis()),
            task: Mutex::new(None),
        }
    }

    pub fn start(self: &Arc<Self>) {
        let mut state = self.state.lock().expect("disk sampler");
        if state.active {
            return;
        }
        state.active = true;
        state.stopped = false;
        state.next_attempt_at = 0;
        drop(state);
        let sampler = Arc::clone(self);
        let handle = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(sampler.options.initial_delay_ms)).await;
            loop {
                if !sampler.is_active() {
                    break;
                }
                let now = (sampler.now_ms)();
                let due = sampler.state.lock().expect("disk sampler").next_attempt_at;
                if now >= due {
                    sampler.sample().await;
                }
                tokio::time::sleep(Duration::from_millis(sampler.options.interval_ms)).await;
            }
        });
        *self.task.lock().expect("disk task") = Some(handle);
    }

    pub fn stop(&self) {
        let mut state = self.state.lock().expect("disk sampler");
        state.active = false;
        state.stopped = true;
        drop(state);
        self.generation.fetch_add(1, Ordering::SeqCst);
        self.cancel_pause.store(true, Ordering::SeqCst);
        if let Some(task) = self.task.lock().expect("disk task").take() {
            task.abort();
        }
    }

    pub fn get_snapshot(&self) -> DiskUsageSnapshot {
        let state = self.state.lock().expect("disk sampler");
        let mut snapshot = state.snapshot.clone();
        if snapshot.status == DiskUsageStatus::Ok {
            if let Some(sampled_at) = snapshot.sampled_at {
                if (self.now_ms)() - sampled_at > DISK_USAGE_STALE_MS {
                    snapshot.status = DiskUsageStatus::Stale;
                }
            }
        }
        snapshot
    }

    pub fn store_snapshot(&self, snapshot: DiskUsageSnapshot) {
        self.state.lock().expect("disk sampler").snapshot = snapshot;
    }

    pub fn opened_directories(&self) -> Vec<PathBuf> {
        self.state.lock().expect("disk sampler").opened.clone()
    }

    pub async fn sample(self: &Arc<Self>) {
        if self.state.lock().expect("disk sampler").stopped {
            return;
        }
        let generation = self.generation.load(Ordering::SeqCst);
        {
            let mut state = self.state.lock().expect("disk sampler");
            if state.in_flight {
                return;
            }
            state.in_flight = true;
            state.opened.clear();
        }
        let started_at = (self.now_ms)();
        let started = Instant::now();
        let outcome = walk(self, generation, started).await;
        let mut state = self.state.lock().expect("disk sampler");
        state.in_flight = false;
        if generation != self.generation.load(Ordering::SeqCst) {
            return;
        }
        let scan_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
        if outcome.status == DiskUsageStatus::Ok {
            state.snapshot = DiskUsageSnapshot {
                status: DiskUsageStatus::Ok,
                data_bytes: Some(outcome.data_bytes),
                logs_bytes: Some(outcome.logs_bytes),
                sampled_at: Some((self.now_ms)()),
                scan_ms: Some(scan_ms),
            };
            state.failures = 0;
        } else if outcome.status != DiskUsageStatus::Pending {
            state.snapshot.status = outcome.status;
            state.snapshot.scan_ms = Some(scan_ms);
            state.failures += 1;
        }
        let multiplier = backoff_multiplier(state.failures);
        state.next_attempt_at = started_at + self.options.interval_ms as i64 * multiplier as i64;
        let published = self.get_snapshot_locked(&state);
        let failed = outcome.status != DiskUsageStatus::Ok;
        drop(state);
        if failed {
            let logger = create_logger("DiskUsageSampler");
            let mut meta = serde_json::Map::new();
            meta.insert(
                "status".into(),
                serde_json::Value::String(outcome.status.as_str().into()),
            );
            meta.insert("scanMs".into(), serde_json::json!(scan_ms));
            logger.debug("Directory usage scan incomplete", Some(&meta));
        }
        if let Err(err) =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| (self.on_sample)(published)))
        {
            let logger = create_logger("DiskUsageSampler");
            let mut meta = serde_json::Map::new();
            meta.insert("err".into(), serde_json::Value::String(format!("{err:?}")));
            logger.warn("Directory usage sample callback failed", Some(&meta));
        }
    }

    fn is_active(&self) -> bool {
        self.state.lock().expect("disk sampler").active
    }

    fn get_snapshot_locked(&self, state: &SamplerState) -> DiskUsageSnapshot {
        let mut snapshot = state.snapshot.clone();
        if snapshot.status == DiskUsageStatus::Ok {
            if let Some(sampled_at) = snapshot.sampled_at {
                if (self.now_ms)() - sampled_at > DISK_USAGE_STALE_MS {
                    snapshot.status = DiskUsageStatus::Stale;
                }
            }
        }
        snapshot
    }
}

pub fn backoff_multiplier(failures: u32) -> u32 {
    if failures < 2 {
        1
    } else {
        6.min(2u32.saturating_pow(failures.saturating_sub(1).min(3)))
    }
}

struct WalkOutcome {
    status: DiskUsageStatus,
    data_bytes: u64,
    logs_bytes: u64,
}

struct Frame {
    path: PathBuf,
    entries: Vec<fs::DirEntry>,
    index: usize,
    dev: u64,
    ino: u64,
    in_logs: bool,
}

async fn walk(sampler: &DiskUsageSampler, generation: u64, started: Instant) -> WalkOutcome {
    let mut frames: Vec<Frame> = Vec::new();
    let mut status = DiskUsageStatus::Ok;
    let mut data_bytes = 0u64;
    let mut logs_bytes = 0u64;
    let mut entries = 0u64;
    let mut completed = false;
    let root = match fs::canonicalize(&sampler.options.data_dir) {
        Ok(path) => path,
        Err(_) => {
            return WalkOutcome {
                status: DiskUsageStatus::Error,
                data_bytes: 0,
                logs_bytes: 0,
            };
        }
    };
    let check = |sampler: &DiskUsageSampler| -> Result<(), DiskUsageStatus> {
        if generation != sampler.generation.load(Ordering::SeqCst) {
            return Err(DiskUsageStatus::Error);
        }
        if started.elapsed().as_millis() >= sampler.options.budget_ms {
            return Err(DiskUsageStatus::Timeout);
        }
        Ok(())
    };
    let open_result = (|| {
        check(sampler)?;
        let meta = fs::symlink_metadata(&root).map_err(|_| DiskUsageStatus::Error)?;
        if !meta.is_dir() || meta.file_type().is_symlink() {
            return Err(DiskUsageStatus::Error);
        }
        let (dev, ino) = identity(&meta);
        let listed = fs::read_dir(&root).map_err(|_| DiskUsageStatus::Error)?;
        let children: Vec<_> = listed.flatten().collect();
        frames.push(Frame {
            path: root.clone(),
            entries: children,
            index: 0,
            dev,
            ino,
            in_logs: false,
        });
        sampler
            .state
            .lock()
            .expect("disk sampler")
            .opened
            .push(root.clone());
        Ok::<(), DiskUsageStatus>(())
    })();
    if open_result.is_err() {
        return WalkOutcome {
            status: open_result.err().unwrap_or(DiskUsageStatus::Error),
            data_bytes: 0,
            logs_bytes: 0,
        };
    }
    loop {
        if frames.is_empty() {
            completed = true;
            break;
        }
        if let Err(interrupted) = check(sampler) {
            status = interrupted;
            break;
        }
        if entries >= sampler.options.max_entries {
            status = DiskUsageStatus::Partial;
            break;
        }
        let frame_index = frames.len() - 1;
        if !directory_still(&frames[frame_index]) {
            status = if frames[frame_index].path == root {
                DiskUsageStatus::Error
            } else {
                DiskUsageStatus::Partial
            };
            frames.pop();
            if status == DiskUsageStatus::Error {
                break;
            }
            continue;
        }
        if frames[frame_index].index >= frames[frame_index].entries.len() {
            frames.pop();
            if frames.is_empty() {
                completed = true;
            }
            continue;
        }
        let entry = frames[frame_index].entries[frames[frame_index].index].path();
        let name = frames[frame_index].entries[frames[frame_index].index].file_name();
        frames[frame_index].index += 1;
        entries += 1;
        if sampler.options.batch_size > 0 && entries.is_multiple_of(sampler.options.batch_size) {
            sampler.cancel_pause.store(false, Ordering::SeqCst);
            let pause = Duration::from_millis(sampler.options.batch_pause_ms);
            tokio::select! {
                _ = tokio::time::sleep(pause) => {}
                _ = wait_cancel(&sampler.cancel_pause) => {}
            }
            if let Err(interrupted) = check(sampler) {
                status = interrupted;
                break;
            }
        }
        let file_type = match fs::symlink_metadata(&entry) {
            Ok(meta) => meta,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(_) => {
                status = DiskUsageStatus::Partial;
                break;
            }
        };
        if file_type.file_type().is_symlink() {
            continue;
        }
        if file_type.is_dir() {
            let normalized = name.to_string_lossy().to_lowercase();
            let parent = frames
                .last()
                .map(|frame| frame.path.clone())
                .unwrap_or_default();
            if parent == root && !SCANNED_DATA_DIRECTORIES.contains(&normalized.as_str()) {
                continue;
            }
            if is_local_worker_payload(&parent, &normalized, &root) {
                continue;
            }
            if frames.len() >= sampler.options.max_depth {
                status = DiskUsageStatus::Partial;
                break;
            }
            let in_logs = frames.last().map(|frame| frame.in_logs).unwrap_or(false)
                || (parent == root && normalized == "logs");
            match fs::read_dir(&entry) {
                Ok(listed) => {
                    let (dev, ino) = identity(&file_type);
                    frames.push(Frame {
                        path: entry.clone(),
                        entries: listed.flatten().collect(),
                        index: 0,
                        dev,
                        ino,
                        in_logs,
                    });
                    sampler
                        .state
                        .lock()
                        .expect("disk sampler")
                        .opened
                        .push(entry);
                }
                Err(_) => {
                    status = DiskUsageStatus::Partial;
                    break;
                }
            }
        } else if file_type.is_file() {
            data_bytes += file_type.len();
            if frames.last().map(|frame| frame.in_logs).unwrap_or(false) {
                logs_bytes += file_type.len();
            }
        }
    }
    if generation != sampler.generation.load(Ordering::SeqCst) {
        return WalkOutcome {
            status: DiskUsageStatus::Pending,
            data_bytes: 0,
            logs_bytes: 0,
        };
    }
    if status == DiskUsageStatus::Ok
        && !completed
        && started.elapsed().as_millis() >= sampler.options.budget_ms
    {
        status = DiskUsageStatus::Timeout;
    }
    WalkOutcome {
        status,
        data_bytes,
        logs_bytes,
    }
}

fn is_local_worker_payload(directory: &Path, name: &str, root: &Path) -> bool {
    let parent_is_worker = directory
        .parent()
        .map(|parent| parent == root.join("local-workers"))
        .unwrap_or(false);
    parent_is_worker
        && (name == "bundle"
            || name.starts_with(".bundle.staging-")
            || name.starts_with(".bundle.backup-"))
}

fn directory_still(frame: &Frame) -> bool {
    match fs::symlink_metadata(&frame.path) {
        Ok(meta) if meta.is_dir() && !meta.file_type().is_symlink() => {
            let (dev, ino) = identity(&meta);
            dev == frame.dev && ino == frame.ino
        }
        _ => false,
    }
}

fn identity(meta: &fs::Metadata) -> (u64, u64) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        (meta.dev(), meta.ino())
    }
    #[cfg(not(unix))]
    {
        let _ = meta;
        (0, 0)
    }
}

async fn wait_cancel(flag: &AtomicBool) {
    while !flag.load(Ordering::SeqCst) {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    fn write_file(path: &Path, bytes: usize) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, vec![b'x'; bytes]).unwrap();
    }

    #[tokio::test]
    async fn counts_mutable_data_and_skips_managed_roots_and_bundles() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write_file(&root.join(".hidden"), 3);
        write_file(&root.join("logs").join("output").join("a.jsonl"), 7);
        write_file(&root.join("local-workers").join("logs").join("b"), 11);
        write_file(&root.join("versions").join("payload.bin"), 1000);
        write_file(
            &root
                .join("local-workers")
                .join("lw_1")
                .join("state")
                .join("checkpoint.json"),
            5,
        );
        write_file(
            &root
                .join("local-workers")
                .join("lw_1")
                .join("bundle")
                .join("index.js"),
            101,
        );
        write_file(&root.join("state").join("checkpoint.json"), 7);
        #[cfg(unix)]
        std::os::unix::fs::symlink(root.join("logs"), root.join("linked-logs")).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_cb = calls.clone();
        let sampler = Arc::new(DiskUsageSampler::new(
            DiskUsageSamplerOptions {
                batch_pause_ms: 0,
                ..DiskUsageSamplerOptions::new(root.to_string_lossy())
            },
            Arc::new(move |_| {
                calls_cb.fetch_add(1, Ordering::SeqCst);
            }),
        ));
        assert_eq!(sampler.get_snapshot().status, DiskUsageStatus::Pending);
        sampler.sample().await;
        let snapshot = sampler.get_snapshot();
        assert_eq!(snapshot.status, DiskUsageStatus::Ok);
        assert_eq!(snapshot.data_bytes, Some(3 + 7 + 11 + 5 + 7));
        assert_eq!(snapshot.logs_bytes, Some(7));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let opened = sampler.opened_directories();
        assert!(opened.iter().all(|path| !path.ends_with("versions")));
        assert!(opened
            .iter()
            .all(|path| path.file_name().and_then(|name| name.to_str()) != Some("bundle")));
        let copy = sampler.get_snapshot();
        assert_eq!(copy.data_bytes, Some(33));
    }

    #[tokio::test]
    async fn reports_an_empty_directory_as_zero_and_returns_a_copy() {
        let dir = tempfile::tempdir().unwrap();
        let sampler = Arc::new(DiskUsageSampler::new(
            DiskUsageSamplerOptions {
                batch_pause_ms: 0,
                ..DiskUsageSamplerOptions::new(dir.path().to_string_lossy())
            },
            Arc::new(|_| {}),
        ));
        sampler.sample().await;
        let mut snapshot = sampler.get_snapshot();
        assert_eq!(snapshot.data_bytes, Some(0));
        snapshot.data_bytes = Some(999);
        assert_eq!(snapshot.data_bytes, Some(999));
        assert_eq!(sampler.get_snapshot().data_bytes, Some(0));
    }

    #[tokio::test]
    async fn does_not_publish_partial_sums_when_the_entry_budget_is_hit() {
        let dir = tempfile::tempdir().unwrap();
        for index in 0..30 {
            write_file(&dir.path().join("logs").join(format!("f{index}")), 4);
        }
        let sampler = Arc::new(DiskUsageSampler::new(
            DiskUsageSamplerOptions {
                batch_pause_ms: 0,
                max_entries: 5,
                ..DiskUsageSamplerOptions::new(dir.path().to_string_lossy())
            },
            Arc::new(|_| {}),
        ));
        sampler.sample().await;
        let snapshot = sampler.get_snapshot();
        assert_eq!(snapshot.status, DiskUsageStatus::Partial);
        assert!(snapshot.data_bytes.is_none());
    }

    #[test]
    fn backs_off_after_two_failures() {
        assert_eq!(backoff_multiplier(0), 1);
        assert_eq!(backoff_multiplier(1), 1);
        assert_eq!(backoff_multiplier(2), 2);
        assert_eq!(backoff_multiplier(3), 4);
        assert_eq!(backoff_multiplier(4), 6);
        assert_eq!(backoff_multiplier(8), 6);
    }

    #[tokio::test]
    async fn marks_a_missing_root_as_error() {
        let sampler = Arc::new(DiskUsageSampler::new(
            DiskUsageSamplerOptions::new("/no/such/pilot-data-dir"),
            Arc::new(|_| {}),
        ));
        sampler.sample().await;
        assert_eq!(sampler.get_snapshot().status, DiskUsageStatus::Error);
        assert!(sampler.get_snapshot().data_bytes.is_none());
    }
}
