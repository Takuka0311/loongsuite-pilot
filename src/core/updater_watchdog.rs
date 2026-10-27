//! Collector-side updater liveness watchdog. Port of `updater-watchdog.ts`.
//!
//! It does not download packages. It observes the updater pid and heartbeat
//! and asks `loongsuite-pilot restart-updater` to recover.

use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::task::JoinHandle;

use crate::metrics::{AlarmContext, AlarmManager};
use crate::updater::{updater_runtime_path, UpdaterRuntimeState};
use crate::utils::{
    check_process_liveness, describe_restart_command_error, is_restart_command_timeout,
    is_restart_failure_fresh, read_json_file, read_restart_failure, sanitize_alarm_text,
    summarize_restart_failure, write_restart_failure, ProcessLiveness, RestartCommandError,
    RestartExitCode, RestartFailureWrite, RestartTarget, UPDATER_PROCESS_PATTERNS,
};

use super::{fields, logger};

const DEFAULT_INTERVAL_MS: u64 = 60_000;
const DEFAULT_STALE_HEARTBEAT_MS: u64 = 3 * 60_000;
const DEFAULT_STARTUP_GRACE_MS: u64 = 3 * 60_000;
const DEFAULT_SLEEP_WAKE_GRACE_MS: u64 = 3 * 60_000;
const DEFAULT_RESTART_COOLDOWN_MS: u64 = 10 * 60_000;
const COMMAND_TIMEOUT_MS: u64 = 90_000;
const RECENT_RESTART_FAILURE_MS: i64 = 30 * 60_000;

pub type BoxFut<T> = Pin<Box<dyn std::future::Future<Output = T> + Send>>;
pub type UpdaterLivenessProbe = Arc<dyn Fn(&str) -> BoxFut<ProcessLiveness> + Send + Sync>;
pub type UpdaterRestartCommand =
    Arc<dyn Fn() -> BoxFut<Result<(String, String), RestartCommandError>> + Send + Sync>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdaterWatchdogStatus {
    Disabled,
    Healthy,
    MissingProcess,
    CommandMismatch,
    MissingHeartbeat,
    StaleHeartbeat,
    PidMismatch,
    Grace,
    RestartRateLimited,
    RestartAttempted,
    RestartFailed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdaterWatchdogResult {
    pub status: UpdaterWatchdogStatus,
    pub reason: Option<String>,
    pub restarted: Option<bool>,
}

pub struct UpdaterWatchdogOptions {
    pub enabled: bool,
    pub data_dir: String,
    pub loongsuite_pilot_bin: Option<String>,
    pub interval_ms: Option<u64>,
    pub stale_heartbeat_ms: Option<u64>,
    pub startup_grace_ms: Option<u64>,
    pub sleep_wake_grace_ms: Option<u64>,
    pub restart_cooldown_ms: Option<u64>,
    pub alarm_manager: Option<Arc<AlarmManager>>,
    pub updater_liveness: Option<UpdaterLivenessProbe>,
    pub restart_command: Option<UpdaterRestartCommand>,
    pub now_ms: Option<Arc<dyn Fn() -> i64 + Send + Sync>>,
}

pub struct UpdaterWatchdog {
    enabled: bool,
    data_dir: String,
    interval_ms: u64,
    stale_heartbeat_ms: u64,
    startup_grace_ms: u64,
    sleep_wake_grace_ms: u64,
    restart_cooldown_ms: u64,
    alarm_manager: Option<Arc<AlarmManager>>,
    liveness: UpdaterLivenessProbe,
    restart_command: UpdaterRestartCommand,
    now_ms: Arc<dyn Fn() -> i64 + Send + Sync>,
    started_at: Mutex<i64>,
    last_tick_at: Mutex<i64>,
    sleep_wake_grace_until: Mutex<i64>,
    last_restart_at: Mutex<i64>,
    checking: AtomicBool,
    stopping: AtomicBool,
    timer: Mutex<Option<JoinHandle<()>>>,
}

impl UpdaterWatchdog {
    pub fn new(opts: UpdaterWatchdogOptions) -> Self {
        let bin = opts.loongsuite_pilot_bin.unwrap_or_else(default_pilot_bin);
        let liveness = opts.updater_liveness.unwrap_or_else(|| {
            Arc::new(|pid_file: &str| {
                let pid_file = pid_file.to_string();
                Box::pin(async move { check_process_liveness(&pid_file, UPDATER_PROCESS_PATTERNS) })
                    as BoxFut<_>
            })
        });
        let restart_bin = bin.clone();
        let restart_command = opts.restart_command.unwrap_or_else(|| {
            Arc::new(move || {
                let bin = restart_bin.clone();
                Box::pin(async move { run_restart_command(&bin).await }) as BoxFut<_>
            })
        });
        Self {
            enabled: opts.enabled,
            data_dir: opts.data_dir,
            interval_ms: opts.interval_ms.unwrap_or(DEFAULT_INTERVAL_MS),
            stale_heartbeat_ms: opts
                .stale_heartbeat_ms
                .unwrap_or(DEFAULT_STALE_HEARTBEAT_MS),
            startup_grace_ms: opts.startup_grace_ms.unwrap_or(DEFAULT_STARTUP_GRACE_MS),
            sleep_wake_grace_ms: opts
                .sleep_wake_grace_ms
                .unwrap_or(DEFAULT_SLEEP_WAKE_GRACE_MS),
            restart_cooldown_ms: opts
                .restart_cooldown_ms
                .unwrap_or(DEFAULT_RESTART_COOLDOWN_MS),
            alarm_manager: opts.alarm_manager,
            liveness,
            restart_command,
            now_ms: opts.now_ms.unwrap_or_else(|| Arc::new(now_ms)),
            started_at: Mutex::new(0),
            last_tick_at: Mutex::new(0),
            sleep_wake_grace_until: Mutex::new(0),
            last_restart_at: Mutex::new(0),
            checking: AtomicBool::new(false),
            stopping: AtomicBool::new(false),
            timer: Mutex::new(None),
        }
    }

    pub fn start(self: &Arc<Self>) {
        if !self.enabled {
            logger("UpdaterWatchdog").info("updater-watchdog disabled", None);
            return;
        }
        if is_agentshell(&self.data_dir) {
            logger("UpdaterWatchdog")
                .info("updater-watchdog disabled for agentshell version", None);
            return;
        }
        *self.started_at.lock().expect("start") = (self.now_ms)();
        *self.last_tick_at.lock().expect("tick") = 0;
        self.stopping.store(false, Ordering::SeqCst);
        let this = Arc::clone(self);
        tokio::spawn(async move {
            let _ = this.run_check().await;
        });
        let this = Arc::clone(self);
        *self.timer.lock().expect("timer") = Some(tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_millis(this.interval_ms.max(1)));
            ticker.tick().await;
            loop {
                ticker.tick().await;
                if this.stopping.load(Ordering::SeqCst) {
                    break;
                }
                let _ = this.run_check().await;
            }
        }));
    }

    pub fn stop(&self) {
        self.stopping.store(true, Ordering::SeqCst);
        if let Some(task) = self.timer.lock().expect("timer").take() {
            task.abort();
        }
    }

    pub async fn run_check(&self) -> UpdaterWatchdogResult {
        if !self.enabled || is_agentshell(&self.data_dir) {
            return result(UpdaterWatchdogStatus::Disabled, None, None);
        }
        if self.stopping.load(Ordering::SeqCst) {
            return result(
                UpdaterWatchdogStatus::Disabled,
                Some("stopped".into()),
                None,
            );
        }
        if self.checking.swap(true, Ordering::SeqCst) {
            return result(
                UpdaterWatchdogStatus::RestartRateLimited,
                Some("check already in flight".into()),
                Some(false),
            );
        }
        let outcome = self.run_check_locked().await;
        self.checking.store(false, Ordering::SeqCst);
        outcome
    }

    async fn run_check_locked(&self) -> UpdaterWatchdogResult {
        let now = (self.now_ms)();
        let last_tick = *self.last_tick_at.lock().expect("tick");
        if last_tick > 0
            && now - last_tick > self.interval_ms as i64 + self.sleep_wake_grace_ms as i64
        {
            *self.sleep_wake_grace_until.lock().expect("grace") =
                now + self.sleep_wake_grace_ms as i64;
        }
        *self.last_tick_at.lock().expect("tick") = now;
        if self.stopping.load(Ordering::SeqCst) {
            return result(
                UpdaterWatchdogStatus::RestartFailed,
                Some("aborted by stop".into()),
                Some(false),
            );
        }
        let process = self.read_process().await;
        if !process.running {
            if self.in_grace(now) {
                return result(UpdaterWatchdogStatus::Grace, Some(process.reason), None);
            }
            self.record_service(&self.with_last_failure(&process.reason).await);
            return self
                .restart(UpdaterWatchdogStatus::MissingProcess, &process.reason)
                .await;
        }
        if !process.command_ok {
            let reason = format!("updater pid {} command mismatch", process.pid.unwrap_or(0));
            if self.in_grace(now) {
                return result(UpdaterWatchdogStatus::Grace, Some(reason), None);
            }
            self.record_failure(&self.with_last_failure(&reason).await);
            return self
                .restart(UpdaterWatchdogStatus::CommandMismatch, &reason)
                .await;
        }
        let heartbeat: Option<UpdaterRuntimeState> =
            read_json_file(&updater_runtime_path(&self.data_dir)).await;
        if self.stopping.load(Ordering::SeqCst) {
            return result(
                UpdaterWatchdogStatus::RestartFailed,
                Some("aborted by stop".into()),
                Some(false),
            );
        }
        let Some(heartbeat) = heartbeat else {
            let reason = "updater heartbeat is missing";
            if self.in_grace(now) {
                return result(UpdaterWatchdogStatus::Grace, Some(reason.into()), None);
            }
            self.record_failure(&self.with_last_failure(reason).await);
            return self
                .restart(UpdaterWatchdogStatus::MissingHeartbeat, reason)
                .await;
        };
        if !cfg!(windows) {
            if let Some(pid) = process.pid {
                if heartbeat.pid != pid {
                    let reason = format!(
                        "updater heartbeat pid {} does not match running pid {pid}",
                        heartbeat.pid
                    );
                    if self.in_grace(now) {
                        return result(UpdaterWatchdogStatus::Grace, Some(reason), None);
                    }
                    self.record_failure(&self.with_last_failure(&reason).await);
                    return self
                        .restart(UpdaterWatchdogStatus::PidMismatch, &reason)
                        .await;
                }
            }
        }
        let heartbeat_at = chrono::DateTime::parse_from_rfc3339(&heartbeat.updated_at)
            .map(|time| time.timestamp_millis())
            .unwrap_or(i64::MIN);
        if heartbeat_at == i64::MIN || now - heartbeat_at > self.stale_heartbeat_ms as i64 {
            let reason = "updater heartbeat is stale";
            if self.in_grace(now) {
                return result(UpdaterWatchdogStatus::Grace, Some(reason.into()), None);
            }
            self.record_failure(&self.with_last_failure(reason).await);
            return self
                .restart(UpdaterWatchdogStatus::StaleHeartbeat, reason)
                .await;
        }
        result(UpdaterWatchdogStatus::Healthy, None, None)
    }

    async fn read_process(&self) -> ProcessView {
        let pid_file = Path::new(&self.data_dir).join("loongsuite-pilot-updater.pid");
        let liveness = (self.liveness)(&pid_file.to_string_lossy()).await;
        if !liveness.running {
            if liveness.pid.is_some()
                && liveness.pid_file_process_alive == Some(true)
                && liveness.pid_file_command_matched == Some(false)
            {
                return ProcessView {
                    running: true,
                    pid: liveness.pid,
                    command_ok: false,
                    reason: format!(
                        "unexpected updater command: {}",
                        liveness
                            .pid_file_command
                            .unwrap_or_else(|| "unknown".into())
                    ),
                };
            }
            return ProcessView {
                running: false,
                pid: liveness.pid,
                command_ok: false,
                reason: liveness.reason,
            };
        }
        ProcessView {
            running: true,
            pid: liveness.pid,
            command_ok: true,
            reason: liveness.reason,
        }
    }

    fn in_grace(&self, now: i64) -> bool {
        now - *self.started_at.lock().expect("start") < self.startup_grace_ms as i64
            || now < *self.sleep_wake_grace_until.lock().expect("grace")
    }

    async fn restart(&self, status: UpdaterWatchdogStatus, reason: &str) -> UpdaterWatchdogResult {
        if self.stopping.load(Ordering::SeqCst) {
            return result(
                UpdaterWatchdogStatus::RestartFailed,
                Some("aborted by stop".into()),
                Some(false),
            );
        }
        let now = (self.now_ms)();
        let last = *self.last_restart_at.lock().expect("restart");
        if last > 0 && now - last < self.restart_cooldown_ms as i64 {
            return result(
                UpdaterWatchdogStatus::RestartRateLimited,
                Some(reason.into()),
                Some(false),
            );
        }
        *self.last_restart_at.lock().expect("restart") = now;
        match (self.restart_command)().await {
            Ok((stdout, stderr)) => {
                logger("UpdaterWatchdog").warn(
                    "updater-watchdog requested updater restart",
                    Some(&fields(serde_json::json!({
                        "status": format!("{status:?}"),
                        "reason": reason,
                        "stdout": sanitize_alarm_text(&stdout, 1_000),
                        "stderr": sanitize_alarm_text(&stderr, 1_000),
                    }))),
                );
                result(
                    UpdaterWatchdogStatus::RestartAttempted,
                    Some(reason.into()),
                    Some(true),
                )
            }
            Err(err) => {
                if self.stopping.load(Ordering::SeqCst) {
                    return result(
                        UpdaterWatchdogStatus::RestartFailed,
                        Some("aborted by stop".into()),
                        Some(false),
                    );
                }
                if is_restart_command_timeout(Some(&err)) {
                    write_restart_failure(
                        &self.data_dir,
                        RestartTarget::Updater,
                        &RestartFailureWrite {
                            stage: "timeout".into(),
                            init_type: None,
                            detail: Some(
                                "restart command killed by timeout before it reported a stage"
                                    .into(),
                            ),
                            diag: None,
                        },
                    );
                }
                let breadcrumb = read_restart_failure(&self.data_dir, RestartTarget::Updater)
                    .await
                    .filter(|item| is_restart_failure_fresh(item, now as f64, None));
                let diagnosis = breadcrumb.as_ref().map(summarize_restart_failure).unwrap_or_else(|| {
                    if is_restart_command_timeout(Some(&err)) {
                        "stage=timeout reason=\"restart command killed by timeout before it reported a stage\"".into()
                    } else {
                        "stage=unknown reason=\"restart command left no diagnostics\"".into()
                    }
                });
                let message = format!(
                    "updater restart command failed: {} | {diagnosis}",
                    describe_restart_command_error(Some(&err))
                );
                self.record_failure(&message);
                result(
                    UpdaterWatchdogStatus::RestartFailed,
                    Some(message),
                    Some(false),
                )
            }
        }
    }

    async fn with_last_failure(&self, message: &str) -> String {
        let not_before = (self.now_ms)() - RECENT_RESTART_FAILURE_MS;
        let breadcrumb = read_restart_failure(&self.data_dir, RestartTarget::Updater)
            .await
            .filter(|item| is_restart_failure_fresh(item, not_before as f64, None));
        match breadcrumb {
            Some(breadcrumb) => format!(
                "{message} | last_restart_failure: {}",
                summarize_restart_failure(&breadcrumb)
            ),
            None => message.to_string(),
        }
    }

    fn record_service(&self, message: &str) {
        if let Some(alarm) = &self.alarm_manager {
            alarm.record(
                "SERVICE_NOT_RUNNING_ALARM",
                "3",
                message,
                Some(AlarmContext {
                    input_name: Some("updater".into()),
                    endpoint_name: None,
                }),
            );
        }
    }

    fn record_failure(&self, message: &str) {
        if let Some(alarm) = &self.alarm_manager {
            alarm.record(
                "UPDATER_FAILURE_ALARM",
                "2",
                message,
                Some(AlarmContext {
                    input_name: Some("updater".into()),
                    endpoint_name: None,
                }),
            );
        }
    }
}

struct ProcessView {
    running: bool,
    pid: Option<u32>,
    command_ok: bool,
    reason: String,
}

fn result(
    status: UpdaterWatchdogStatus,
    reason: Option<String>,
    restarted: Option<bool>,
) -> UpdaterWatchdogResult {
    UpdaterWatchdogResult {
        status,
        reason,
        restarted,
    }
}

fn default_pilot_bin() -> String {
    let home = std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .unwrap_or_default();
    if cfg!(windows) {
        Path::new(&home)
            .join(".local")
            .join("bin")
            .join("loongsuite-pilot-service.ps1")
            .to_string_lossy()
            .into_owned()
    } else {
        Path::new(&home)
            .join(".local")
            .join("bin")
            .join("loongsuite-pilot")
            .to_string_lossy()
            .into_owned()
    }
}

fn is_agentshell(data_dir: &str) -> bool {
    std::fs::read_to_string(Path::new(data_dir).join("current"))
        .map(|text| text.to_ascii_lowercase().contains("-agentshell"))
        .unwrap_or(false)
}

async fn run_restart_command(bin: &str) -> Result<(String, String), RestartCommandError> {
    let mut command = if cfg!(windows) {
        let mut command = tokio::process::Command::new("powershell.exe");
        command.args([
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            bin,
            "restart-updater",
        ]);
        command
    } else {
        let mut command = tokio::process::Command::new(bin);
        command.arg("restart-updater");
        command
    };
    command
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    let child = command.output();
    match tokio::time::timeout(Duration::from_millis(COMMAND_TIMEOUT_MS), child).await {
        Ok(Ok(output)) if output.status.success() => Ok((
            String::from_utf8_lossy(&output.stdout).into_owned(),
            String::from_utf8_lossy(&output.stderr).into_owned(),
        )),
        Ok(Ok(output)) => Err(RestartCommandError {
            code: output
                .status
                .code()
                .map(|code| RestartExitCode::Number(code as i64)),
            stdout: Some(String::from_utf8_lossy(&output.stdout).into_owned()),
            stderr: Some(String::from_utf8_lossy(&output.stderr).into_owned()),
            ..RestartCommandError::default()
        }),
        Ok(Err(err)) => Err(RestartCommandError {
            message: Some(err.to_string()),
            ..RestartCommandError::default()
        }),
        Err(_) => Err(RestartCommandError {
            killed: Some(true),
            code: Some(RestartExitCode::Name("ETIMEDOUT".into())),
            ..RestartCommandError::default()
        }),
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn alive(pid: u32) -> ProcessLiveness {
        ProcessLiveness {
            running: true,
            pid: Some(pid),
            source: crate::utils::ProcessLivenessSource::PidFile,
            reason: "ok".into(),
            pid_file_state: None,
            pid_file_process_alive: Some(true),
            pid_file_command: Some("loongsuite-pilot-updater".into()),
            pid_file_command_matched: Some(true),
        }
    }

    fn missing() -> ProcessLiveness {
        ProcessLiveness {
            running: false,
            pid: None,
            source: crate::utils::ProcessLivenessSource::None,
            reason: "missing".into(),
            pid_file_state: None,
            pid_file_process_alive: None,
            pid_file_command: None,
            pid_file_command_matched: None,
        }
    }

    fn watchdog(dir: &std::path::Path, liveness: ProcessLiveness, grace: u64) -> UpdaterWatchdog {
        let clock = Arc::new(std::sync::atomic::AtomicI64::new(10_000));
        let now = Arc::clone(&clock);
        UpdaterWatchdog::new(UpdaterWatchdogOptions {
            enabled: true,
            data_dir: dir.to_string_lossy().into_owned(),
            loongsuite_pilot_bin: Some("pilot".into()),
            interval_ms: Some(60_000),
            stale_heartbeat_ms: Some(1_000),
            startup_grace_ms: Some(grace),
            sleep_wake_grace_ms: Some(0),
            restart_cooldown_ms: Some(50_000),
            alarm_manager: None,
            updater_liveness: Some(Arc::new(move |_| {
                let snapshot = ProcessLiveness {
                    running: liveness.running,
                    pid: liveness.pid,
                    source: liveness.source,
                    reason: liveness.reason.clone(),
                    pid_file_state: liveness.pid_file_state,
                    pid_file_process_alive: liveness.pid_file_process_alive,
                    pid_file_command: liveness.pid_file_command.clone(),
                    pid_file_command_matched: liveness.pid_file_command_matched,
                };
                Box::pin(async move { snapshot })
            })),
            restart_command: Some(Arc::new(|| {
                Box::pin(async { Ok(("ok".into(), String::new())) })
            })),
            now_ms: Some(Arc::new(move || {
                now.load(std::sync::atomic::Ordering::SeqCst)
            })),
        })
    }

    #[tokio::test]
    async fn healthy_when_heartbeat_matches_the_pid() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("logs")).unwrap();
        let updated = chrono::Utc::now().to_rfc3339();
        std::fs::write(dir.path().join("logs").join("updater-runtime.json"), serde_json::json!({
            "status": "running", "pid": 42, "version": "1.2.0", "updatedAt": updated, "consecutiveFailures": 0
        }).to_string()).unwrap();
        let watchdog = watchdog(dir.path(), alive(42), 0);
        *watchdog.started_at.lock().unwrap() = 0;
        let outcome = watchdog.run_check().await;
        assert_eq!(outcome.status, UpdaterWatchdogStatus::Healthy);
    }

    #[tokio::test]
    async fn startup_grace_skips_a_missing_updater() {
        let dir = tempfile::tempdir().unwrap();
        let watchdog = watchdog(dir.path(), missing(), 60_000);
        *watchdog.started_at.lock().unwrap() = 10_000;
        let outcome = watchdog.run_check().await;
        assert_eq!(outcome.status, UpdaterWatchdogStatus::Grace);
    }

    #[tokio::test]
    async fn agentshell_current_disables_the_watchdog() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("current"), "1.0.0-agentshell").unwrap();
        let watchdog = watchdog(dir.path(), missing(), 0);
        let outcome = watchdog.run_check().await;
        assert_eq!(outcome.status, UpdaterWatchdogStatus::Disabled);
    }
}
