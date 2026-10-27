//! One-shot removal of the pre-migration `sls-failed-logs` directory.
//! Port of `legacy-sls-failed-log-cleanup-service.ts`.

use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::task::JoinHandle;

use super::{fields, logger};

pub const LEGACY_SLS_CLEANUP_STARTUP_DELAY_MS: u64 = 30_000;
pub const LEGACY_SLS_CLEANUP_FILE_DELAY_MS: u64 = 100;
pub const LEGACY_SLS_CLEANUP_RETRY_DELAYS_MS: [u64; 3] = [250, 1_000, 4_000];

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LegacySlsFailedLogCleanupResult {
    pub renamed: bool,
    pub deleted: u64,
    pub skipped: u64,
    pub errors: u64,
    pub logical_bytes: u64,
}

pub struct LegacySlsFailedLogCleanupService {
    legacy_dir: PathBuf,
    pending_dir: PathBuf,
    startup_delay_ms: u64,
    file_delay_ms: u64,
    retry_delays_ms: Vec<u64>,
    startup: std::sync::Mutex<Option<JoinHandle<()>>>,
}

impl LegacySlsFailedLogCleanupService {
    pub fn new(data_dir: impl AsRef<Path>) -> Self {
        Self::with_delays(
            data_dir,
            LEGACY_SLS_CLEANUP_STARTUP_DELAY_MS,
            LEGACY_SLS_CLEANUP_FILE_DELAY_MS,
            LEGACY_SLS_CLEANUP_RETRY_DELAYS_MS.to_vec(),
        )
    }

    pub fn with_delays(
        data_dir: impl AsRef<Path>,
        startup_delay_ms: u64,
        file_delay_ms: u64,
        retry_delays_ms: Vec<u64>,
    ) -> Self {
        let data_dir = data_dir.as_ref();
        Self {
            legacy_dir: data_dir.join("sls-failed-logs"),
            pending_dir: data_dir.join("sls-failed-logs.delete-pending"),
            startup_delay_ms,
            file_delay_ms,
            retry_delays_ms,
            startup: std::sync::Mutex::new(None),
        }
    }

    pub fn start(self: &std::sync::Arc<Self>) {
        if self.startup.lock().expect("startup").is_some() {
            return;
        }
        let this = std::sync::Arc::clone(self);
        *self.startup.lock().expect("startup") = Some(tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(this.startup_delay_ms)).await;
            *this.startup.lock().expect("startup") = None;
            let _ = this.run_cleanup().await;
        }));
    }

    pub fn stop(&self) {
        if let Some(task) = self.startup.lock().expect("startup").take() {
            task.abort();
        }
    }

    pub async fn run_cleanup(&self) -> LegacySlsFailedLogCleanupResult {
        match self.run_once().await {
            Ok(result) => result,
            Err(err) => {
                logger("LegacySlsFailureCleanup").warn(
                    "legacy SLS failure cleanup failed",
                    Some(&fields(serde_json::json!({ "error": err }))),
                );
                let mut result = LegacySlsFailedLogCleanupResult::default();
                result.errors = 1;
                result
            }
        }
    }

    async fn run_once(&self) -> Result<LegacySlsFailedLogCleanupResult, String> {
        let mut result = LegacySlsFailedLogCleanupResult::default();
        if self.pending_dir.exists() {
            if !self.pending_dir.is_dir()
                || self
                    .pending_dir
                    .symlink_metadata()
                    .map(|meta| meta.file_type().is_symlink())
                    .unwrap_or(false)
            {
                logger("LegacySlsFailureCleanup").warn(
                    "legacy pending path is not a regular directory; skipping",
                    Some(&fields(
                        serde_json::json!({ "path": "sls-failed-logs.delete-pending" }),
                    )),
                );
                result.skipped += 1;
                return Ok(result);
            }
            self.clean_pending(&mut result).await;
            self.log_result(&result);
            return Ok(result);
        }
        if !self.legacy_dir.exists() {
            return Ok(result);
        }
        if !self.legacy_dir.is_dir() {
            logger("LegacySlsFailureCleanup").warn(
                "legacy SLS failure path is not a regular directory; skipping",
                Some(&fields(serde_json::json!({ "path": "sls-failed-logs" }))),
            );
            result.skipped += 1;
            return Ok(result);
        }
        if !self.retry_rename().await {
            result.errors += 1;
            return Ok(result);
        }
        result.renamed = true;
        self.clean_pending(&mut result).await;
        self.log_result(&result);
        Ok(result)
    }

    async fn clean_pending(&self, result: &mut LegacySlsFailedLogCleanupResult) {
        let entries = match std::fs::read_dir(&self.pending_dir) {
            Ok(entries) => entries.filter_map(|entry| entry.ok()).collect::<Vec<_>>(),
            Err(err) => {
                result.errors += 1;
                logger("LegacySlsFailureCleanup").warn(
                    "failed to enumerate legacy SLS failure directory",
                    Some(&fields(serde_json::json!({ "error": err.to_string() }))),
                );
                return;
            }
        };
        let mut candidates = Vec::new();
        for entry in &entries {
            let name = entry.file_name().to_string_lossy().into_owned();
            if is_legacy_jsonl_name(&name) {
                candidates.push(name);
            } else {
                result.skipped += 1;
                logger("LegacySlsFailureCleanup").warn(
                    "skipping unknown legacy SLS failure entry",
                    Some(&fields(serde_json::json!({ "file": name }))),
                );
            }
        }
        for (index, name) in candidates.iter().enumerate() {
            let path = self.pending_dir.join(name);
            let meta = match std::fs::symlink_metadata(&path) {
                Ok(meta) => meta,
                Err(err) => {
                    result.errors += 1;
                    logger("LegacySlsFailureCleanup").warn(
                        "failed to inspect legacy SLS failure file",
                        Some(&fields(
                            serde_json::json!({ "file": name, "error": err.to_string() }),
                        )),
                    );
                    continue;
                }
            };
            if meta.file_type().is_symlink() || !meta.is_file() {
                result.skipped += 1;
                continue;
            }
            if self.retry_unlink(&path).await {
                result.deleted += 1;
                result.logical_bytes += meta.len();
            } else {
                result.errors += 1;
            }
            if index + 1 < candidates.len() && self.file_delay_ms > 0 {
                tokio::time::sleep(Duration::from_millis(self.file_delay_ms)).await;
            }
        }
        if let Ok(remaining) = std::fs::read_dir(&self.pending_dir) {
            if remaining.count() == 0 {
                let _ = std::fs::remove_dir(&self.pending_dir);
            }
        }
    }

    async fn retry_rename(&self) -> bool {
        self.retry(
            || std::fs::rename(&self.legacy_dir, &self.pending_dir),
            "rename",
        )
        .await
    }

    async fn retry_unlink(&self, path: &Path) -> bool {
        let path = path.to_path_buf();
        self.retry(move || std::fs::remove_file(&path), "unlink")
            .await
    }

    async fn retry<F>(&self, mut operation: F, name: &str) -> bool
    where
        F: FnMut() -> std::io::Result<()>,
    {
        let mut attempt = 0;
        loop {
            match operation() {
                Ok(()) => return true,
                Err(err) => {
                    let code = io_code(&err);
                    if let Some(delay) = self
                        .retry_delays_ms
                        .get(attempt)
                        .copied()
                        .filter(|_| is_transient(&code))
                    {
                        attempt += 1;
                        tokio::time::sleep(Duration::from_millis(delay)).await;
                        continue;
                    }
                    logger("LegacySlsFailureCleanup").warn(
                        "legacy SLS failure cleanup operation failed",
                        Some(&fields(serde_json::json!({ "operation": name, "error": code, "attempts": attempt + 1 }))),
                    );
                    return false;
                }
            }
        }
    }

    fn log_result(&self, result: &LegacySlsFailedLogCleanupResult) {
        if !result.renamed && result.deleted == 0 && result.skipped == 0 && result.errors == 0 {
            return;
        }
        logger("LegacySlsFailureCleanup").info(
            "legacy SLS failure cleanup complete",
            Some(&fields(serde_json::json!({
                "renamed": result.renamed,
                "deleted": result.deleted,
                "skipped": result.skipped,
                "errors": result.errors,
                "logicalBytes": result.logical_bytes,
            }))),
        );
    }
}

pub fn is_legacy_jsonl_name(name: &str) -> bool {
    !name.starts_with('.') && name.len() > ".jsonl".len() && name.ends_with(".jsonl")
}

fn is_transient(code: &str) -> bool {
    matches!(code, "EPERM" | "EBUSY" | "EACCES")
}

fn io_code(err: &std::io::Error) -> String {
    match err.raw_os_error() {
        Some(1) => "EPERM".into(),
        Some(2) => "ENOENT".into(),
        Some(13) => "EACCES".into(),
        Some(16) => "EBUSY".into(),
        _ => match err.kind() {
            std::io::ErrorKind::NotFound => "ENOENT".into(),
            std::io::ErrorKind::PermissionDenied => "EACCES".into(),
            _ => err.to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jsonl_names_exclude_hidden_and_short_files() {
        assert!(is_legacy_jsonl_name("failed.jsonl"));
        assert!(!is_legacy_jsonl_name(".hidden.jsonl"));
        assert!(!is_legacy_jsonl_name(".jsonl"));
        assert!(!is_legacy_jsonl_name("notes.txt"));
    }

    #[tokio::test]
    async fn renames_the_legacy_directory_and_deletes_jsonl_files() {
        let dir = tempfile::tempdir().unwrap();
        let legacy = dir.path().join("sls-failed-logs");
        std::fs::create_dir(&legacy).unwrap();
        std::fs::write(legacy.join("one.jsonl"), b"abcdef").unwrap();
        std::fs::write(legacy.join("notes.txt"), b"skip").unwrap();
        let service = LegacySlsFailedLogCleanupService::with_delays(dir.path(), 0, 0, vec![]);
        let result = service.run_cleanup().await;
        assert!(result.renamed);
        assert_eq!(result.deleted, 1);
        assert_eq!(result.logical_bytes, 6);
        assert_eq!(result.skipped, 1);
        let pending = dir.path().join("sls-failed-logs.delete-pending");
        assert!(pending.join("notes.txt").exists());
        assert!(!pending.join("one.jsonl").exists());
    }
}
