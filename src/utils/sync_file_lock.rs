//! Shared bounded empty-file lock; interoperable with existing Agent host scripts.
use std::fs::{self, OpenOptions};
use std::io;
use std::path::Path;

pub fn with_file_lock<T>(
    lock_file: &Path,
    wait_ms: u64,
    stale_ms: u64,
    action: impl FnOnce() -> T,
) -> Option<T> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(wait_ms);
    if let Some(parent) = lock_file.parent() {
        let _ = fs::create_dir_all(parent);
    }
    let mut acquired = false;
    let result = loop {
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(lock_file)
        {
            Ok(file) => {
                drop(file);
                acquired = true;
                break Some(action());
            }
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
                if let Ok(meta) = fs::metadata(lock_file) {
                    if let Ok(modified) = meta.modified() {
                        if modified
                            .elapsed()
                            .map(|age| age.as_millis() as u64 > stale_ms)
                            .unwrap_or(false)
                        {
                            let _ = fs::remove_file(lock_file);
                            continue;
                        }
                    }
                }
                if std::time::Instant::now() >= deadline {
                    break None;
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            Err(_) => break None,
        }
    };
    if acquired {
        let _ = fs::remove_file(lock_file);
    }
    result
}
