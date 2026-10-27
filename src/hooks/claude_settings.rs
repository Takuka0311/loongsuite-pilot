//! Serialize cooperating Pilot writers of one Claude settings directory.

use crate::utils::acquire_single_instance_lock;
use std::path::Path;
use std::time::{Duration, Instant};

pub async fn with_claude_settings_lock<T, Fut>(
    settings_path: &str,
    update: impl FnOnce() -> Fut,
    timeout_ms: u64,
) -> Result<T, String>
where
    Fut: std::future::Future<Output = T>,
{
    let parent = Path::new(settings_path)
        .parent()
        .map(|path| path.to_string_lossy().into_owned())
        .unwrap_or_else(|| ".".to_string());
    tokio::fs::create_dir_all(&parent)
        .await
        .map_err(|err| err.to_string())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o700));
    }
    let dir = tokio::fs::canonicalize(&parent)
        .await
        .map_err(|err| err.to_string())?;
    let lock_path = dir.join(".loongsuite-pilot-inject.lock");
    let lock_path = lock_path.to_string_lossy().into_owned();
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        let result = acquire_single_instance_lock(&lock_path, None);
        if let Some(err) = result.error {
            return Err(err.to_string());
        }
        if let Some(lock) = result.lock {
            let value = update().await;
            lock.release();
            return Ok(value);
        }
        if Instant::now() >= deadline {
            return Err("Cannot acquire configuration lock (busy)".into());
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

pub fn quote_claude_hook_path(script: &str) -> String {
    let safe = script
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '.' | '/' | ':' | '-'));
    if safe {
        script.to_string()
    } else {
        format!("'{}'", script.replace('\'', r#"'"'"'"#))
    }
}

/// Exact old spellings of this installed path.
pub fn legacy_claude_hook_paths(script: &str) -> Vec<String> {
    let canonical = quote_claude_hook_path(script);
    let mut out = Vec::new();
    for value in [
        script.to_string(),
        format!("\"{script}\""),
        format!("'{script}'"),
    ] {
        if value != canonical && !out.contains(&value) {
            out.push(value);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quotes_paths_with_spaces_and_apostrophes() {
        assert_eq!(quote_claude_hook_path("/opt/hook.sh"), "/opt/hook.sh");
        assert_eq!(
            quote_claude_hook_path("/opt/my hook.sh"),
            "'/opt/my hook.sh'"
        );
        assert_eq!(
            quote_claude_hook_path("/opt/o'brien.sh"),
            "'/opt/o'\"'\"'brien.sh'"
        );
    }

    #[test]
    fn legacy_spellings_exclude_the_canonical_form() {
        let legacy = legacy_claude_hook_paths("/opt/my hook.sh");
        assert!(legacy.contains(&"/opt/my hook.sh".to_string()));
        assert!(legacy.contains(&"\"/opt/my hook.sh\"".to_string()));
        assert!(!legacy.contains(&quote_claude_hook_path("/opt/my hook.sh")));
    }
}
