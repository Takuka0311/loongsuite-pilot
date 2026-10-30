//! Shared serialization for append, sealing and retention of local JSONL files.
use std::collections::HashMap;
use std::future::Future;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use tokio::io::AsyncWriteExt;

pub const DEFAULT_JSONL_SEGMENT_BYTES: u64 = 100 * 1024 * 1024;
pub const LOCAL_LOG_EMERGENCY_FILE_BYTES: u64 = 3 * 1024 * 1024 * 1024;
type FileLocks = Mutex<HashMap<PathBuf, Weak<tokio::sync::Mutex<()>>>>;
static WRITES: OnceLock<FileLocks> = OnceLock::new();

fn lock_key(path: &Path) -> PathBuf {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(path)
    };
    let mut key = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                key.pop();
            }
            part => key.push(part.as_os_str()),
        }
    }
    key
}
pub async fn with_jsonl_file_lock<T, F, Fut>(path: &Path, action: F) -> T
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = T>,
{
    let owner = {
        let mut writes = WRITES
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .expect("JSONL file locks");
        writes.retain(|_, owner| owner.strong_count() > 0);
        let key = lock_key(path);
        match writes.get(&key).and_then(Weak::upgrade) {
            Some(owner) => owner,
            None => {
                let owner = Arc::new(tokio::sync::Mutex::new(()));
                writes.insert(key, Arc::downgrade(&owner));
                owner
            }
        }
    };
    let _guard = owner.lock().await;
    action().await
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputSegment {
    pub stream: String,
    pub date: String,
}
pub fn parse_output_segment(file: &str) -> Option<OutputSegment> {
    static PATTERN: OnceLock<regex::Regex> = OnceLock::new();
    let pattern = PATTERN.get_or_init(|| {
        regex::Regex::new(r"^(.+)\.part-([0-9a-f]{32})-(\d{4}-\d{2}-\d{2}|all)\.jsonl$")
            .expect("output segment")
    });
    let captures = pattern.captures(file)?;
    Some(OutputSegment {
        stream: captures[1].into(),
        date: captures[3].into(),
    })
}
fn sealed_path(file: &Path) -> std::io::Result<PathBuf> {
    static PATTERN: OnceLock<regex::Regex> = OnceLock::new();
    let pattern = PATTERN.get_or_init(|| {
        regex::Regex::new(r"^(.*)-(\d{4}-\d{2}-\d{2}|all)\.jsonl$").expect("active output")
    });
    let name = file
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    let captures = pattern.captures(name).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Unsupported JSONL output filename",
        )
    })?;
    Ok(file.with_file_name(format!(
        "{}.part-{}-{}.jsonl",
        &captures[1],
        uuid::Uuid::new_v4().simple(),
        &captures[2]
    )))
}
pub async fn append_segmented_jsonl(
    file: &Path,
    line: &str,
    max_file_bytes: u64,
    failed_data: bool,
) -> std::io::Result<()> {
    with_jsonl_file_lock(file, || async {
        if let Some(parent) = file.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let mut size = match tokio::fs::symlink_metadata(file).await {
            Ok(metadata) => {
                if !metadata.is_file() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "JSONL output is not a regular file",
                    ));
                }
                #[cfg(windows)]
                {
                    use std::os::windows::fs::MetadataExt;
                    if metadata.file_attributes() & 0x400 != 0 {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            "JSONL output is a reparse point",
                        ));
                    }
                }
                metadata.len()
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => return Err(error),
        };
        let bytes = (line.len() as u64).saturating_add(1);
        let limit = max_file_bytes.max(1);
        if size > LOCAL_LOG_EMERGENCY_FILE_BYTES {
            tokio::fs::remove_file(file).await?;
            crate::utils::create_logger("SegmentedJsonlWriter").warn(
                "Deleted oversized local JSONL file",
                Some(
                    &serde_json::json!({"bytes":size,"discardedFailedData":failed_data})
                        .as_object()
                        .cloned()
                        .unwrap_or_default(),
                ),
            );
            size = 0;
        }
        if size > 0 && size.saturating_add(bytes) > limit {
            tokio::fs::rename(file, sealed_path(file)?).await?;
        }
        if bytes > limit {
            crate::utils::create_logger("SegmentedJsonlWriter").warn(
                "JSONL record exceeds file size limit",
                Some(
                    &serde_json::json!({"bytes":bytes,"maxFileBytes":limit})
                        .as_object()
                        .cloned()
                        .unwrap_or_default(),
                ),
            );
        }
        let mut options = tokio::fs::OpenOptions::new();
        options.create(true).append(true);
        #[cfg(unix)]
        {
            options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
        }
        #[cfg(windows)]
        {
            options.custom_flags(0x0020_0000);
        }
        let mut output = options.open(file).await?;
        #[cfg(windows)]
        {
            use std::os::windows::fs::MetadataExt;
            if output.metadata().await?.file_attributes() & 0x400 != 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "JSONL output is a reparse point",
                ));
            }
        }
        let mut data = Vec::with_capacity(line.len().saturating_add(1));
        data.extend_from_slice(line.as_bytes());
        data.push(b'\n');
        output.write_all(&data).await?;
        output.flush().await
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    #[tokio::test]
    async fn concurrent_producers_seal_complete_utf8_records_without_missing_or_duplicate_ids() {
        let root = tempfile::tempdir().unwrap();
        let file = root.path().join("copilot-2026-10-30.jsonl");
        let mut producers = tokio::task::JoinSet::new();
        for producer in 0..16 {
            let file = file.clone();
            producers.spawn(async move {
                for index in 0..64 {
                    let line =
                        serde_json::json!({"id":format!("{producer}:{index}"),"text":"完整记录🚀"})
                            .to_string();
                    append_segmented_jsonl(&file, &line, 1024, false)
                        .await
                        .unwrap();
                }
            });
        }
        while let Some(result) = producers.join_next().await {
            result.unwrap();
        }
        let mut ids = HashSet::new();
        let mut count = 0;
        for entry in std::fs::read_dir(root.path()).unwrap() {
            let path = entry.unwrap().path();
            assert!(std::fs::metadata(&path).unwrap().len() <= 1024);
            if path != file {
                let segment =
                    parse_output_segment(path.file_name().unwrap().to_str().unwrap()).unwrap();
                assert_eq!(segment.stream, "copilot");
                assert_eq!(segment.date, "2026-10-30");
            }
            for line in std::fs::read_to_string(path).unwrap().lines() {
                let value: serde_json::Value = serde_json::from_str(line).unwrap();
                assert_eq!(value["text"], "完整记录🚀");
                assert!(ids.insert(value["id"].as_str().unwrap().to_owned()));
                count += 1;
            }
        }
        assert_eq!(count, 1024);
    }
    #[tokio::test]
    async fn oversized_event_is_kept_whole_and_sparse_emergency_file_is_reclaimed() {
        let root = tempfile::tempdir().unwrap();
        let file = root.path().join("copilot-all.jsonl");
        let line = serde_json::json!({"body":"x".repeat(4096)}).to_string();
        append_segmented_jsonl(&file, &line, 1024, false)
            .await
            .unwrap();
        assert_eq!(std::fs::read_to_string(&file).unwrap(), format!("{line}\n"));
        append_segmented_jsonl(&file, "{}", 1024, false)
            .await
            .unwrap();
        assert_eq!(std::fs::read_to_string(&file).unwrap(), "{}\n");
        #[cfg(unix)]
        {
            let sparse = std::fs::OpenOptions::new().write(true).open(&file).unwrap();
            sparse.set_len(LOCAL_LOG_EMERGENCY_FILE_BYTES + 1).unwrap();
            drop(sparse);
            append_segmented_jsonl(&file, r#"{"new":true}"#, 1024, true)
                .await
                .unwrap();
            assert_eq!(std::fs::read_to_string(file).unwrap(), "{\"new\":true}\n");
        }
    }
    #[cfg(unix)]
    #[tokio::test]
    async fn symbolic_link_outputs_never_modify_the_target() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("user.txt");
        std::fs::write(&target, "preserve").unwrap();
        let file = root.path().join("copilot-all.jsonl");
        std::os::unix::fs::symlink(&target, &file).unwrap();
        assert!(append_segmented_jsonl(&file, "{}", 1024, false)
            .await
            .is_err());
        assert_eq!(std::fs::read_to_string(target).unwrap(), "preserve");
    }
}
