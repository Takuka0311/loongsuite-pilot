//! Independent, body-free proof that a source appeared during a live collector.
//! A historical baseline floor is an initial exclusion boundary, never an ACK.
//! Rebuilding at that floor does not supply firstOwned or change copied-history rules.
use crate::utils::file_identity::{identity_from_open, OpenedFileIdentity};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

const ANCHOR_LIMIT: usize = 4096;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct BaselineFloor {
    pub offset: u64,
    pub copied: bool,
}
pub(super) struct BaselineWitness {
    marker: Ownership,
    size: u64,
}
impl BaselineWitness {
    pub(super) fn size(&self) -> u64 {
        self.size
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Ownership {
    schema: u8,
    device: u64,
    inode: u64,
    birthtime_bits: u64,
    anchor_length: usize,
    anchor_sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    baseline_floor: Option<BaselineFloor>,
}
fn marker_path(wakeup: &Path, source: &Path) -> PathBuf {
    let key = hex::encode(Sha256::digest(source.as_os_str().as_encoded_bytes()));
    wakeup.join("source-owners").join(format!("{key}.json"))
}
fn open_regular(path: &Path) -> io::Result<File> {
    if fs::symlink_metadata(path)?.file_type().is_symlink() {
        return Err(io::Error::other("source ownership refuses symlinks"));
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        options.custom_flags(windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT);
    }
    let file = options.open(path)?;
    identity_from_open(&file)?;
    Ok(file)
}
fn read_marker(path: &Path) -> io::Result<Option<Ownership>> {
    let file = match open_regular(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let mut bytes = Vec::new();
    file.take(4097).read_to_end(&mut bytes)?;
    if bytes.len() > 4096 {
        return Err(io::Error::other("source ownership marker exceeds budget"));
    }
    let marker: Ownership = serde_json::from_slice(&bytes)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    if marker.schema != 1
        || (marker.anchor_length == 0
            && marker.baseline_floor.is_none_or(|floor| floor.offset != 0))
        || marker.anchor_length > ANCHOR_LIMIT
        || marker.anchor_sha256.len() != 64
        || !marker
            .anchor_sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(io::Error::other("invalid source ownership marker"));
    }
    Ok(Some(marker))
}
fn same_identity(a: &OpenedFileIdentity, b: &OpenedFileIdentity) -> bool {
    a.dev == b.dev && a.ino == b.ino && a.birthtime_ms.to_bits() == b.birthtime_ms.to_bits()
}
fn matches_source(marker: &Ownership, source: &Path) -> io::Result<bool> {
    let mut file = match open_regular(source) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    let before = identity_from_open(&file)?;
    if before.dev != marker.device
        || before.ino != marker.inode
        || before.birthtime_ms.to_bits() != marker.birthtime_bits
        || before.size < marker.anchor_length as u64
        || marker
            .baseline_floor
            .is_some_and(|floor| before.size < floor.offset)
    {
        return Ok(false);
    }
    let mut anchor = vec![0; marker.anchor_length];
    file.read_exact(&mut anchor)?;
    let after = identity_from_open(&file)?;
    Ok(same_identity(&before, &after)
        && hex::encode(Sha256::digest(&anchor)) == marker.anchor_sha256)
}
#[cfg(unix)]
fn sync_dir(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}
#[cfg(not(unix))]
fn sync_dir(_path: &Path) -> io::Result<()> {
    // File::sync_all precedes NamedTempFile's platform atomic replacement.
    Ok(())
}
fn private_directory(path: &Path) -> io::Result<()> {
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(path)?;
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(io::Error::other("invalid source ownership directory"));
    }
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        sync_dir(parent)?;
    }
    Ok(())
}
fn capture_source(source: &Path) -> io::Result<BaselineWitness> {
    let mut file = open_regular(source)?;
    let before = identity_from_open(&file)?;
    let length = usize::try_from(before.size.min(ANCHOR_LIMIT as u64)).map_err(io::Error::other)?;
    let mut anchor = vec![0; length];
    file.read_exact(&mut anchor)?;
    let after = identity_from_open(&file)?;
    if !same_identity(&before, &after) || after.size < before.size {
        return Err(io::Error::other("rollout changed while capturing source"));
    }
    Ok(BaselineWitness {
        marker: Ownership {
            schema: 1,
            device: before.dev,
            inode: before.ino,
            birthtime_bits: before.birthtime_ms.to_bits(),
            anchor_length: length,
            anchor_sha256: hex::encode(Sha256::digest(&anchor)),
            baseline_floor: None,
        },
        size: before.size,
    })
}
fn persist_marker(wakeup: &Path, source: &Path, marker: &Ownership) -> io::Result<()> {
    private_directory(wakeup)?;
    let directory = wakeup.join("source-owners");
    private_directory(&directory)?;
    let mut temporary = tempfile::NamedTempFile::new_in(&directory)?;
    serde_json::to_writer(&mut temporary, marker).map_err(io::Error::other)?;
    temporary.write_all(b"\n")?;
    temporary.as_file().sync_all()?;
    temporary
        .persist(marker_path(wakeup, source))
        .map_err(|error| error.error)?;
    sync_dir(&directory)?;
    Ok(())
}
fn remember_blocking(wakeup: &Path, source: &Path) -> io::Result<()> {
    if let Some(marker) = read_marker(&marker_path(wakeup, source))? {
        if matches_source(&marker, source)? {
            return Ok(());
        }
    }
    let witness = capture_source(source)?;
    // A new empty live file cannot emit and needs no zero-length live proof.
    if witness.size == 0 {
        return Ok(());
    }
    persist_marker(wakeup, source, &witness.marker)
}
fn remember_baseline_blocking(
    wakeup: &Path,
    source: &Path,
    mut witness: BaselineWitness,
    floor: BaselineFloor,
) -> io::Result<()> {
    if floor.offset > witness.size || !matches_source(&witness.marker, source)? {
        return Err(io::Error::other(
            "source changed before baseline floor was persisted",
        ));
    }
    if let Some(existing) = read_marker(&marker_path(wakeup, source))? {
        if matches_source(&existing, source)? {
            // A baseline exclusion boundary is immutable for this file identity.
            // Never replace a live claim or advance a historical floor to a tail.
            return if existing.baseline_floor == Some(floor) {
                Ok(())
            } else {
                Err(io::Error::other(
                    "refusing to replace the initial source boundary",
                ))
            };
        }
    }
    witness.marker.baseline_floor = Some(floor);
    persist_marker(wakeup, source, &witness.marker)
}
pub(super) async fn capture_baseline(source: &str) -> io::Result<BaselineWitness> {
    let source = PathBuf::from(source);
    tokio::task::spawn_blocking(move || capture_source(&source))
        .await
        .map_err(io::Error::other)?
}
pub(super) async fn remember_baseline(
    wakeup: &str,
    source: &str,
    witness: BaselineWitness,
    floor: BaselineFloor,
) -> io::Result<()> {
    let (wakeup, source) = (PathBuf::from(wakeup), PathBuf::from(source));
    tokio::task::spawn_blocking(move || {
        remember_baseline_blocking(&wakeup, &source, witness, floor)
    })
    .await
    .map_err(io::Error::other)?
}
pub(super) async fn baseline_floor(
    wakeup: &str,
    source: &str,
) -> io::Result<Option<BaselineFloor>> {
    let (wakeup, source) = (PathBuf::from(wakeup), PathBuf::from(source));
    tokio::task::spawn_blocking(move || {
        let Some(marker) = read_marker(&marker_path(&wakeup, &source))? else {
            return Ok(None);
        };
        if matches_source(&marker, &source)? {
            Ok(marker.baseline_floor)
        } else {
            Ok(None)
        }
    })
    .await
    .map_err(io::Error::other)?
}
pub(super) async fn remember(wakeup: &str, source: &str) -> io::Result<()> {
    let (wakeup, source) = (PathBuf::from(wakeup), PathBuf::from(source));
    tokio::task::spawn_blocking(move || remember_blocking(&wakeup, &source))
        .await
        .map_err(io::Error::other)?
}
pub(super) async fn was_live(wakeup: &str, source: &str) -> io::Result<bool> {
    let (wakeup, source) = (PathBuf::from(wakeup), PathBuf::from(source));
    tokio::task::spawn_blocking(move || {
        let Some(marker) = read_marker(&marker_path(&wakeup, &source))? else {
            return Ok(false);
        };
        Ok(marker.baseline_floor.is_none() && matches_source(&marker, &source)?)
    })
    .await
    .map_err(io::Error::other)?
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn proof_survives_append_without_body_or_acknowledged_progress() {
        let root = tempfile::tempdir().unwrap();
        let (wakeup, source) = (
            root.path().join("wakeups"),
            root.path().join("rollout.jsonl"),
        );
        fs::write(&source, b"synthetic-private-prompt\n").unwrap();
        remember_blocking(&wakeup, &source).unwrap();
        let path = marker_path(&wakeup, &source);
        let body = fs::read_to_string(&path).unwrap();
        assert!(!body.contains("synthetic-private-prompt"));
        let value: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(value.as_object().unwrap().len(), 6);
        for key in ["lastOffset", "scanOffset", "firstOwnedTurnId"] {
            assert!(value.get(key).is_none());
        }
        let marker = read_marker(&path).unwrap().unwrap();
        OpenOptions::new()
            .append(true)
            .open(&source)
            .unwrap()
            .write_all(b"new-live-tail\n")
            .unwrap();
        assert!(matches_source(&marker, &source).unwrap());
    }
    #[test]
    fn replacement_and_in_place_rewrite_cannot_reuse_proof() {
        let root = tempfile::tempdir().unwrap();
        let (wakeup, source) = (
            root.path().join("wakeups"),
            root.path().join("rollout.jsonl"),
        );
        fs::write(&source, b"new owned rollout\n").unwrap();
        remember_blocking(&wakeup, &source).unwrap();
        let marker = read_marker(&marker_path(&wakeup, &source))
            .unwrap()
            .unwrap();
        fs::write(&source, b"copied old history\n").unwrap();
        assert!(!matches_source(&marker, &source).unwrap());
        fs::rename(&source, root.path().join("replaced.jsonl")).unwrap();
        fs::write(&source, b"new owned rollout\n").unwrap();
        assert!(!matches_source(&marker, &source).unwrap());
    }
    #[test]
    fn empty_source_has_no_proof_and_corrupt_marker_fails_closed() {
        let root = tempfile::tempdir().unwrap();
        let (wakeup, source) = (
            root.path().join("wakeups"),
            root.path().join("rollout.jsonl"),
        );
        fs::write(&source, b"").unwrap();
        remember_blocking(&wakeup, &source).unwrap();
        assert!(!marker_path(&wakeup, &source).exists());
        fs::write(&source, b"live\n").unwrap();
        remember_blocking(&wakeup, &source).unwrap();
        fs::write(marker_path(&wakeup, &source), b"{truncated").unwrap();
        assert!(read_marker(&marker_path(&wakeup, &source)).is_err());
        assert!(remember_blocking(&wakeup, &source).is_err());
    }
    #[test]
    fn ownership_write_failure_reaches_the_emission_gate() {
        let root = tempfile::tempdir().unwrap();
        let (wakeup, source) = (
            root.path().join("wakeups"),
            root.path().join("rollout.jsonl"),
        );
        fs::write(&source, b"live\n").unwrap();
        fs::create_dir(&wakeup).unwrap();
        fs::write(wakeup.join("source-owners"), b"not a directory").unwrap();
        assert!(remember_blocking(&wakeup, &source).is_err());
    }
    #[test]
    fn baseline_floor_is_immutable_without_body_or_delivery_cursor() {
        let root = tempfile::tempdir().unwrap();
        let (wakeup, source) = (
            root.path().join("wakeups"),
            root.path().join("rollout.jsonl"),
        );
        fs::write(&source, b"synthetic-private-history\n").unwrap();
        let witness = capture_source(&source).unwrap();
        let floor = BaselineFloor {
            offset: witness.size(),
            copied: false,
        };
        remember_baseline_blocking(&wakeup, &source, witness, floor).unwrap();
        OpenOptions::new()
            .append(true)
            .open(&source)
            .unwrap()
            .write_all(b"new-unaccepted-tail\n")
            .unwrap();
        let marker = read_marker(&marker_path(&wakeup, &source))
            .unwrap()
            .unwrap();
        assert_eq!(marker.baseline_floor, Some(floor));
        assert!(matches_source(&marker, &source).unwrap());
        let serialized = fs::read_to_string(marker_path(&wakeup, &source)).unwrap();
        assert!(!serialized.contains("synthetic-private-history"));
        assert!(!serialized.contains("new-unaccepted-tail"));
        let value: serde_json::Value = serde_json::from_str(&serialized).unwrap();
        assert!(value.get("lastOffset").is_none());
        assert!(value.get("scanOffset").is_none());
        let witness = capture_source(&source).unwrap();
        let advanced = BaselineFloor {
            offset: witness.size(),
            copied: false,
        };
        assert!(remember_baseline_blocking(&wakeup, &source, witness, advanced).is_err());
        assert_eq!(
            read_marker(&marker_path(&wakeup, &source))
                .unwrap()
                .unwrap()
                .baseline_floor,
            Some(floor)
        );
    }
    #[test]
    fn source_changed_after_witness_cannot_acquire_a_historical_floor() {
        let root = tempfile::tempdir().unwrap();
        let (wakeup, source) = (
            root.path().join("wakeups"),
            root.path().join("rollout.jsonl"),
        );
        fs::write(&source, b"original\n").unwrap();
        let witness = capture_source(&source).unwrap();
        let floor = BaselineFloor {
            offset: witness.size(),
            copied: false,
        };
        fs::rename(&source, root.path().join("original.jsonl")).unwrap();
        fs::write(&source, b"original\n").unwrap();
        assert!(remember_baseline_blocking(&wakeup, &source, witness, floor).is_err());
        assert!(!marker_path(&wakeup, &source).exists());
    }
    #[test]
    fn an_initial_empty_file_can_have_an_explicit_zero_exclusion_floor() {
        let root = tempfile::tempdir().unwrap();
        let (wakeup, source) = (
            root.path().join("wakeups"),
            root.path().join("rollout.jsonl"),
        );
        fs::write(&source, b"").unwrap();
        let witness = capture_source(&source).unwrap();
        let floor = BaselineFloor {
            offset: 0,
            copied: false,
        };
        remember_baseline_blocking(&wakeup, &source, witness, floor).unwrap();
        OpenOptions::new()
            .append(true)
            .open(&source)
            .unwrap()
            .write_all(b"new tail\n")
            .unwrap();
        let marker = read_marker(&marker_path(&wakeup, &source))
            .unwrap()
            .unwrap();
        assert_eq!(marker.baseline_floor, Some(floor));
        assert!(matches_source(&marker, &source).unwrap());
    }
}
