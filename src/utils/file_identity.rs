//! Identity from the exact open file used for reading, including stable Windows
//! handle metadata. MetadataExt file_index remains nightly-only in Rust 1.99.1.
use std::fs::File;
use std::io;
#[derive(Clone, Debug)]
pub(crate) struct OpenedFileIdentity {
    pub dev: u64,
    pub ino: u64,
    pub birthtime_ms: f64,
    pub size: u64,
}
pub(crate) fn identity_from_open(file: &File) -> io::Result<OpenedFileIdentity> {
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "JSONL source is not a regular file",
        ));
    }
    #[cfg(unix)]
    let (dev, ino) = {
        use std::os::unix::fs::MetadataExt;
        (metadata.dev(), metadata.ino())
    };
    #[cfg(windows)]
    let (dev, ino) = {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Storage::FileSystem::{
            GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION,
        };
        let mut info = std::mem::MaybeUninit::<BY_HANDLE_FILE_INFORMATION>::zeroed();
        // SAFETY: the File owns a valid handle and the output has the API's exact layout.
        let success =
            unsafe { GetFileInformationByHandle(file.as_raw_handle(), info.as_mut_ptr()) };
        if success == 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: a successful call initializes the entire structure.
        let info = unsafe { info.assume_init() };
        (
            u64::from(info.dwVolumeSerialNumber),
            (u64::from(info.nFileIndexHigh) << 32) | u64::from(info.nFileIndexLow),
        )
    };
    let birthtime_ms = metadata
        .created()
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|duration| duration.as_millis() as f64)
        .unwrap_or(0.0);
    Ok(OpenedFileIdentity {
        dev,
        ino,
        birthtime_ms,
        size: metadata.len(),
    })
}
