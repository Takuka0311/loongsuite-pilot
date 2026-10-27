//! Directory watches. On Linux this uses `inotify` via libc (already linked by
//! libstd). A watch that cannot be created is logged and skipped, matching the
//! TypeScript fallback to polling.

use std::collections::{HashMap, HashSet};
use std::os::fd::RawFd;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use crate::utils::create_logger;
use serde_json::{json, Map, Value};

fn logger() -> crate::utils::BoundLogger {
    create_logger("FileWatcher")
}

struct WatchState {
    dirty: Vec<String>,
    dirty_set: HashSet<String>,
    dirs: Vec<String>,
    stop: bool,
}

pub struct FileWatcher {
    state: Arc<Mutex<WatchState>>,
    thread: Mutex<Option<JoinHandle<()>>>,
    fd: Mutex<Option<RawFd>>,
}

impl FileWatcher {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(WatchState {
                dirty: Vec::new(),
                dirty_set: HashSet::new(),
                dirs: Vec::new(),
                stop: false,
            })),
            thread: Mutex::new(None),
            fd: Mutex::new(None),
        }
    }

    pub fn watch(&self, dirs: &[String]) {
        let mut unique = Vec::new();
        for dir in dirs {
            if !unique.iter().any(|existing: &String| existing == dir) {
                unique.push(dir.clone());
            }
        }
        for dir in unique {
            if self
                .state
                .lock()
                .unwrap()
                .dirs
                .iter()
                .any(|existing| existing == &dir)
            {
                continue;
            }
            if !Path::new(&dir).is_dir() {
                logger().warn(
                    "failed to create fs.watch, degrading to polling",
                    Some(&meta(&[
                        ("dir", json!(dir)),
                        ("error", json!("not a directory")),
                    ])),
                );
                continue;
            }
            self.state.lock().unwrap().dirs.push(dir);
        }
        self.restart_thread();
    }

    pub fn dirty_files(&self) -> Vec<String> {
        let mut state = self.state.lock().unwrap();
        let files = std::mem::take(&mut state.dirty);
        state.dirty_set.clear();
        files
    }

    pub fn add_dirty(&self, file_path: &str) {
        let mut state = self.state.lock().unwrap();
        if state.dirty_set.insert(file_path.to_string()) {
            state.dirty.push(file_path.to_string());
        }
    }

    pub fn rewatch(&self) {
        self.restart_thread();
    }

    pub fn close(&self) {
        self.state.lock().unwrap().stop = true;
        self.state.lock().unwrap().dirs.clear();
        self.state.lock().unwrap().dirty.clear();
        self.state.lock().unwrap().dirty_set.clear();
        let fd = self.fd.lock().unwrap().take();
        if let Some(fd) = fd {
            unsafe { sys_close(fd) };
        }
        let thread = self.thread.lock().unwrap().take();
        if let Some(thread) = thread {
            let _ = thread.join();
        }
        self.state.lock().unwrap().stop = false;
    }

    fn restart_thread(&self) {
        let fd = self.fd.lock().unwrap().take();
        if let Some(fd) = fd {
            unsafe { sys_close(fd) };
        }
        let thread = self.thread.lock().unwrap().take();
        if let Some(thread) = thread {
            self.state.lock().unwrap().stop = true;
            let _ = thread.join();
            self.state.lock().unwrap().stop = false;
        }
        let dirs = self.state.lock().unwrap().dirs.clone();
        if dirs.is_empty() {
            return;
        }
        // IN_CLOEXEC | IN_NONBLOCK. A blocking read cannot be interrupted by close()
        // from the thread that joins this loop, so the watch thread polls.
        let fd = unsafe { sys_inotify_init1(0x80000 | 0x800) };
        if fd < 0 {
            logger().warn(
                "failed to create fs.watch, degrading to polling",
                Some(&meta(&[("error", json!("inotify_init"))])),
            );
            return;
        }
        let mut wd_to_dir = HashMap::new();
        for dir in &dirs {
            let c_dir = match std::ffi::CString::new(dir.as_str()) {
                Ok(value) => value,
                Err(_) => continue,
            };
            let wd = unsafe { sys_inotify_add_watch(fd, c_dir.as_ptr(), 0x0000_00ff) };
            if wd >= 0 {
                wd_to_dir.insert(wd, dir.clone());
            } else {
                logger().warn(
                    "failed to create fs.watch, degrading to polling",
                    Some(&meta(&[
                        ("dir", json!(dir)),
                        ("error", json!("inotify_add_watch")),
                    ])),
                );
            }
        }
        *self.fd.lock().unwrap() = Some(fd);
        let state = Arc::clone(&self.state);
        let thread = std::thread::spawn(move || read_loop(fd, wd_to_dir, state));
        *self.thread.lock().unwrap() = Some(thread);
    }
}

impl Default for FileWatcher {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for FileWatcher {
    fn drop(&mut self) {
        self.close();
    }
}

fn read_loop(fd: RawFd, wd_to_dir: HashMap<i32, String>, state: Arc<Mutex<WatchState>>) {
    let mut buf = [0u8; 4096];
    loop {
        if state.lock().unwrap().stop {
            break;
        }
        let mut poll_fd = PollFd {
            fd,
            events: 1,
            revents: 0,
        };
        let ready = unsafe { sys_poll(&mut poll_fd, 1, 50) };
        if state.lock().unwrap().stop {
            break;
        }
        if ready < 0 {
            let err = errno();
            if err == 4 {
                continue;
            }
            break;
        }
        if ready == 0 {
            continue;
        }
        let read = unsafe { sys_read(fd, buf.as_mut_ptr(), buf.len()) };
        if read < 0 {
            let err = errno();
            if err == 11 || err == 4 {
                continue;
            }
            break;
        }
        if read == 0 {
            break;
        }
        let mut offset = 0usize;
        while offset + 16 <= read as usize {
            let wd = i32::from_ne_bytes(buf[offset..offset + 4].try_into().unwrap());
            let len =
                u32::from_ne_bytes(buf[offset + 12..offset + 16].try_into().unwrap()) as usize;
            let name_start = offset + 16;
            let name_end = (name_start + len).min(read as usize);
            if name_end > name_start {
                let name = &buf[name_start..name_end];
                let name = name.split(|byte| *byte == 0).next().unwrap_or(name);
                if let Ok(name) = std::str::from_utf8(name) {
                    if !name.is_empty() {
                        if let Some(dir) = wd_to_dir.get(&wd) {
                            let path = Path::new(dir).join(name).to_string_lossy().to_string();
                            let mut state = state.lock().unwrap();
                            if state.dirty_set.insert(path.clone()) {
                                state.dirty.push(path);
                            }
                        }
                    }
                }
            }
            offset = name_end;
        }
    }
}

pub fn extract_parent_dirs(patterns: &[String]) -> Vec<String> {
    let mut dirs = Vec::new();
    for pattern in patterns {
        let parent = Path::new(pattern)
            .parent()
            .map(|path| path.to_string_lossy().to_string())
            .unwrap_or_else(|| ".".into());
        if !dirs.iter().any(|existing: &String| existing == &parent) {
            dirs.push(parent);
        }
    }
    dirs
}

fn meta(pairs: &[(&str, Value)]) -> Map<String, Value> {
    pairs
        .iter()
        .map(|(key, value)| ((*key).to_string(), value.clone()))
        .collect()
}

#[cfg(unix)]
unsafe fn sys_inotify_init1(flags: i32) -> i32 {
    unsafe extern "C" {
        fn inotify_init1(flags: i32) -> i32;
    }
    unsafe { inotify_init1(flags) }
}

#[cfg(unix)]
unsafe fn sys_inotify_add_watch(fd: i32, path: *const i8, mask: u32) -> i32 {
    unsafe extern "C" {
        fn inotify_add_watch(fd: i32, pathname: *const i8, mask: u32) -> i32;
    }
    unsafe { inotify_add_watch(fd, path, mask) }
}

#[repr(C)]
struct PollFd {
    fd: i32,
    events: i16,
    revents: i16,
}

#[cfg(unix)]
unsafe fn sys_poll(fds: *mut PollFd, nfds: u64, timeout_ms: i32) -> i32 {
    unsafe extern "C" {
        fn poll(fds: *mut PollFd, nfds: u64, timeout: i32) -> i32;
    }
    unsafe { poll(fds, nfds, timeout_ms) }
}

#[cfg(not(unix))]
unsafe fn sys_poll(_fds: *mut PollFd, _nfds: u64, _timeout_ms: i32) -> i32 {
    -1
}

#[cfg(unix)]
fn errno() -> i32 {
    unsafe extern "C" {
        fn __errno_location() -> *mut i32;
    }
    unsafe { *__errno_location() }
}

#[cfg(not(unix))]
fn errno() -> i32 {
    0
}

#[cfg(unix)]
unsafe fn sys_read(fd: i32, buf: *mut u8, len: usize) -> isize {
    unsafe extern "C" {
        fn read(fd: i32, buf: *mut u8, count: usize) -> isize;
    }
    unsafe { read(fd, buf, len) }
}

#[cfg(unix)]
unsafe fn sys_close(fd: i32) -> i32 {
    unsafe extern "C" {
        fn close(fd: i32) -> i32;
    }
    unsafe { close(fd) }
}

#[cfg(not(unix))]
unsafe fn sys_inotify_init1(_flags: i32) -> i32 {
    -1
}
#[cfg(not(unix))]
unsafe fn sys_inotify_add_watch(_fd: i32, _path: *const i8, _mask: u32) -> i32 {
    -1
}
#[cfg(not(unix))]
unsafe fn sys_read(_fd: i32, _buf: *mut u8, _len: usize) -> isize {
    -1
}
#[cfg(not(unix))]
unsafe fn sys_close(_fd: i32) -> i32 {
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dirty_set_dedup_and_parent_dirs() {
        let watcher = FileWatcher::new();
        watcher.add_dirty("/tmp/a.log");
        watcher.add_dirty("/tmp/b.log");
        assert_eq!(watcher.dirty_files().len(), 2);
        assert!(watcher.dirty_files().is_empty());
        watcher.add_dirty("/some/file.log");
        assert_eq!(watcher.dirty_files(), vec!["/some/file.log".to_string()]);
        watcher.watch(&["/nonexistent/dir/that/does/not/exist".into()]);
        assert!(watcher.dirty_files().is_empty());
        watcher.close();
        assert!(watcher.dirty_files().is_empty());
        watcher.rewatch();
        let mut dirs = extract_parent_dirs(&[
            "/var/log/*.log".into(),
            "/var/log/nginx/*.log".into(),
            "/var/log/*.txt".into(),
        ]);
        dirs.sort();
        assert_eq!(
            dirs,
            vec!["/var/log".to_string(), "/var/log/nginx".to_string()]
        );
        assert!(extract_parent_dirs(&[]).is_empty());
    }

    #[test]
    fn marks_files_when_the_directory_changes() {
        let dir = std::env::temp_dir().join(format!("file-watcher-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let watcher = FileWatcher::new();
        watcher.watch(&[
            dir.to_string_lossy().to_string(),
            dir.to_string_lossy().to_string(),
        ]);
        std::fs::write(dir.join("test.log"), "hello\n").unwrap();
        std::thread::sleep(std::time::Duration::from_millis(200));
        let dirty = watcher.dirty_files();
        assert!(
            dirty.iter().any(|file| file.contains("test.log")),
            "{dirty:?}"
        );
        watcher.add_dirty("/tmp/pending.log");
        watcher.rewatch();
        assert_eq!(watcher.dirty_files(), vec!["/tmp/pending.log".to_string()]);
        std::fs::write(dir.join("after-rewatch.log"), "data\n").unwrap();
        std::thread::sleep(std::time::Duration::from_millis(200));
        let dirty = watcher.dirty_files();
        assert!(
            dirty.iter().any(|file| file.contains("after-rewatch.log")),
            "{dirty:?}"
        );
        watcher.close();
        let _ = std::fs::remove_dir_all(&dir);
    }
}
