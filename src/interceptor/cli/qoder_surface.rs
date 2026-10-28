//! Decide whether a Qoder hook belongs to the desktop app or qodercli.

use crate::interceptor::types::QoderSurface;
use std::path::Path;

#[cfg(test)]
struct WindowsExecOptions {
    pub windows_hide: bool,
    pub timeout_ms: u64,
}

pub fn resolve_qoder_surface() -> Option<QoderSurface> {
    resolve_qoder_surface_with(|key| std::env::var(key).ok(), &ancestor_names())
}

pub fn resolve_qoder_surface_with(
    env: impl Fn(&str) -> Option<String>,
    ancestors: &[String],
) -> Option<QoderSurface> {
    if let Some(surface) = classify_config_dir(env("QODER_CONFIG_DIR").as_deref()) {
        return Some(surface);
    }
    for name in ancestors {
        if let Some(surface) = classify_executable(name) {
            return Some(surface);
        }
    }
    None
}

pub fn classify_executable(raw: &str) -> Option<QoderSurface> {
    let name = normalize_name(raw);
    if name.is_empty() || is_non_qoder_host(&name) {
        return None;
    }
    if name.contains("qodercli") || name.contains("qoder-cli") || name.contains("qoder_cli") {
        return Some(QoderSurface::QoderCli);
    }
    if name.contains("qoderwork") || name.contains("qoder-work") {
        return None;
    }
    if name.contains("qwenwork") || name.contains("qwen-work") {
        return None;
    }
    if name.contains("qoder") {
        return Some(QoderSurface::Qoder);
    }
    None
}

fn classify_config_dir(raw: Option<&str>) -> Option<QoderSurface> {
    let raw = raw?;
    let normalized = raw.replace('\\', "/").to_lowercase();
    if normalized.contains(".qoderwork") {
        return None;
    }
    if normalized.contains(".qoder") {
        return Some(QoderSurface::Qoder);
    }
    None
}

fn is_non_qoder_host(name: &str) -> bool {
    name.contains("node")
        || name.contains("loongsuite-pilot")
        || name.contains("interceptor")
        || name.contains("coding-agent-security")
}

fn normalize_name(raw: &str) -> String {
    Path::new(raw)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(raw)
        .trim_end_matches(".exe")
        .trim_end_matches(".EXE")
        .to_lowercase()
}

fn ancestor_names() -> Vec<String> {
    #[cfg(unix)]
    {
        let mut names = Vec::new();
        let mut pid = std::os::unix::process::parent_id() as i64;
        for _ in 0..12 {
            if pid <= 1 {
                break;
            }
            let Some(info) = read_process(pid) else {
                break;
            };
            names.push(info.name);
            pid = info.ppid;
        }
        names
    }
    #[cfg(not(unix))]
    {
        Vec::new()
    }
}

#[cfg(any(unix, test))]
struct ProcessInfo {
    name: String,
    ppid: i64,
}

#[cfg(unix)]
fn read_process(pid: i64) -> Option<ProcessInfo> {
    if cfg!(target_os = "linux") {
        let exe = std::fs::read_link(format!("/proc/{pid}/exe")).ok()?;
        let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
        let close = stat.rfind(')')?;
        let rest: Vec<&str> = stat[close + 2..].split(' ').collect();
        let ppid = rest
            .get(1)
            .and_then(|value| value.parse().ok())
            .unwrap_or(0);
        return Some(ProcessInfo {
            name: exe.to_string_lossy().to_string(),
            ppid,
        });
    }
    None
}

#[cfg(test)]
fn windows_process_command(pid: u32) -> (String, Vec<String>, WindowsExecOptions) {
    let script = [
        format!(
            "$p = Get-CimInstance Win32_Process -Filter \"ProcessId = {pid}\" -ErrorAction Stop"
        ),
        "if ($null -ne $p) {".to_string(),
        "  Write-Output ([string]$p.Name)".to_string(),
        "  Write-Output ([string]$p.ParentProcessId)".to_string(),
        "}".to_string(),
    ]
    .join("; ");
    (
        "powershell.exe".to_string(),
        vec![
            "-NoProfile".to_string(),
            "-NonInteractive".to_string(),
            "-Command".to_string(),
            script,
        ],
        WindowsExecOptions {
            windows_hide: true,
            timeout_ms: 1000,
        },
    )
}

#[cfg(test)]
fn read_windows_process(
    pid: u32,
    mut exec: impl FnMut(&str, &[String], &WindowsExecOptions) -> Result<String, String>,
) -> Option<ProcessInfo> {
    let (file, args, options) = windows_process_command(pid);
    let out = exec(&file, &args, &options).ok()?;
    let mut lines = out.trim().lines().map(str::trim);
    let name = lines.next().unwrap_or("").to_string();
    if name.is_empty() {
        return None;
    }
    let ppid = lines.next().unwrap_or("0").parse().unwrap_or(0);
    Some(ProcessInfo { name, ppid })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::interceptor::types::QoderSurface;

    #[test]
    fn classifies_desktop_and_cli_executables() {
        assert_eq!(
            classify_executable("/usr/bin/qoder"),
            Some(QoderSurface::Qoder)
        );
        assert_eq!(classify_executable("Qoder.exe"), Some(QoderSurface::Qoder));
        assert_eq!(
            classify_executable("qodercli"),
            Some(QoderSurface::QoderCli)
        );
        assert_eq!(
            classify_executable("qoder-cli"),
            Some(QoderSurface::QoderCli)
        );
    }

    #[test]
    fn ignores_qoder_work_and_interceptor_hosts() {
        assert_eq!(classify_executable("qoderwork"), None);
        assert_eq!(classify_executable("QwenWorkCN"), None);
        assert_eq!(classify_executable("qwenworkcn.exe"), None);
        assert_eq!(classify_executable("node"), None);
        assert_eq!(classify_executable("interceptor-cli"), None);
    }

    #[test]
    fn prefers_qoder_config_dir_when_it_points_at_the_desktop_profile() {
        let surface = resolve_qoder_surface_with(
            |key| (key == "QODER_CONFIG_DIR").then(|| "/home/u/.qoder".to_string()),
            &[],
        );
        assert_eq!(surface, Some(QoderSurface::Qoder));
    }

    #[test]
    fn uses_powershell_cim_rather_than_wmic() {
        let mut seen = None;
        let info = read_windows_process(456, |file, args, options| {
            seen = Some((file.to_string(), args.to_vec(), options.windows_hide));
            assert_eq!(options.timeout_ms, 1000);
            Ok("qodercli.exe\r\n123\r\n".to_string())
        })
        .unwrap();
        assert_eq!(info.name, "qodercli.exe");
        assert_eq!(info.ppid, 123);
        let (file, args, hide) = seen.unwrap();
        assert_eq!(file, "powershell.exe");
        assert!(args.iter().any(|arg| arg == "-NonInteractive"));
        assert!(args.iter().any(|arg| arg == "-Command"));
        assert!(hide);
    }
}
