//! 原生 Hook 的本地协议测试，不代表真实 Agent 会话或跨平台注入验收。

use serde_json::{json, Value};
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

fn workspace() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}

fn execute(mut command: Command, root: &Path, input: &[u8]) -> Output {
    command
        .current_dir(root)
        .env_clear()
        .env("HOME", root)
        .env("USERPROFILE", root)
        .env("APPDATA", root.join("appdata"))
        .env("LOCALAPPDATA", root.join("localappdata"))
        .env("LOONGSUITE_PILOT_CACHE_DIR", root.join("other-cache"))
        .env("CODEX_HOME", root.join("codex 中文🚀"))
        .env("PATH", root.join("no-runtimes"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for key in [
        "SystemRoot",
        "WINDIR",
        "TEMP",
        "TMP",
        "ComSpec",
        "ProgramFiles",
        "ProgramFiles(x86)",
        "ProgramW6432",
        "PATHEXT",
    ] {
        if let Some(value) = std::env::var_os(key) {
            command.env(key, value);
        }
    }
    let label = format!("{command:?}");
    let mut child =
        loongsuite_pilot::utils::candidate_process::CandidateChild::spawn(&mut command).unwrap();
    let stdout = child.take_stdout().unwrap();
    let stderr = child.take_stderr().unwrap();
    let out = std::thread::spawn(move || {
        let mut bytes = Vec::new();
        stdout
            .take(1024 * 1024 + 1)
            .read_to_end(&mut bytes)
            .unwrap();
        bytes
    });
    let err = std::thread::spawn(move || {
        let mut bytes = Vec::new();
        stderr
            .take(1024 * 1024 + 1)
            .read_to_end(&mut bytes)
            .unwrap();
        bytes
    });
    child.take_stdin().unwrap().write_all(input).unwrap();
    let start = Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if start.elapsed() > Duration::from_secs(20) {
            // The scoped process group/Job kills the native grandchild too,
            // so readers cannot stay blocked after PowerShell is terminated.
            drop(child);
            let stdout = out.join().unwrap();
            let stderr = err.join().unwrap();
            panic!(
                "Hook 未在限定时间内退出：{label}，stdout={}，stderr={}",
                String::from_utf8_lossy(&stdout[..stdout.len().min(2048)]),
                String::from_utf8_lossy(&stderr[..stderr.len().min(2048)])
            );
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    let stdout = out.join().unwrap();
    let stderr = err.join().unwrap();
    assert!(
        stdout.len() <= 1024 * 1024 && stderr.len() <= 1024 * 1024,
        "Hook 输出超限"
    );
    Output {
        status,
        stdout,
        stderr,
    }
}

fn fixture(root: &Path) -> PathBuf {
    let hooks = root.join("data with spaces 中文🚀/hooks");
    fs::create_dir_all(hooks.join("shared")).unwrap();
    for file in [
        "codex-loongsuite-pilot-hook.sh",
        "codex-loongsuite-pilot-hook.ps1",
        "shared/native.sh",
        "shared/native.ps1",
    ] {
        fs::copy(
            workspace().join("assets/hooks").join(file),
            hooks.join(file),
        )
        .unwrap();
    }
    let bin = root.join(if cfg!(windows) {
        "原生 程序🚀.exe"
    } else {
        "原生 程序🚀"
    });
    fs::copy(env!("CARGO_BIN_EXE_loongsuite-pilot"), &bin).unwrap();
    fs::write(hooks.join("pilot-bin"), format!("{}\n", bin.display())).unwrap();
    fs::create_dir(root.join("other-cache")).unwrap();
    fs::write(root.join("other-cache/current"), "../../unrelated").unwrap();
    hooks
}

fn assert_codex_roundtrip(mut command: Command, root: &Path, hooks: &Path, script: &Path) {
    command.arg(script).arg("stop");
    let marker = hooks
        .parent()
        .unwrap()
        .join("state/codex/transcript-wakeups/native-hook.json");
    if marker.exists() {
        fs::remove_file(&marker).unwrap();
    }
    let mut payload = vec![0xef, 0xbb, 0xbf];
    payload.extend(serde_json::to_vec(&json!({
        "session_id":"native-hook", "turn_id":"轮次 🚀", "transcript_path":"C:/用户 文件/记录🚀.jsonl"
    })).unwrap());
    let output = execute(command, root, &payload);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(String::from_utf8(output.stdout).unwrap().trim(), "{}");
    let marker = hooks
        .parent()
        .unwrap()
        .join("state/codex/transcript-wakeups/native-hook.json");
    let value: Value = serde_json::from_slice(&fs::read(marker).unwrap()).unwrap();
    assert_eq!(value["turn_id"], "轮次 🚀");
    assert_eq!(value["transcript_path"], "C:/用户 文件/记录🚀.jsonl");
    assert_eq!(
        value["codex_home"],
        root.join("codex 中文🚀").to_string_lossy().as_ref()
    );
    assert!(!root.join(".loongsuite-pilot").exists());
}

#[cfg(unix)]
#[test]
fn shell_preserves_raw_bom_unicode_and_bound_install_without_runtimes() {
    let root = tempfile::tempdir().unwrap();
    let hooks = fixture(root.path());
    assert_codex_roundtrip(
        Command::new("/bin/bash"),
        root.path(),
        &hooks,
        &hooks.join("codex-loongsuite-pilot-hook.sh"),
    );
}

#[cfg(unix)]
#[test]
fn shell_rejects_bad_pointer_and_missing_binary_without_protocol_noise() {
    let root = tempfile::tempdir().unwrap();
    let hooks = fixture(root.path());
    fs::write(hooks.join("pilot-bin"), "/missing/native-binary\n").unwrap();
    let mut command = Command::new("/bin/bash");
    command.arg(hooks.join("codex-loongsuite-pilot-hook.sh"));
    let output = execute(command, root.path(), b"{}");
    assert!(output.status.success());
    assert_eq!(output.stdout, b"{}\n");
    assert!(!output.stderr.is_empty());
    assert!(!hooks.parent().unwrap().join("state").exists());
    fs::remove_file(hooks.join("pilot-bin")).unwrap();
    let mut command = Command::new("/bin/bash");
    command.arg(hooks.join("codex-loongsuite-pilot-hook.sh"));
    let output = execute(command, root.path(), b"{}");
    assert_eq!(output.stdout, b"{}\n");
    assert!(!output.stderr.is_empty());
}

#[cfg(unix)]
#[test]
fn every_shell_entry_has_valid_syntax_and_no_legacy_runtime_fallback() {
    let mut count = 0;
    for entry in fs::read_dir(workspace().join("assets/hooks")).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|s| s.to_str()) != Some("sh") {
            continue;
        }
        let status = Command::new("/bin/bash")
            .arg("-n")
            .arg(&path)
            .status()
            .unwrap();
        assert!(status.success(), "{}", path.display());
        // 此入口在 Agent 进程内加载 preload，属于批准保留的宿主桥接。
        if path
            .file_name()
            .is_some_and(|name| name == "qodercli-runtime-wrapper.sh")
        {
            continue;
        }
        let body = fs::read_to_string(&path).unwrap();
        for forbidden in [
            "python",
            "node-bin",
            "node.exe",
            "hook-processor.mjs",
            "command -v loongsuite-pilot",
        ] {
            assert!(!body.contains(forbidden), "{}: {forbidden}", path.display());
        }
        count += 1;
    }
    assert_eq!(count, 14);
}

#[cfg(windows)]
#[test]
fn powershell_51_and_pwsh_preserve_native_stdin_in_both_language_modes() {
    let root = tempfile::tempdir().unwrap();
    let hooks = fixture(root.path());
    for shell in ["powershell.exe", "pwsh.exe"] {
        let executable = which::which(shell).unwrap_or_else(|_| panic!("Windows 验收缺少 {shell}"));
        for constrained in [false, true] {
            let harness = hooks.join(format!(
                "runner-{shell}-{}.ps1",
                if constrained { "constrained" } else { "full" }
            ));
            let mode = if constrained {
                "$ExecutionContext.SessionState.LanguageMode = 'ConstrainedLanguage'\n"
            } else {
                ""
            };
            fs::write(
                &harness,
                format!(
                    "{mode}& (Join-Path $PSScriptRoot 'codex-loongsuite-pilot-hook.ps1') stop\n"
                ),
            )
            .unwrap();
            let mut command = Command::new(&executable);
            command.args([
                "-NoProfile",
                "-NonInteractive",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
            ]);
            assert_codex_roundtrip(command, root.path(), &hooks, &harness);
        }
    }
}

#[cfg(unix)]
#[test]
fn qodercn_bootstraps_bash_from_sh_or_posix_mode_without_losing_stdin_or_agent_id() {
    for program in ["/bin/sh", "/bin/bash"] {
        let root = tempfile::tempdir().unwrap();
        let hooks = fixture(root.path());
        let script = hooks.join("qodercn-loongsuite-pilot-hook.sh");
        fs::copy(
            workspace().join("assets/hooks/qodercn-loongsuite-pilot-hook.sh"),
            &script,
        )
        .unwrap();
        fs::create_dir(root.path().join("no-runtimes")).unwrap();
        std::os::unix::fs::symlink("/bin/bash", root.path().join("no-runtimes/bash")).unwrap();
        let transcript = root.path().join("CN transcript 中文🚀.jsonl");
        let rows = [
            json!({"type":"user","timestamp":"2026-10-01T10:00:00Z","message":{"content":"你好 🚀"}}),
            json!({"type":"assistant","timestamp":"2026-10-01T10:00:01Z","message":{
                "model":"cn-test","content":[{"type":"text","text":"hello back"}],"stop_reason":"end_turn"}}),
            json!({"type":"last-prompt","lastPrompt":"你好 🚀"}),
        ];
        fs::write(
            &transcript,
            rows.iter()
                .map(|row| row.to_string() + "\n")
                .collect::<String>(),
        )
        .unwrap();
        let mut payload = vec![0xef, 0xbb, 0xbf];
        payload.extend(
            serde_json::to_vec(
                &json!({"session_id":"cn-through-sh","transcript_path":transcript,
            "extra":{"user":{"name":"CN 用户"},"email":"cn@example.com"}}),
            )
            .unwrap(),
        );
        let mut command = Command::new("/bin/sh");
        command.args([
            "-c",
            "POSIXLY_CORRECT=1 exec \"$@\"",
            "pilot-posix-launch",
            program,
        ]);
        if program == "/bin/bash" {
            command.arg("--posix");
        }
        command.arg(&script).arg("qoder-cn");
        let result = execute(command, root.path(), &payload);
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert_eq!(result.stdout, b"{}\n");
        let history = hooks.parent().unwrap().join("logs/qoder-cn/history");
        let records: Vec<Value> = fs::read_dir(history)
            .unwrap()
            .flat_map(|entry| {
                fs::read_to_string(entry.unwrap().path())
                    .unwrap()
                    .lines()
                    .map(|line| serde_json::from_str::<Value>(line).unwrap())
                    .collect::<Vec<_>>()
            })
            .collect();
        assert!(!records.is_empty());
        assert!(records
            .iter()
            .all(|row| row["gen_ai.agent.type"] == "qoder-cn"
                && row["user.name"] == "CN 用户"
                && row["user.email"] == "cn@example.com"));
        assert!(records
            .iter()
            .any(|row| row.to_string().contains("你好 🚀")));
    }
}

#[cfg(windows)]
#[test]
fn native_argument_probe_fixture() {
    let args: Vec<String> = std::env::args().collect();
    if !args.iter().any(|arg| arg == "pilot-argument-values") {
        return;
    }
    let mut input = Vec::new();
    std::io::stdin().read_to_end(&mut input).unwrap();
    fs::write(
        std::env::current_dir().unwrap().join("argv-probe.json"),
        serde_json::to_vec(&json!({"args":args,"input":input})).unwrap(),
    )
    .unwrap();
    std::io::stdout()
        .write_all("PILOT_NATIVE_STDOUT:中文🚀\n".as_bytes())
        .unwrap();
}

#[cfg(windows)]
#[test]
fn powershell_bridge_preserves_raw_bytes_and_native_argument_boundaries() {
    let root = tempfile::tempdir().unwrap();
    let hooks = fixture(root.path());
    let probe = root.path().join("argv probe 中文🚀.exe");
    fs::copy(std::env::current_exe().unwrap(), &probe).unwrap();
    fs::write(hooks.join("pilot-bin"), format!("{}\n", probe.display())).unwrap();
    let values = [
        "pilot-argument-values",
        "",
        "space argument",
        r#"quote"inside"#,
        r#"trailing space \"#,
        "中文🚀",
        r#"C:\space dir\quote"and-slash\"#,
    ];
    let mut args = vec![
        "--exact",
        "native_argument_probe_fixture",
        "--nocapture",
        "--",
    ];
    args.extend(values);
    let literal = |arg: &str| format!("'{}'", arg.replace(char::from(39), "''"));
    let array = args
        .iter()
        .map(|arg| literal(arg))
        .collect::<Vec<_>>()
        .join(",");
    let mut input = vec![0xef, 0xbb, 0xbf];
    input.extend(serde_json::to_vec(&json!({"probe":"中文🚀"})).unwrap());
    for shell in ["powershell.exe", "pwsh.exe"] {
        let executable = which::which(shell).unwrap_or_else(|_| panic!("Windows 验收缺少 {shell}"));
        for constrained in [false, true] {
            let output_path = root.path().join("argv-probe.json");
            if output_path.exists() {
                fs::remove_file(&output_path).unwrap();
            }
            let script = hooks.join(format!("argv-{shell}-{constrained}.ps1"));
            let mode = if constrained {
                "$ExecutionContext.SessionState.LanguageMode = 'ConstrainedLanguage'\n"
            } else {
                ""
            };
            let body = format!("{mode}. (Join-Path $PSScriptRoot 'shared\\native.ps1')\nInvoke-PilotNative -HookDir $PSScriptRoot -Arguments @({array}) -EmptyResult ''\n");
            // Windows PowerShell 5.1 needs a BOM for literal Unicode in scripts.
            let mut source = vec![0xef, 0xbb, 0xbf];
            source.extend(body.as_bytes());
            fs::write(&script, source).unwrap();
            let mut command = Command::new(&executable);
            command
                .args([
                    "-NoProfile",
                    "-NonInteractive",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-File",
                ])
                .arg(&script);
            let output = execute(command, root.path(), &input);
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(
                output.stderr.is_empty(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(String::from_utf8(output.stdout)
                .unwrap()
                .contains("PILOT_NATIVE_STDOUT:中文🚀"));
            let observed: Value = serde_json::from_slice(&fs::read(&output_path).unwrap()).unwrap();
            let actual: Vec<String> = serde_json::from_value(observed["args"].clone()).unwrap();
            let expected: Vec<String> = args.iter().map(|arg| arg.to_string()).collect();
            assert_eq!(&actual[1..], expected.as_slice());
            assert_eq!(observed["input"], json!(input));
        }
    }
}
