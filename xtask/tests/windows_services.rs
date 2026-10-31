//! Windows 原生服务入口实跑验收；系统任务操作全部替换为隔离记录器。
//! 非 Windows 不运行此文件，不将 Linux 的静态检查冒充平台验收。

#![cfg(windows)]

use serde_json::{json, Value};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

fn shells() -> Vec<PathBuf> {
    let system = PathBuf::from(std::env::var_os("SystemRoot").expect("需要 Windows 系统目录"));
    let legacy = system.join("System32/WindowsPowerShell/v1.0/powershell.exe");
    let pwsh = std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default())
        .map(|directory| directory.join("pwsh.exe"))
        .find(|path| path.is_file())
        .expect("Windows 验收必须同时具备 PowerShell 5.1 和 pwsh，不能跳过");
    assert!(legacy.is_file(), "缺少 PowerShell 5.1");
    vec![legacy, pwsh]
}

struct Fixture {
    _temp: tempfile::TempDir,
    home: PathBuf,
    data: PathBuf,
    cache: PathBuf,
    binary: PathBuf,
    script: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let home = temporary.path().join("用户 O'Brien 张三 🚀");
        let data = home.join("数据 [隔离]");
        let cache = home.join("缓存");
        let version = cache.join("versions/1.2.0");
        fs::create_dir_all(version.join("bin")).unwrap();
        fs::create_dir_all(&data).unwrap();
        fs::write(cache.join("current"), "\u{feff}1.2.0\n").unwrap();
        fs::write(version.join("runtime-manifest.json"), "{}").unwrap();
        let binary = version.join("bin/loongsuite-pilot.exe");
        fs::copy(env!("CARGO_BIN_EXE_pilot-fixture"), &binary).unwrap();
        let script = home.join("loongsuite-pilot-service.ps1");
        fs::write(
            &script,
            include_bytes!("../../scripts/loongsuite-pilot.ps1"),
        )
        .unwrap();
        Self {
            _temp: temporary,
            home,
            data,
            cache,
            binary,
            script,
        }
    }

    fn invoke(&self, shell: &Path, body: &str, constrained: bool) -> Output {
        let test = self.home.join("case.ps1");
        let prelude = if constrained {
            "$ExecutionContext.SessionState.LanguageMode = 'ConstrainedLanguage'\n"
        } else {
            ""
        };
        fs::write(
            &test,
            format!(
                "\u{feff}{prelude}. $env:PILOT_SCRIPT\n$ErrorActionPreference = 'Stop'\n{body}\n"
            ),
        )
        .unwrap();
        let out = self.home.join("stdout.txt");
        let err = self.home.join("stderr.txt");
        let mut command = Command::new(shell);
        command
            .args([
                "-NoProfile",
                "-NonInteractive",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
            ])
            .arg(&test)
            .env_clear()
            .current_dir(&self.home)
            .env("HOME", &self.home)
            .env("USERPROFILE", &self.home)
            .env("APPDATA", self.home.join("appdata"))
            .env("LOCALAPPDATA", self.home.join("localappdata"))
            .env("TEMP", &self.home)
            .env("TMP", &self.home)
            .env("USERNAME", "pilot-fixture")
            .env("USERDOMAIN", "TEST")
            .env("LOONGSUITE_PILOT_DATA_DIR", &self.data)
            .env("LOONGSUITE_PILOT_CACHE_DIR", &self.cache)
            .env(
                "AGENT_DATA_COLLECTION_CONFIG",
                self.home.join("显式配置.json"),
            )
            .env("PILOT_SCRIPT", &self.script)
            .env("PILOT_FIXTURE_LOG", self.home.join("invocations.jsonl"))
            .env("PILOT_FIXTURE_MODE", "exit")
            .env("PATH", shell.parent().unwrap())
            .stdin(Stdio::null())
            .stdout(fs::File::create(&out).unwrap())
            .stderr(fs::File::create(&err).unwrap());
        for key in ["SystemRoot", "WINDIR", "PSModulePath"] {
            if let Some(value) = std::env::var_os(key) {
                command.env(key, value);
            }
        }
        let mut child = command.spawn().unwrap();
        let start = Instant::now();
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            if start.elapsed() > Duration::from_secs(30) {
                let _ = child.kill();
                let _ = child.wait();
                panic!("PowerShell 验收超时：{}", shell.display());
            }
            std::thread::sleep(Duration::from_millis(10));
        };
        Output {
            status,
            stdout: fs::read(out).unwrap(),
            stderr: fs::read(err).unwrap(),
        }
    }

    fn records(&self) -> Vec<Value> {
        fs::read_to_string(self.home.join("invocations.jsonl"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }
}

fn success(output: &Output) {
    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn both_shells_preserve_empty_quotes_unicode_and_exit_codes_including_clm() {
    for shell in shells() {
        for constrained in [false, true] {
            let fixture = Fixture::new();
            success(&fixture.invoke(&shell, r#"
Invoke-Pilot -Arguments @('span-attr', 'set', '自定义', '', 'a"b', 'C:\space end\', 'O''Brien 🚀', '\\"')
if ($script:PilotExitCode -ne 0) {
    $diagnostics = @($Error | ForEach-Object { [string]$_.FullyQualifiedErrorId + ': ' + [string]$_.Exception.Message })
    throw ('native invocation failed: exit=' + $script:PilotExitCode + ' last=' + $global:LASTEXITCODE + ' | ' + ($diagnostics -join ' | '))
}
$env:PILOT_FIXTURE_MODE = 'fail'
Invoke-Pilot -Arguments @('probe') 2>$null
if ($script:PilotExitCode -eq 0) { throw 'failure hidden' }
$LASTEXITCODE = 0
Invoke-Pilot -Binary (Join-Path $DATA_DIR 'missing.exe') -Arguments @() 2>$null
if ($script:PilotExitCode -eq 0) { throw 'missing binary reused stale success' }
"#, constrained));
            let records = fixture.records();
            assert_eq!(records.len(), 2);
            assert_eq!(
                records[0]["args"],
                json!([
                    "span-attr",
                    "set",
                    "自定义",
                    "",
                    "a\"b",
                    "C:\\space end\\",
                    "O'Brien 🚀",
                    "\\\\\""
                ])
            );
            assert_eq!(records[0]["data"], fixture.data.to_str().unwrap());
            assert_eq!(records[0]["cache"], fixture.cache.to_str().unwrap());
            assert_eq!(
                records[0]["config"],
                fixture.home.join("显式配置.json").to_str().unwrap()
            );
        }
    }
}

#[test]
fn pointers_reject_traversal_legacy_and_junctions_without_repairs() {
    for shell in shells() {
        let fixture = Fixture::new();
        success(&fixture.invoke(
            &shell,
            "if (-not (Resolve-PilotBinary)) { throw 'valid pointer rejected' }",
            false,
        ));
        for pointer in ["../outside", ".", "..", "bad.", "1.2.0/bin", "", "1.2.0 "] {
            // 指针允许外围空白，目录名自身不允许空白。
            let pointer = if pointer == "1.2.0 " {
                "1. 2.0"
            } else {
                pointer
            };
            fs::write(fixture.cache.join("current"), pointer).unwrap();
            success(&fixture.invoke(
                &shell,
                "if (Resolve-PilotBinary) { throw 'bad pointer accepted' }",
                false,
            ));
            assert_eq!(
                fs::read_to_string(fixture.cache.join("current")).unwrap(),
                pointer
            );
        }
        fs::write(fixture.cache.join("current"), "linked").unwrap();
        success(&fixture.invoke(&shell, r#"
New-Item -ItemType Junction -Path (Join-Path $VERSIONS_DIR 'linked') -Target (Join-Path $VERSIONS_DIR '1.2.0') | Out-Null
if (Resolve-PilotBinary) { throw 'junction accepted' }
"#, false));
    }
}

#[test]
fn vbs_launchers_run_native_roles_and_keep_unicode_without_registering_tasks() {
    for shell in shells() {
        let fixture = Fixture::new();
        for role in ["run", "run-updater"] {
            let body = format!(
                r#"
function New-ScheduledTaskAction {{ param($Execute, $Argument, $WorkingDirectory) return @{{ Execute=$Execute; Arguments=$Argument }} }}
Ensure-Dirs
$path = Join-Path $BOOTSTRAP_DIR '{role}.vbs'
$action = New-HiddenTaskAction $path (Resolve-PilotBinary) '{role}'
if ($action.Execute -ne 'wscript.exe') {{ throw 'incorrect task executable' }}
& (Join-Path $env:SystemRoot 'System32\cscript.exe') //B //NoLogo $path
exit $LASTEXITCODE
"#
            );
            success(&fixture.invoke(&shell, &body, false));
            let bytes = fs::read(fixture.cache.join(format!("bin/{role}.vbs"))).unwrap();
            assert!(bytes.starts_with(&[0xff, 0xfe]));
            let words: Vec<_> = bytes[2..]
                .chunks_exact(2)
                .map(|part| u16::from_le_bytes([part[0], part[1]]))
                .collect();
            let source = String::from_utf16(&words).unwrap();
            // Join-Path can expand the temp directory's Windows 8.3 alias.
            // Check that the actual VBS command names the same binary.
            let invocation = source
                .lines()
                .find(|line| line.starts_with("WScript.Quit sh.Run("))
                .unwrap();
            let emitted_binary = invocation
                .strip_prefix("WScript.Quit sh.Run(\"\"\"")
                .unwrap()
                .split("\"\" ")
                .next()
                .unwrap();
            assert_eq!(
                fs::canonicalize(emitted_binary).unwrap(),
                fs::canonicalize(&fixture.binary).unwrap()
            );
            assert!(!source.contains("daemon.js"));
            let records = fixture.records();
            let last = records.last().unwrap();
            assert_eq!(last["args"], json!([role]));
            assert_eq!(last["data"], fixture.data.to_str().unwrap());
            assert_eq!(last["cache"], fixture.cache.to_str().unwrap());
        }
    }
}

const MOCK_SERVICES: &str = r#"
function Get-ScheduledTask { return $null }
function Stop-PidFile {}
function Stop-OrphanProcesses {}
function Start-Sleep {}
function Schedule-UpdaterRestart { throw 'unexpected handoff' }
function Get-RestartDiagnostics { return @{} }
function Start-BackgroundDaemon { throw 'managed install must not fall back' }
$script:started = $false
function Start-ScheduledTask { $script:started = $true }
function Get-InitType { return 'taskscheduler' }
"#;

#[test]
fn registration_denial_does_not_skip_start_and_failed_selfheal_writes_breadcrumb() {
    for shell in shells() {
        for target in ["Collector", "Updater"] {
            let fixture = Fixture::new();
            let option = if target == "Collector" {
                "-Options @('--defer-updater-restart')"
            } else {
                ""
            };
            let success_body = format!(
                r#"{MOCK_SERVICES}
function Get-TaskQuery {{ return @{{exists=$true; error=''}} }}
function Get-TaskStartIntent {{ return @{{should_start=$true; confirmed_missing=$false; exists_schtasks='yes'}} }}
function Install-{target}Task {{ throw 'registration denied' }}
function Wait-ForCollectorHeartbeat {{ return $true }}
function Wait-ForUpdaterAlive {{ return $true }}
Cmd-Restart{target} {option}
if (-not $script:started) {{ throw 'start skipped after denied registration' }}
"#
            );
            success(&fixture.invoke(&shell, &success_body, false));
            let failure_body = format!(
                r#"{MOCK_SERVICES}
function Get-TaskQuery {{ return @{{exists=$false; error=''}} }}
function Get-TaskStartIntent {{ return @{{should_start=$false; confirmed_missing=$true; exists_schtasks='no'}} }}
function Install-{target}Task {{ return $true }}
function Wait-ForCollectorHeartbeat {{ return $false }}
function Wait-ForUpdaterAlive {{ return $false }}
$failed = $false
try {{ Cmd-Restart{target} {option} }} catch {{ $failed = $true }}
if (-not $failed -or -not $script:started) {{ throw 'selfheal failure or start lost' }}
"#
            );
            success(&fixture.invoke(&shell, &failure_body, false));
            let text = fs::read_to_string(fixture.data.join(format!(
                "logs/last-restart-failure-{}.json",
                target.to_lowercase()
            )))
            .unwrap();
            let report: Value = serde_json::from_str(text.trim_start_matches('\u{feff}')).unwrap();
            assert_eq!(report["stage"], "selfheal-not-running");
            assert_eq!(report["target"], target.to_lowercase());
            assert!(report["diag"].is_object());
        }
    }
}
