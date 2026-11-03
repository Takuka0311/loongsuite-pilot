//! Disposable Windows-only phase probe. Production ACL/script/options are unchanged.
use loongsuite_pilot::local_workers::{WINDOWS_ACL_COMMAND_TIMEOUT_MS, WINDOWS_ACL_PROTECT};
use loongsuite_pilot::utils::candidate_process;
use std::time::Duration;

const STAGES: &[&str] = &[
    "entry",
    "before-import-utility",
    "import-utility-ready",
    "before-import-security",
    "import-security-ready",
    "before-identity",
    "identity-ready",
    "before-utility-select",
    "utility-select-ready",
    "before-getacl",
    "getacl-ready",
    "before-access-rules",
    "access-rules-ready",
    "before-private-check",
    "private-check-ready",
    "already-private",
    "before-descriptor",
    "descriptor-ready",
    "before-setacl",
    "setacl-ready",
    "before-verify-getacl",
    "verify-getacl-ready",
    "complete",
];

fn around(script: &mut String, source: &str, before: &str, after: &str) {
    assert_eq!(
        script.matches(source).count(),
        1,
        "production phase boundary changed"
    );
    *script = script.replacen(
        source,
        &format!("Trace-AclStage '{before}'\n{source}\nTrace-AclStage '{after}'"),
        1,
    );
}

fn staged_script(explicit_imports: bool) -> String {
    let mut script = WINDOWS_ACL_PROTECT.to_owned();
    around(
        &mut script,
        "$currentSid = [System.Security.Principal.WindowsIdentity]::GetCurrent().User.Value",
        "before-identity",
        "identity-ready",
    );
    around(
        &mut script,
        r#"$allowedSids = @($currentSid, "S-1-5-18", "S-1-5-32-544") | Select-Object -Unique"#,
        "before-utility-select",
        "utility-select-ready",
    );
    around(
        &mut script,
        "$acl = Get-Acl -LiteralPath $target",
        "before-getacl",
        "getacl-ready",
    );
    around(&mut script,
        "  $rules = $acl.GetAccessRules(\n    $true,\n    $true,\n    [System.Security.Principal.SecurityIdentifier]\n  )",
        "before-access-rules", "access-rules-ready");
    around(
        &mut script,
        "if (Test-PrivateAcl $acl) {\n  exit 0\n}",
        "before-private-check",
        "private-check-ready",
    );
    script = script.replacen(
        "  exit 0\n",
        "  Trace-AclStage 'already-private'\n  exit 0\n",
        1,
    );
    around(&mut script,
        "$acl.SetSecurityDescriptorSddlForm(\n  $sddl,\n  [System.Security.AccessControl.AccessControlSections]::Access\n)",
        "before-descriptor", "descriptor-ready");
    around(
        &mut script,
        "Set-Acl -LiteralPath $target -AclObject $acl",
        "before-setacl",
        "setacl-ready",
    );
    around(
        &mut script,
        "$verified = Get-Acl -LiteralPath $target",
        "before-verify-getacl",
        "verify-getacl-ready",
    );
    let imports = if explicit_imports {
        r#"
Trace-AclStage 'before-import-utility'
Import-Module -Name ($PSHOME + '\Modules\Microsoft.PowerShell.Utility\Microsoft.PowerShell.Utility.psd1') -ErrorAction Stop
Trace-AclStage 'import-utility-ready'
Trace-AclStage 'before-import-security'
Import-Module -Name ($PSHOME + '\Modules\Microsoft.PowerShell.Security\Microsoft.PowerShell.Security.psd1') -ErrorAction Stop
Trace-AclStage 'import-security-ready'
"#
    } else {
        ""
    };
    format!(
        r#"[IO.File]::WriteAllText($env:PILOT_TEST_ACL_STAGE_FILE, ('entry 0' + [Environment]::NewLine))
$aclWatch = [Diagnostics.Stopwatch]::StartNew()
function Trace-AclStage([string]$stage) {{
    [IO.File]::AppendAllText($env:PILOT_TEST_ACL_STAGE_FILE, $stage + ' ' + $aclWatch.ElapsedMilliseconds + [Environment]::NewLine)
}}
{imports}
{script}
Trace-AclStage 'complete'
"#
    )
}

async fn probe(explicit_imports: bool) {
    let root = tempfile::Builder::new()
        .prefix("pilot-arm-acl-probe-")
        .tempdir()
        .unwrap();
    let target = root.path().join("实例 目录");
    let trace = root.path().join("phase.trace");
    std::fs::create_dir_all(&target).unwrap();
    let case = if explicit_imports {
        "explicit-builtin-imports"
    } else {
        "current-production"
    };
    let mut command = tokio::process::Command::new("powershell.exe");
    command
        .args(["-NoProfile", "-NonInteractive", "-Command"])
        .arg(staged_script(explicit_imports))
        .env("LOONGSUITE_WORKER_ACL_TARGET", &target)
        .env("PILOT_TEST_ACL_STAGE_FILE", &trace);
    let result = candidate_process::output(
        &mut command,
        Duration::from_millis(WINDOWS_ACL_COMMAND_TIMEOUT_MS),
        1024 * 1024,
    )
    .await;
    let category = match &result {
        Ok(output) if output.status.success() => "success",
        Ok(_) => "nonzero",
        Err(error) if error.kind() == std::io::ErrorKind::TimedOut => "timeout",
        Err(_) => "io-error",
    };
    eprintln!("worker-acl-probe case={case} result={category}");
    let phases = std::fs::read_to_string(&trace).unwrap_or_default();
    for line in phases.lines() {
        if let Some((stage, millis)) = line.split_once(' ') {
            if STAGES.contains(&stage)
                && !millis.is_empty()
                && millis.bytes().all(|byte| byte.is_ascii_digit())
            {
                eprintln!("worker-acl-probe case={case} stage={stage} elapsed_ms={millis}");
            }
        }
    }
    assert!(
        matches!(result, Ok(ref output) if output.status.success()),
        "exact production ACL command did not finish; case={case} category={category}"
    );
}

// A real test exists only on Windows. Other targets type-check these complete
// portable bodies without registering a fake passed or ignored Windows test.
#[cfg_attr(windows, tokio::test)]
pub async fn a_current_production_acl_stage_probe() {
    probe(false).await;
}

#[cfg_attr(windows, tokio::test)]
pub async fn b_builtin_import_acl_stage_probe() {
    probe(true).await;
}
