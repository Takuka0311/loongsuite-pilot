//! Complete public CLI entry under an ephemeral CI-only macOS user.
//! No test switch or adapter replacement is added to the product.
#[cfg(target_os = "macos")]
#[test]
fn physical_macos_whole_shipped_cli_install_status_uninstall_without_node_or_build_output() {
    use std::{path::Path, process::Command, time::Duration};
    assert_eq!(
        std::env::var("GITHUB_ACTIONS").as_deref(),
        Ok("true"),
        "this account fixture runs only on an ephemeral GitHub macOS runner"
    );
    assert_eq!(
        Command::new("/usr/bin/id")
            .arg("-u")
            .output()
            .unwrap()
            .stdout,
        b"0\n",
        "workflow must run this fixture as root so timeout cleanup owns all private children"
    );
    let state = std::env::var_os("PILOT_SHORTCUT_CLI_CI_STATE").expect("private cleanup journal");
    let script =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/host/macos/shortcut_cli_owned_user.py");
    let mut command = tokio::process::Command::new("/usr/bin/python3");
    command
        .arg(script)
        .arg("--state")
        .arg(state)
        .arg("--binary")
        .arg(env!("CARGO_BIN_EXE_loongsuite-pilot"))
        .arg("--icon")
        .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("assets/dashboard-shortcut/AppIcon.icns"))
        .arg("--target")
        .arg(loongsuite_pilot::utils::runtime_paths::build_target())
        .arg("--edition")
        .arg(loongsuite_pilot::utils::runtime_paths::build_edition())
        .arg("--version")
        .arg(env!("CARGO_PKG_VERSION"));
    // Account setup and cleanup are separately bounded fixture phases. The
    // Python case itself preserves the original shared thirty-second deadline.
    let output = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(loongsuite_pilot::utils::candidate_process::output(
            &mut command,
            Duration::from_secs(60),
            1024 * 1024,
        ))
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["result"], "PASS");
    assert_eq!(result["actions"], 5);
    assert_eq!(result["realUserDockUnchanged"], true);
    assert_eq!(result["noUiOrDockStarted"], true);
}
