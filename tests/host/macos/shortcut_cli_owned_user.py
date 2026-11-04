"""One original GUI entry contract, executed under an ephemeral CI-only UID."""
import argparse, hashlib, json, os, pathlib, plistlib, pwd, shutil, subprocess, sys, time, uuid

DEADLINE = None
def command(argv, user=None, check=True, timeout=5, env=None):
    if user:
        argv = ["/usr/bin/sudo", "-n", "-H", "-u", user] + list(argv)
    if DEADLINE is not None:
        timeout = min(timeout, max(0.01, DEADLINE - time.monotonic()))
    result = subprocess.run(argv, capture_output=True, timeout=timeout, env=env)
    if check and result.returncode:
        raise AssertionError("fixture command failed: " + repr(argv[:4]) + "\n" +
            result.stderr.decode("utf-8", "replace")[-2000:])
    return result

def cleanup(state_path):
    state_path = pathlib.Path(state_path)
    if not state_path.exists():
        return
    state = json.loads(state_path.read_text())
    user, uid, root = state["user"], state["uid"], pathlib.Path(state["root"])
    assert user.startswith("pilotcli_") and len(user) == 21 and user[9:].isalnum()
    assert 61000 <= uid <= 64999
    assert root.parent == pathlib.Path("/private/tmp") and root.name == "pilot-shortcut-cli-" + state["nonce"]
    found = command(["/usr/bin/dscl", ".", "-read", "/Users/" + user, "UniqueID"], check=False)
    if found.returncode == 0:
        assert found.stdout.decode().strip() == "UniqueID: " + str(uid)
        command(["/usr/bin/pkill", "-TERM", "-u", str(uid)], check=False, timeout=2)
        command(["/usr/bin/dscl", ".", "-delete", "/Users/" + user])
    elif state.get("created"):
        # Creation is local to this randomly named, absent-before-test record.
        probe = command(["/usr/bin/dscl", ".", "-read", "/Users/" + user], check=False)
        if probe.returncode == 0:
            assert ("UniqueID:" not in probe.stdout.decode())
            command(["/usr/bin/dscl", ".", "-delete", "/Users/" + user])
    shutil.rmtree(root, ignore_errors=False) if root.exists() else None
    state_path.unlink()

def snapshot(user):
    result = command(["/usr/bin/defaults", "export", "com.apple.dock", "-"], user=user, check=False)
    return result.returncode, result.stdout

def main(args):
    global DEADLINE
    assert sys.platform == "darwin" and os.geteuid() == 0
    assert os.environ.get("GITHUB_ACTIONS") == "true", "never run the account fixture on a user Mac"
    runner = os.environ["PILOT_SHORTCUT_CLI_RUNNER_USER"]
    assert pwd.getpwnam(runner).pw_uid != 0
    nonce = uuid.uuid4().hex
    user = "pilotcli_" + nonce[:12]
    uid = 61000 + int(nonce[12:20], 16) % 4000
    while True:
        try:
            pwd.getpwuid(uid)
        except KeyError:
            break
        uid = 61000 + ((uid - 61000 + 1) % 4000)
    root = pathlib.Path("/private/tmp") / ("pilot-shortcut-cli-" + nonce)
    home = root / "private home 配置 O'Brien & spaces"
    state = {"user":user, "uid":uid, "root":str(root), "nonce":nonce, "created":False}
    state_path = pathlib.Path(args.state)
    state_path.write_text(json.dumps(state)); state_path.chmod(0o600)
    original_dock = snapshot(runner)
    try:
        assert command(["/usr/bin/dscl", ".", "-read", "/Users/" + user], check=False).returncode != 0
        root.mkdir(mode=0o755); root.chmod(0o755)
        home.mkdir(mode=0o755); home.chmod(0o755)
        record = "/Users/" + user
        command(["/usr/bin/dscl", ".", "-create", record])
        state["created"] = True; state_path.write_text(json.dumps(state))
        for key, value in [
            ("UniqueID", str(uid)), ("PrimaryGroupID", "20"),
            ("NFSHomeDirectory", str(home)), ("UserShell", "/usr/bin/false"),
            ("RealName", "Pilot isolated CLI fixture " + nonce), ("IsHidden", "1"),
        ]:
            command(["/usr/bin/dscl", ".", "-create", record, key, value])
        pkg = home / "package with spaces"
        (pkg / "bin").mkdir(parents=True)
        (pkg / "assets/dashboard-shortcut").mkdir(parents=True)
        (pkg / "agents.d").mkdir()
        binary = pkg / "bin/loongsuite-pilot"
        shutil.copy2(args.binary, binary); binary.chmod(0o755)
        icon = pkg / "assets/dashboard-shortcut/AppIcon.icns"
        shutil.copy2(args.icon, icon)
        resources = [{"path":str(path.relative_to(pkg)), "sha256":hashlib.sha256(path.read_bytes()).hexdigest()}
            for path in [binary, icon]]
        (pkg / "runtime-manifest.json").write_text(json.dumps({
            "schema":1, "runtime":"rust", "version":args.version, "gitCommit":"c9dcc0a2c274ac5765f2513715f78a3a86f4d1e2",
            "target":args.target, "edition":args.edition, "binary":"bin/loongsuite-pilot", "resources":resources,
        }))
        config = home / "配置 ' & spaces.json"
        config.write_text('\ufeff' + json.dumps({"dashboard":{"port":9130}, "dataDir":str(home / "data")}))
        (home / ".Trash").mkdir(mode=0o700)
        (home / "Library/Preferences").mkdir(parents=True)
        initial = {"persistent-apps":[], "persistent-others":[], "autohide":True, "tilesize":44}
        initial_file = root / "private-initial.plist"
        initial_file.write_bytes(plistlib.dumps(initial))
        for path in [home] + list(home.rglob("*")):
            os.chown(path, uid, 20)
        # Identity positive control precedes every real CLI action.
        identity = command(["/usr/bin/osascript", "-l", "JavaScript", "-e",
            "ObjC.import('Foundation'); JSON.stringify({home:ObjC.unwrap($.NSHomeDirectory()),user:ObjC.unwrap($.NSUserName())})"],
            user=user).stdout
        identity = json.loads(identity)
        assert identity["user"] == user, identity
        assert pathlib.Path(identity["home"]).resolve(strict=True) == home.resolve(strict=True), identity
        assert command(["/usr/bin/id", "-u"], user=user).stdout.strip() == str(uid).encode()
        command(["/usr/bin/defaults", "import", "com.apple.dock", str(initial_file)], user=user)
        before_private = snapshot(user)
        assert snapshot(runner) == original_dock, "runner Dock changed during fixture staging"
        assert command(["/usr/bin/pgrep", "-u", str(uid), "-x", "Dock"], check=False).returncode != 0
        # Account/package setup corresponds to original beforeEach. All original
        # case actions below share the original thirty-second overall deadline.
        started = time.monotonic(); DEADLINE = started + 30
        action_count = 0
        def cli(action):
            nonlocal action_count
            argv = ["/usr/bin/env", "-i", "HOME="+str(home), "USERPROFILE="+str(home),
                "USER="+user, "LOGNAME="+user, "PATH=/usr/bin:/bin:/usr/sbin:/sbin",
                "AGENT_DATA_COLLECTION_CONFIG="+str(config), "LOONGSUITE_PILOT_DATA_DIR="+str(home / "data"),
                "LOONGSUITE_PILOT_CACHE_DIR="+str(home / "cache"), "LOONGSUITE_PILOT_LANG=en", "NODE_PATH=",
                str(binary), "dashboard-shortcut", action]
            result = command(argv, user=user, timeout=20).stdout.decode()
            action_count += 1
            return result
        assert "http://127.0.0.1:9130/" in cli("install")
        shortcut = home / "Library/Application Support/LoongSuite Pilot/Shortcuts/LoongSuite Pilot Dashboard.webloc"
        stored = plistlib.loads(shortcut.read_bytes())
        assert stored["URL"] == "http://127.0.0.1:9130/"
        private_snapshot = snapshot(user)
        config.write_text(json.dumps({"dashboard":{"port":9131},"dataDir":str(home / "data")}))
        os.chown(config, uid, 20)
        assert "http://127.0.0.1:9130/" in cli("status")
        assert plistlib.loads(shortcut.read_bytes())["URL"] == "http://127.0.0.1:9130/"
        assert "http://127.0.0.1:9131/" in cli("install")
        assert snapshot(user) == private_snapshot
        config.unlink(); icon.unlink()
        assert "http://127.0.0.1:9131/" in cli("status")
        assert "Shortcut: not installed" in cli("uninstall")
        assert not shortcut.exists()
        assert snapshot(user) == before_private
        assert any((home / ".Trash").iterdir()), "real private-user trash was not used"
        for name in ["node_modules","dist","src"]:
            assert not (pkg / name).exists()
        assert command(["/usr/bin/pgrep", "-u", str(uid), "-x", "Dock"], check=False).returncode != 0
        assert snapshot(runner) == original_dock, "real runner Dock data changed"
        assert time.monotonic() - started < 30
        print(json.dumps({"result":"PASS","originalSource":"tests/integration/dashboard-shortcut-macos.test.mjs:L118:C3",
            "actions":action_count,"seconds":time.monotonic()-started,"nativeCli":True,"privateUid":True,
            "realUserDockUnchanged":True,"noUiOrDockStarted":True,"noNodeBuildOrDependencies":True}))
    finally:
        DEADLINE = None
        cleanup(state_path)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cleanup-state")
    parser.add_argument("--state"); parser.add_argument("--binary"); parser.add_argument("--icon")
    parser.add_argument("--target"); parser.add_argument("--edition"); parser.add_argument("--version")
    args = parser.parse_args()
    if args.cleanup_state:
        assert os.geteuid() == 0 and os.environ.get("GITHUB_ACTIONS") == "true"
        cleanup(args.cleanup_state)
    else:
        main(args)
