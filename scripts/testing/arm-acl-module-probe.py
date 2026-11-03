#!/usr/bin/env python3
"""Private module/CLM diagnostic; no product configuration or services."""
import json
import os
import shutil
import subprocess
import sys
BASE = r"""
$ErrorActionPreference = 'Stop'
try {
  $env:PSModulePath = $PSHOME + '\Modules'
  Import-Module -Name ($PSHOME + '\Modules\Microsoft.PowerShell.Utility\Microsoft.PowerShell.Utility.psd1') -ErrorAction Stop
  Import-Module -Name ($PSHOME + '\Modules\Microsoft.PowerShell.Management\Microsoft.PowerShell.Management.psd1') -ErrorAction Stop
  ARCHIVE_BEFORE
  $ExecutionContext.SessionState.LanguageMode = 'ConstrainedLanguage'
  Import-Module -Name ($PSHOME + '\Modules\Microsoft.PowerShell.Archive\Microsoft.PowerShell.Archive.psd1') -ErrorAction Stop
  'ARCHIVE_IMPORT_SUCCESS'
} catch {
  'FullyQualifiedErrorId=' + $_.FullyQualifiedErrorId
  'ScriptStackTrace=' + $_.ScriptStackTrace
  'PositionMessage=' + $_.InvocationInfo.PositionMessage
  exit 7
}
"""
IMPORT = r"Import-Module -Name ($PSHOME + '\Modules\Microsoft.PowerShell.Archive\Microsoft.PowerShell.Archive.psd1') -ErrorAction Stop"
def main():
    if sys.platform != 'win32':
        raise SystemExit('Windows diagnostic only.')
    lines = []
    for shell in ['powershell.exe', 'pwsh.exe']:
        for preloaded in [False, True]:
            path = shutil.which(shell)
            if path is None:
                lines.append(json.dumps({'shell': shell, 'preload_archive': preloaded, 'category': 'missing'}))
                continue
            process = subprocess.Popen(
                [path, '-NoProfile', '-NonInteractive', '-Command',
                 BASE.replace('ARCHIVE_BEFORE', IMPORT if preloaded else '')],
                stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                creationflags=subprocess.CREATE_NO_WINDOW,
            )
            category = 'completed'
            try:
                stdout, stderr = process.communicate(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                stdout, stderr = process.communicate()
                category = 'timeout'
            lines.append(json.dumps({'shell': shell, 'preload_archive': preloaded,
                                     'category': category, 'exit': process.returncode,
                                     'native_stderr_present': bool(stderr)}))
            lines.extend(stdout[:16384].decode('utf-8', errors='replace').splitlines())
    text = '\n'.join(lines) + '\n'
    sys.stdout.write(text)
    with open(os.path.join(os.environ.get('RUNNER_TEMP', os.getcwd()), 'acl-module-modes.log'),
              'w', encoding='utf-8') as handle:
        handle.write(text)

def reader_main():
    import tempfile
    import pathlib
    directory = pathlib.Path(__file__).resolve().parents[2]
    history = (directory / 'tests/local_worker_acl_history.rs').read_text(encoding='utf-8')
    marker = "r#\"$ErrorActionPreference = 'Stop'\nfunction Read-AclSummary"
    a = history.index(marker) + len('r#"')
    reader = history[a:history.index('"#', a)]
    library = (directory / 'src/local-workers/private_directory.rs').read_text(encoding='utf-8')
    marker = 'pub const WINDOWS_ACL_PROTECT: &str = r#"'
    a = library.index(marker) + len(marker)
    acl = library[a:library.index('"#;', a)]
    bootstrap = r"""
Import-Module -Name ($PSHOME + '\Modules\Microsoft.PowerShell.Utility\Microsoft.PowerShell.Utility.psd1') -ErrorAction Stop
Import-Module -Name ($PSHOME + '\Modules\Microsoft.PowerShell.Security\Microsoft.PowerShell.Security.psd1') -ErrorAction Stop
"""
    header = r"""
[IO.File]::WriteAllText($env:ACL_TEST_STAGE_TRACE, ('entry 0' + [Environment]::NewLine))
$aclWatch = [Diagnostics.Stopwatch]::StartNew()
function Trace-ReaderStage([string]$stage) {
  [IO.File]::AppendAllText($env:ACL_TEST_STAGE_TRACE, $stage + ' ' + $aclWatch.ElapsedMilliseconds + [Environment]::NewLine)
}
"""
    before = '  $acl = Get-Acl -LiteralPath $target'
    assert reader.count(before) == 1
    reader = reader.replace(before, "Trace-ReaderStage 'before-getacl'\n" + before + "\nTrace-ReaderStage 'getacl-ready'")
    before = '[Console]::Out.Write((ConvertTo-Json -InputObject $result -Depth 6 -Compress))'
    assert reader.count(before) == 1
    reader = reader.replace(before, "Trace-ReaderStage 'before-json'\n" + before + "\nTrace-ReaderStage 'json-ready'")
    lines = []
    with tempfile.TemporaryDirectory(prefix='pilot-acl-reader-probe-') as raw:
        root = pathlib.Path(raw)
        target = root / '实例 目录'
        token = target / 'credentials' / 'bootstrap-token'
        token.parent.mkdir(parents=True)
        token.write_text('private-fixture-token', encoding='utf-8')
        env = os.environ.copy()
        env.update({'LOONGSUITE_WORKER_ACL_TARGET':str(target),
                    'ACL_TEST_TARGET':str(target),'ACL_TEST_TOKEN':str(token)})
        setup = subprocess.Popen(['powershell.exe','-NoProfile','-NonInteractive','-Command',acl],
                    env=env,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE,
                    creationflags=subprocess.CREATE_NO_WINDOW)
        try:
            setup_stdout, setup_stderr = setup.communicate(timeout=15)
            lines.append(json.dumps({'case':'production-prepare','exit':setup.returncode}))
        except subprocess.TimeoutExpired:
            setup.kill()
            setup.communicate()
            lines.append(json.dumps({'case':'production-prepare','category':'timeout'}))
        for preloaded in [False, True]:
            label = 'explicit-builtins' if preloaded else 'original-summary'
            trace = root / (label + '.trace')
            env['ACL_TEST_STAGE_TRACE'] = str(trace)
            script = header + (bootstrap if preloaded else '') + reader
            # Same default console flags as the existing Rust summary fixture.
            child = subprocess.Popen(['powershell.exe','-NoProfile','-NonInteractive','-Command',script],
                       env=env,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            category = 'completed'
            try:
                stdout, stderr = child.communicate(timeout=30)
            except subprocess.TimeoutExpired:
                child.kill()
                stdout, stderr = child.communicate()
                category = 'timeout'
            lines.append(json.dumps({'case':label,'category':category,'exit':child.returncode,
                                     'stdout_present':bool(stdout),'stderr_present':bool(stderr)}))
            if trace.exists():
                for line in trace.read_text(encoding='utf-8').splitlines():
                    parts = line.split(' ')
                    if len(parts)==2 and parts[0] in ['entry','before-getacl','getacl-ready','before-json','json-ready'] and parts[1].isdigit():
                        lines.append('reader-probe case='+label+' stage='+parts[0]+' elapsed_ms='+parts[1])
    text = '\n'.join(lines)+'\n'
    sys.stdout.write(text)
    with open(os.path.join(os.environ.get('RUNNER_TEMP',os.getcwd()),'acl-reader-modes.log'),
              'w',encoding='utf-8') as handle:
        handle.write(text)

if __name__ == '__main__':
    if '--reader' in sys.argv:
        reader_main()
    else:
        main()
