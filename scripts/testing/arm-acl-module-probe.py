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
if __name__ == '__main__':
    main()
