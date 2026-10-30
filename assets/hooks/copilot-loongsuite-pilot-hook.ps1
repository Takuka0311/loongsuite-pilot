$ErrorActionPreference = 'Stop'
$EmptyResult = '{}'
$NativeArgs = @('hook-process', 'copilot')
$Event = if ($args.Count -gt 0) { $args[0] } else { 'unknown' }
if ($Event -notin @('session-start', 'user-prompt-submit', 'user-prompt-submitted', 'pre-tool-use', 'post-tool-use', 'post-tool-use-failure', 'session-end', 'stop', 'agent-stop')) { Write-Output $EmptyResult; exit 0 }
$NativeArgs += $Event
try {
    . (Join-Path $PSScriptRoot 'shared\native.ps1')
    Invoke-PilotNative -HookDir $PSScriptRoot -Arguments $NativeArgs -EmptyResult $EmptyResult
} catch {
    Write-Error '[loongsuite-pilot] native hook bootstrap failed' -ErrorAction Continue
    if ($EmptyResult) { Write-Output $EmptyResult }
}
exit 0
