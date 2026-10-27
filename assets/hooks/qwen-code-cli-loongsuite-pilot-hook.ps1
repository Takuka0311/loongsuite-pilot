$ErrorActionPreference = 'Stop'
$EmptyResult = '{}'
$NativeArgs = @('hook-process', 'qwen-code-cli')
$Event = if ($args.Count -gt 0) { $args[0] } else { 'unknown' }
if ($Event -notin @('stop', 'subagent-start', 'subagent-stop')) { Write-Output $EmptyResult; exit 0 }
$NativeArgs += $Event
try {
    . (Join-Path $PSScriptRoot 'shared\native.ps1')
    Invoke-PilotNative -HookDir $PSScriptRoot -Arguments $NativeArgs -EmptyResult $EmptyResult
} catch {
    Write-Error '[loongsuite-pilot] native hook bootstrap failed' -ErrorAction Continue
    if ($EmptyResult) { Write-Output $EmptyResult }
}
exit 0
