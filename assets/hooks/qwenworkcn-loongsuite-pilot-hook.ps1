$ErrorActionPreference = 'Stop'
$EmptyResult = ''
$NativeArgs = @('hook-process', 'qwen-work-cn')
$NativeArgs += @('--agent-id', 'qwen-work-cn', '--log-prefix', 'qwen-work-cn')
try {
    . (Join-Path $PSScriptRoot 'shared\native.ps1')
    Invoke-PilotNative -HookDir $PSScriptRoot -Arguments $NativeArgs -EmptyResult $EmptyResult
} catch {
    Write-Error '[loongsuite-pilot] native hook bootstrap failed' -ErrorAction Continue
    if ($EmptyResult) { Write-Output $EmptyResult }
}
exit 0
