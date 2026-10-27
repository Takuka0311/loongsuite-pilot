$ErrorActionPreference = 'Stop'
$EmptyResult = ''
$NativeArgs = @('hook-process', 'qoder-auto')
$NativeArgs = @('interceptor', 'hook', '--agent', 'qoder-auto') + $args
try {
    . (Join-Path $PSScriptRoot 'shared\native.ps1')
    Invoke-PilotNative -HookDir $PSScriptRoot -Arguments $NativeArgs -EmptyResult $EmptyResult
} catch {
    Write-Error '[loongsuite-pilot] native hook bootstrap failed' -ErrorAction Continue
    if ($EmptyResult) { Write-Output $EmptyResult }
}
exit 0
