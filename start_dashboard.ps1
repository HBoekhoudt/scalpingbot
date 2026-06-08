# ==========================================
# IKBR TELEMETRY DASHBOARD START SCRIPT
# ==========================================

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$dashboardPath = Join-Path $scriptDir "telemetry_dashboard.py"
$runtimeDir = Join-Path $scriptDir "runtime_state"
$logPath = Join-Path $runtimeDir "dashboard.log"

if (-not (Test-Path -LiteralPath $dashboardPath)) {
    Write-Error "telemetry_dashboard.py not found at $dashboardPath"
    exit 2
}

New-Item -ItemType Directory -Force -Path $runtimeDir | Out-Null

$hostValue = if ($env:IKBR_DASHBOARD_HOST) { $env:IKBR_DASHBOARD_HOST } else { "127.0.0.1" }
$portValue = if ($env:IKBR_DASHBOARD_PORT) { $env:IKBR_DASHBOARD_PORT } else { "8088" }
$pythonExe = if ($env:PYTHON_EXE) { $env:PYTHON_EXE } else { "python" }

$command = @"
Set-Location -LiteralPath '$scriptDir'
& '$pythonExe' -u '$dashboardPath' *>> '$logPath'
"@

$process = Start-Process powershell `
    -ArgumentList "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", $command `
    -WorkingDirectory $scriptDir `
    -WindowStyle Hidden `
    -PassThru

Write-Host "Telemetry dashboard started."
Write-Host "PID: $($process.Id)"
Write-Host "URL: http://$hostValue`:$portValue/"
Write-Host "Log: $logPath"
if ($env:IKBR_DASHBOARD_TOKEN) {
    Write-Host "Token authentication is enabled. Token value is not printed."
}
