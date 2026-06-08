# ==========================================
# IKBR TELEMETRY DASHBOARD STOP SCRIPT
# ==========================================

$ErrorActionPreference = "Stop"

$dashboardProcesses = Get-CimInstance Win32_Process |
    Where-Object {
        $_.CommandLine -and
        $_.CommandLine -match "telemetry_dashboard\.py" -and
        $_.CommandLine -notmatch "scalpingbot\.py"
    }

if (-not $dashboardProcesses -or $dashboardProcesses.Count -eq 0) {
    Write-Host "No telemetry dashboard processes found."
    exit 0
}

foreach ($process in $dashboardProcesses) {
    Write-Host "Stopping telemetry dashboard PID $($process.ProcessId)"
    Stop-Process -Id $process.ProcessId -Force -ErrorAction Stop
}

Write-Host "Telemetry dashboard stopped."
