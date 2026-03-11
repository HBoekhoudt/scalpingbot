# ==========================================
# IKBR SCALPING BOT START SCRIPT
# ==========================================

Write-Host ""
Write-Host "======================================="
Write-Host "Starting IKBR Scalping Bot"
Write-Host "======================================="
Write-Host ""

# -------------------------------------------------
# PATH CONFIG
# -------------------------------------------------

$projectPath = "C:\Users\hboek\OneDrive\Documents\Python\Projects\ikbr_scalpingbot"

$venvActivate = "C:\Users\hboek\OneDrive\Documents\Python\VEnv\OT\VE_OT312\Scripts\Activate.ps1"

$ngrokPath = "C:\ngrok\ngrok.exe"

# -------------------------------------------------
# STOP OLD UVICORN
# -------------------------------------------------

Write-Host "Checking for existing uvicorn processes..."

Get-Process -Name uvicorn -ErrorAction SilentlyContinue | Stop-Process -Force

Start-Sleep -Seconds 1

# -------------------------------------------------
# START UVICORN
# -------------------------------------------------

Write-Host "Starting UVICORN server..."

Start-Process powershell `
-ArgumentList "-NoExit", "-Command", "
cd '$projectPath';
& '$venvActivate';
uvicorn app:app --host 0.0.0.0 --port 8000
"

Start-Sleep -Seconds 3

# -------------------------------------------------
# START NGROK
# -------------------------------------------------

Write-Host "Starting NGROK tunnel..."

Start-Process powershell `
-ArgumentList "-NoExit", "-Command", "
& '$ngrokPath' http 8000
"

Write-Host ""
Write-Host "======================================="
Write-Host "Bot startup complete"
Write-Host "======================================="
Write-Host ""

Write-Host "Next steps:"
Write-Host "1. Start TWS paper trading"
Write-Host "2. Check /health endpoint"
Write-Host "3. Use NGROK URL in TradingView webhook"
Write-Host ""