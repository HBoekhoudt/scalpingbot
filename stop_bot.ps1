# ==========================================
# IKBR SCALPING BOT STOP SCRIPT
# ==========================================

Write-Host ""
Write-Host "Stopping IKBR Scalping Bot..."
Write-Host ""

# Stop uvicorn
Get-Process -Name uvicorn -ErrorAction SilentlyContinue | Stop-Process -Force

# Stop ngrok
Get-Process -Name ngrok -ErrorAction SilentlyContinue | Stop-Process -Force

# Stop python processes used by bot
Get-Process -Name python -ErrorAction SilentlyContinue | Stop-Process -Force

Write-Host ""
Write-Host "Bot stopped."
Write-Host ""