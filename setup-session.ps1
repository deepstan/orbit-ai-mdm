# Run this ONCE at the start of every new PowerShell window/tab, from your
# project folder, with:   . .\setup-session.ps1
# (the leading ". " -- "dot sourcing" -- is required so the environment
# variables it sets actually apply to YOUR current shell, not a subshell
# that disappears when the script ends)

# --- 1. Add Postgres to PATH -------------------------------------------
$pgRoot = "C:\Program Files\PostgreSQL"
if (Test-Path $pgRoot) {
    $pgVersion = (Get-ChildItem $pgRoot | Sort-Object Name -Descending | Select-Object -First 1).Name
    $pgBin = "$pgRoot\$pgVersion\bin"
    if ($env:Path -notlike "*$pgBin*") {
        $env:Path += ";$pgBin"
    }
    Write-Host "Postgres $pgVersion added to PATH ($pgBin)" -ForegroundColor Green
} else {
    Write-Host "Postgres not found at $pgRoot -- skipping (edit this script if it's installed elsewhere)" -ForegroundColor Yellow
}

# --- 2. Activate the venv, if one exists here ---------------------------
if (Test-Path ".\venv\Scripts\Activate.ps1") {
    . .\venv\Scripts\Activate.ps1
    Write-Host "Virtual environment activated" -ForegroundColor Green
} else {
    Write-Host "No .\venv found -- create one with: python -m venv venv" -ForegroundColor Yellow
}

# --- 3. Set DATABASE_URL -------------------------------------------------
# EDIT THIS LINE with your real password, then save the file --
# it'll be set automatically every time you run this script.
$env:DATABASE_URL = "postgresql://postgres:Abcd1234@localhost:5432/mdm_dev"
Write-Host "DATABASE_URL set" -ForegroundColor Green

# --- 4. Quick sanity check ------------------------------------------------
Write-Host ""
Write-Host "Session ready. Quick checks:" -ForegroundColor Cyan
Write-Host "  psql --version"
try { psql --version } catch { Write-Host "  (psql still not found -- check the path in step 1)" -ForegroundColor Red }
Write-Host "  python --version"
try { python --version } catch { Write-Host "  (python not found)" -ForegroundColor Red }
