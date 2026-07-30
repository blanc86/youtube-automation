# NOTE: $ErrorActionPreference does NOT apply to native executable exit codes
# in Windows PowerShell 5.1, so every step below needs its own explicit
# $LASTEXITCODE check. Without them this script reports success while the
# tools underneath it are failing.
$ErrorActionPreference = "Stop"
$py = ".\.venv\Scripts\python.exe"

Write-Host "== ruff check ==" -ForegroundColor Cyan
& $py -m ruff check src tests
if ($LASTEXITCODE -ne 0) { Write-Host "ruff check FAILED" -ForegroundColor Red; exit 1 }

Write-Host "== ruff format ==" -ForegroundColor Cyan
& $py -m ruff format --check src tests
if ($LASTEXITCODE -ne 0) { Write-Host "ruff format FAILED" -ForegroundColor Red; exit 1 }

Write-Host "== mypy ==" -ForegroundColor Cyan
& $py -m mypy
if ($LASTEXITCODE -ne 0) { Write-Host "mypy FAILED" -ForegroundColor Red; exit 1 }

Write-Host "== import-linter ==" -ForegroundColor Cyan
& ".\.venv\Scripts\lint-imports.exe"
if ($LASTEXITCODE -ne 0) { Write-Host "import-linter FAILED" -ForegroundColor Red; exit 1 }

Write-Host "== pytest ==" -ForegroundColor Cyan
& $py -m pytest
if ($LASTEXITCODE -ne 0) { Write-Host "pytest FAILED" -ForegroundColor Red; exit 1 }

Write-Host "ALL CHECKS PASSED" -ForegroundColor Green
