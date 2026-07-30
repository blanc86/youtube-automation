# NOTE: $ErrorActionPreference does NOT apply to native executable exit codes
# in Windows PowerShell 5.1, so every step below needs its own explicit
# $LASTEXITCODE check. Without them this script reports success while the
# tools underneath it are failing.
$ErrorActionPreference = "Stop"

# Run from the repo root regardless of where the caller invoked this from, so
# the relative paths below and pytest's rootdir resolve the same way every time.
Set-Location (Join-Path $PSScriptRoot "..")

# Prefer the project venv; fall back to whatever python is on PATH. CI installs
# into the runner's interpreter and has no .venv, and this script is CI's single
# gate step - hardcoding the venv path would make it fail there.
$py = ".\.venv\Scripts\python.exe"
if (-not (Test-Path $py)) {
    $py = (Get-Command python -ErrorAction Stop).Source
}

Write-Host "== ruff check ==" -ForegroundColor Cyan
& $py -m ruff check src tests
if ($LASTEXITCODE -ne 0) { Write-Host "ruff check FAILED" -ForegroundColor Red; exit 1 }

Write-Host "== ruff format ==" -ForegroundColor Cyan
& $py -m ruff format --check src tests
if ($LASTEXITCODE -ne 0) { Write-Host "ruff format FAILED" -ForegroundColor Red; exit 1 }

Write-Host "== mypy ==" -ForegroundColor Cyan
& $py -m mypy
if ($LASTEXITCODE -ne 0) { Write-Host "mypy FAILED" -ForegroundColor Red; exit 1 }

# NB: `python -m importlinter.cli` is NOT a substitute. That module has no
# __main__ guard, so it imports, does nothing and exits 0 - a silently passing
# gate. It must be the console script.
Write-Host "== import-linter ==" -ForegroundColor Cyan
$lintImports = ".\.venv\Scripts\lint-imports.exe"
if (-not (Test-Path $lintImports)) {
    $lintImports = (Get-Command lint-imports -ErrorAction Stop).Source
}
& $lintImports
if ($LASTEXITCODE -ne 0) { Write-Host "import-linter FAILED" -ForegroundColor Red; exit 1 }

# addopts carries -m "not integration", so this is the hermetic suite only.
Write-Host "== pytest (unit) ==" -ForegroundColor Cyan
& $py -m pytest
if ($LASTEXITCODE -ne 0) { Write-Host "pytest FAILED" -ForegroundColor Red; exit 1 }

# Separate step because these shell out to a real ffmpeg and nvidia-smi. Keeping
# them out of the default run stops the hermetic suite depending on the host,
# while this step keeps them genuinely gated rather than quietly skipped.
Write-Host "== pytest (integration) ==" -ForegroundColor Cyan
& $py -m pytest -m integration
if ($LASTEXITCODE -ne 0) { Write-Host "pytest integration FAILED" -ForegroundColor Red; exit 1 }

Write-Host "ALL CHECKS PASSED" -ForegroundColor Green
