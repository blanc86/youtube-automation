# Thin wrapper. The gate itself lives in scripts/check.py so that Windows,
# macOS and Linux run byte-identical checks and cannot drift apart - which is
# exactly what happened when CI listed the steps separately and silently lost
# `ruff format --check`.
$ErrorActionPreference = "Stop"

$root = Join-Path $PSScriptRoot ".."
$py = Join-Path $root ".venv\Scripts\python.exe"
if (-not (Test-Path $py)) {
    $py = (Get-Command python -ErrorAction Stop).Source
}

& $py (Join-Path $PSScriptRoot "check.py")
exit $LASTEXITCODE
