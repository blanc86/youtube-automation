$ErrorActionPreference = "Stop"
$py = ".\.venv\Scripts\python.exe"

Write-Host "== ruff ==" -ForegroundColor Cyan
& $py -m ruff check src tests
& $py -m ruff format --check src tests

Write-Host "== mypy ==" -ForegroundColor Cyan
& $py -m mypy

Write-Host "== import-linter ==" -ForegroundColor Cyan
& ".\.venv\Scripts\lint-imports.exe"
if ($LASTEXITCODE -ne 0) { throw "import-linter contracts violated" }

Write-Host "== pytest ==" -ForegroundColor Cyan
& $py -m pytest

Write-Host "ALL CHECKS PASSED" -ForegroundColor Green
