#!/usr/bin/env bash
# Thin wrapper for macOS and Linux. The gate itself lives in scripts/check.py so
# every platform runs byte-identical checks and cannot drift apart.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [ -x "$root/.venv/bin/python" ]; then
    py="$root/.venv/bin/python"
else
    py="$(command -v python3 || command -v python)"
fi

exec "$py" "$root/scripts/check.py"
