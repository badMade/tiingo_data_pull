#!/usr/bin/env bash
set -euo pipefail

# Determine the repository root from the location of this script.
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Allow overriding the Python binary and venv location through env vars.
PYTHON_BIN="${PYTHON_BIN:-python3.11}"
VENV_DIR="${VENV_DIR:-$REPO_ROOT/.venv}"

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "error: $PYTHON_BIN not found on PATH" >&2
  exit 1
fi

if [ ! -d "$VENV_DIR" ]; then
  echo "Creating virtual environment at $VENV_DIR"
  "$PYTHON_BIN" -m venv "$VENV_DIR"
fi

# shellcheck source=/dev/null
source "$VENV_DIR/bin/activate"

echo "Upgrading pip + build tooling"
python -m pip install --upgrade pip
python -m pip install --upgrade setuptools wheel

echo "Installing tiingo_data_pull in editable mode"
python -m pip install -e "$REPO_ROOT"

echo
echo "Done. Activate the environment with:"
echo "  source \"$VENV_DIR/bin/activate\""
