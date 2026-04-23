#!/usr/bin/env bash
# Bootstrap the repo with uv.
#
# - Installs the Python version declared in .python-version (via uv's
#   managed Python) if missing.
# - Creates .venv at the repo root and installs every workspace member
#   (common, apps/*) plus the dev dependency group (ruff, pytest,
#   commitizen, httpx) into it.
# - Wires up .githooks.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

if ! command -v uv >/dev/null 2>&1; then
  echo "uv is required but not installed. See: https://docs.astral.sh/uv/getting-started/installation/" >&2
  exit 1
fi

PYTHON_VERSION="$(cat "$ROOT_DIR/.python-version")"
uv python install "$PYTHON_VERSION"

echo "Syncing workspace (.venv) with all members and dev group..."
uv sync --all-packages --all-groups

chmod +x .githooks/pre-commit .githooks/pre-push
git config core.hooksPath .githooks

echo
echo "Bootstrap complete. Activate with: source .venv/bin/activate"
