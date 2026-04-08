#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PYTHON_VERSION="$(cat "$ROOT_DIR/.python-version")"

cd "$ROOT_DIR"

version_matches() {
  python3 - "$PYTHON_VERSION" <<'PY'
import re
import sys

expected = sys.argv[1]
current = sys.version.split()[0]
pattern = re.compile(r"^\d+\.\d+\.\d+$")

if not pattern.match(expected):
    raise SystemExit(1)

raise SystemExit(0 if current == expected else 1)
PY
}

if command -v pyenv >/dev/null 2>&1; then
  if pyenv versions --bare | grep -Fxq "$PYTHON_VERSION"; then
    pyenv local "$PYTHON_VERSION"
  elif version_matches; then
    echo "python3 already matches $PYTHON_VERSION; skipping pyenv install"
  else
    echo "Attempting to install Python $PYTHON_VERSION via pyenv..."
    if pyenv install -s "$PYTHON_VERSION"; then
      pyenv local "$PYTHON_VERSION"
    else
      echo "pyenv install failed; continuing with the available python3 interpreter" >&2
    fi
  fi
fi

if ! version_matches; then
  echo "python3 $(python3 --version 2>&1 | awk '{print $2}') does not match required version $PYTHON_VERSION" >&2
  echo "Install the requested version with pyenv or update .python-version before bootstrapping." >&2
  exit 1
fi

python3 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements-dev.txt

for requirements_file in apps/*/requirements.txt; do
  pip install -r "$requirements_file"
done

echo "Bootstrap complete. Activate with: source .venv/bin/activate"
