#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
python_bin="${PYTHON_BIN:-${repo_root}/.venv/bin/python}"

if [[ ! -x "${python_bin}" ]]; then
  python_bin="python3"
fi

cd "${repo_root}"
exec "${python_bin}" simulator/producer.py --scenario normal
