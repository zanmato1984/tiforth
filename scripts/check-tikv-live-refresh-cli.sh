#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  scripts/check-tikv-live-refresh-cli.sh

Runs CLI regression checks for TiKV live refresh scripts without requiring live env vars.

Checks per script:
  - bash syntax (`bash -n`)
  - `--help` exits 0
  - `--bad-flag` exits 2
  - no args exits 1 when env vars are missing
  - `--dry-run` exits 0 and prints `DRY-RUN:`
  - `--dry-run --write-artifacts` exits 0 and includes `--write-artifacts`
  - duplicate dry-run flag exits 2
USAGE
}

if [[ ${1:-} == "-h" || ${1:-} == "--help" ]]; then
  usage
  exit 0
fi

if [[ $# -ne 0 ]]; then
  echo "Unknown argument: $1" >&2
  usage >&2
  exit 2
fi

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

scripts=(
  scripts/refresh-first-union-tikv-live-artifacts.sh
  scripts/refresh-first-decimal128-tikv-live-artifacts.sh
  scripts/refresh-first-temporal-date32-tikv-live-artifacts.sh
  scripts/refresh-first-temporal-timestamp-tz-tikv-live-artifacts.sh
  scripts/refresh-first-tikv-live-artifacts.sh
)

tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/tiforth-refresh-cli-check.XXXXXX")
cleanup() {
  rm -rf "$tmp_dir"
}
trap cleanup EXIT

for script in "${scripts[@]}"; do
  if [[ ! -f "$script" ]]; then
    echo "Missing script under test: $script" >&2
    exit 1
  fi

  bash -n "$script"
  "$script" --help >"$tmp_dir/help.txt"

  set +e
  "$script" --bad-flag >"$tmp_dir/bad.txt" 2>&1
  bad_status=$?
  "$script" >"$tmp_dir/missing.txt" 2>&1
  missing_status=$?
  "$script" --dry-run >"$tmp_dir/dry.txt" 2>&1
  dry_status=$?
  "$script" --dry-run --write-artifacts >"$tmp_dir/dry_write.txt" 2>&1
  dry_write_status=$?
  "$script" --dry-run --dry-run >"$tmp_dir/dup.txt" 2>&1
  dup_status=$?
  set -e

  printf '%s bad=%s missing=%s dry=%s dry+write=%s dup=%s\n' \
    "$script" "$bad_status" "$missing_status" "$dry_status" "$dry_write_status" "$dup_status"

  if [[ $bad_status -ne 2 || $missing_status -ne 1 || $dry_status -ne 0 || $dry_write_status -ne 0 || $dup_status -ne 2 ]]; then
    echo "Unexpected status matrix for $script" >&2
    exit 1
  fi

  if ! rg -q "DRY-RUN:" "$tmp_dir/dry.txt"; then
    echo "Missing DRY-RUN marker for $script" >&2
    exit 1
  fi

  if ! rg -q -- "--write-artifacts" "$tmp_dir/dry_write.txt"; then
    echo "Missing --write-artifacts marker for $script dry-run+write output" >&2
    exit 1
  fi
done

bash -n scripts/lib/tikv-live-refresh-common.sh

echo "TiKV live refresh CLI checks passed for ${#scripts[@]} script(s)."
