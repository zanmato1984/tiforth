#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  scripts/refresh-first-tikv-live-artifacts.sh [--write-artifacts] [--dry-run]

Modes:
  default            run live TiKV refresh scripts for first-union, first-decimal128, first-temporal-date32,
                     and first-temporal-timestamp-tz; print artifacts without rewriting checked-in files
  --write-artifacts  run the same sequence and overwrite checked-in TiKV artifacts after successful runs
  --dry-run          print the per-slice commands that would run; skip env checks and execution

Runs in order:
  1. scripts/refresh-first-union-tikv-live-artifacts.sh
  2. scripts/refresh-first-decimal128-tikv-live-artifacts.sh
  3. scripts/refresh-first-temporal-date32-tikv-live-artifacts.sh
  4. scripts/refresh-first-temporal-timestamp-tz-tikv-live-artifacts.sh

Required env vars (per prefix):
  TIFORTH_TIDB_MYSQL_HOST
  TIFORTH_TIDB_MYSQL_PORT
  TIFORTH_TIDB_MYSQL_USER
  TIFORTH_TIDB_MYSQL_DATABASE
  TIFORTH_TIFLASH_MYSQL_HOST
  TIFORTH_TIFLASH_MYSQL_PORT
  TIFORTH_TIFLASH_MYSQL_USER
  TIFORTH_TIFLASH_MYSQL_DATABASE
  TIFORTH_TIKV_MYSQL_HOST
  TIFORTH_TIKV_MYSQL_PORT
  TIFORTH_TIKV_MYSQL_USER
  TIFORTH_TIKV_MYSQL_DATABASE

Optional env vars:
  TIFORTH_TIDB_MYSQL_PASSWORD
  TIFORTH_TIDB_MYSQL_BIN
  TIFORTH_TIFLASH_MYSQL_PASSWORD
  TIFORTH_TIFLASH_MYSQL_BIN
  TIFORTH_TIKV_MYSQL_PASSWORD
  TIFORTH_TIKV_MYSQL_BIN
USAGE
}

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
# shellcheck source=scripts/lib/tikv-live-refresh-common.sh
source "$repo_root/scripts/lib/tikv-live-refresh-common.sh"

if tiforth_live_refresh_parse_mode_flags "$@"; then
  :
else
  parse_status=$?
  if [[ $parse_status -eq 10 ]]; then
    exit 0
  fi
  exit "$parse_status"
fi

write_artifacts=$TIFORTH_LIVE_REFRESH_WRITE_ARTIFACTS
dry_run=$TIFORTH_LIVE_REFRESH_DRY_RUN
cd "$repo_root"

if [[ $dry_run -ne 1 ]]; then
  tiforth_live_refresh_require_mysql_env "consolidated TiKV live artifact refresh"
fi

refresh_scripts=(
  scripts/refresh-first-union-tikv-live-artifacts.sh
  scripts/refresh-first-decimal128-tikv-live-artifacts.sh
  scripts/refresh-first-temporal-date32-tikv-live-artifacts.sh
  scripts/refresh-first-temporal-timestamp-tz-tikv-live-artifacts.sh
)

for refresh_script in "${refresh_scripts[@]}"; do
  if [[ ! -f "$refresh_script" ]]; then
    echo "Required refresh script not found: $refresh_script" >&2
    exit 1
  fi
done

refresh_args=()
if [[ $write_artifacts -eq 1 ]]; then
  refresh_args+=(--write-artifacts)
fi
if [[ $dry_run -eq 1 ]]; then
  refresh_args+=(--dry-run)
fi

for refresh_script in "${refresh_scripts[@]}"; do
  if [[ ${#refresh_args[@]} -eq 0 ]]; then
    echo "Running $refresh_script"
  else
    echo "Running $refresh_script ${refresh_args[*]}"
  fi
  bash "$refresh_script" "${refresh_args[@]}"
done
