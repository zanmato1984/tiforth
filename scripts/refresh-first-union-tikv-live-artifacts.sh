#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  scripts/refresh-first-union-tikv-live-artifacts.sh [--write-artifacts] [--dry-run]

Modes:
  default            run live first-union TiKV execution and print artifacts
  --write-artifacts  overwrite checked-in first-union TiKV artifacts after a successful run
  --dry-run          print the cargo command that would run; skip env checks and execution

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

command=(cargo run -p tiforth-harness-differential --bin first_union_slice_tikv_live --)
if [[ $write_artifacts -eq 1 ]]; then
  command+=(--write-artifacts)
fi

if [[ $dry_run -eq 1 ]]; then
  tiforth_live_refresh_print_command "${command[@]}"
  exit 0
fi

tiforth_live_refresh_require_mysql_env "first-union TiKV live artifact refresh"
"${command[@]}"
