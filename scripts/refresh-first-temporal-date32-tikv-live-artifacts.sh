#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  scripts/refresh-first-temporal-date32-tikv-live-artifacts.sh [--write-artifacts]

Modes:
  default            run live first-temporal-date32 TiKV execution and print artifacts
  --write-artifacts  overwrite checked-in first-temporal-date32 TiKV artifacts after a successful run

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

if tiforth_live_refresh_parse_write_artifacts_flag "$@"; then
  :
else
  parse_status=$?
  if [[ $parse_status -eq 10 ]]; then
    exit 0
  fi
  exit "$parse_status"
fi

write_artifacts=$TIFORTH_LIVE_REFRESH_WRITE_ARTIFACTS
cd "$repo_root"
tiforth_live_refresh_require_mysql_env "first-temporal-date32 TiKV live artifact refresh"

command=(cargo run -p tiforth-harness-differential --bin first_temporal_date32_slice_tikv_live --)
if [[ $write_artifacts -eq 1 ]]; then
  command+=(--write-artifacts)
fi

"${command[@]}"
