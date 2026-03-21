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

if [[ ${1:-} == "-h" || ${1:-} == "--help" ]]; then
  usage
  exit 0
fi

write_artifacts=0
if [[ $# -gt 1 ]]; then
  usage >&2
  exit 2
elif [[ $# -eq 1 ]]; then
  if [[ $1 != "--write-artifacts" ]]; then
    echo "Unknown argument: $1" >&2
    usage >&2
    exit 2
  fi
  write_artifacts=1
fi

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

required_suffixes=(HOST PORT USER DATABASE)
prefixes=(TIFORTH_TIDB_MYSQL TIFORTH_TIFLASH_MYSQL TIFORTH_TIKV_MYSQL)
missing=()

for prefix in "${prefixes[@]}"; do
  for suffix in "${required_suffixes[@]}"; do
    key="${prefix}_${suffix}"
    if [[ -z "${!key:-}" ]]; then
      missing+=("$key")
    fi
  done
done

if [[ ${#missing[@]} -gt 0 ]]; then
  echo "Missing required env vars for first-temporal-date32 TiKV live artifact refresh:" >&2
  printf '  - %s\n' "${missing[@]}" >&2
  echo "Set them and rerun this script." >&2
  exit 1
fi

command=(cargo run -p tiforth-harness-differential --bin first_temporal_date32_slice_tikv_live --)
if [[ $write_artifacts -eq 1 ]]; then
  command+=(--write-artifacts)
fi

"${command[@]}"
