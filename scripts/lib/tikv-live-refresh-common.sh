#!/usr/bin/env bash

# Parse optional --write-artifacts flag shared by TiKV live-refresh scripts.
# Sets TIFORTH_LIVE_REFRESH_WRITE_ARTIFACTS=0 (inspect mode) or 1 (write mode).
# Returns:
#   0  parse success
#   2  invalid arguments
#  10  help rendered
TIFORTH_LIVE_REFRESH_WRITE_ARTIFACTS=0

tiforth_live_refresh_parse_write_artifacts_flag() {
  if [[ ${1:-} == "-h" || ${1:-} == "--help" ]]; then
    usage
    return 10
  fi

  local write_artifacts=0
  if [[ $# -gt 1 ]]; then
    usage >&2
    return 2
  elif [[ $# -eq 1 ]]; then
    if [[ $1 != "--write-artifacts" ]]; then
      echo "Unknown argument: $1" >&2
      usage >&2
      return 2
    fi
    write_artifacts=1
  fi

  TIFORTH_LIVE_REFRESH_WRITE_ARTIFACTS=$write_artifacts
}

# Validate shared TiDB/TiFlash/TiKV MySQL env vars used by TiKV live-refresh scripts.
tiforth_live_refresh_require_mysql_env() {
  local context=$1
  local required_suffixes=(HOST PORT USER DATABASE)
  local prefixes=(TIFORTH_TIDB_MYSQL TIFORTH_TIFLASH_MYSQL TIFORTH_TIKV_MYSQL)
  local missing=()

  for prefix in "${prefixes[@]}"; do
    for suffix in "${required_suffixes[@]}"; do
      local key="${prefix}_${suffix}"
      if [[ -z "${!key:-}" ]]; then
        missing+=("$key")
      fi
    done
  done

  if [[ ${#missing[@]} -gt 0 ]]; then
    echo "Missing required env vars for ${context}:" >&2
    printf '  - %s\n' "${missing[@]}" >&2
    echo "Set them and rerun this script." >&2
    return 1
  fi
}
