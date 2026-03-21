#!/usr/bin/env bash

# Shared mode flags for TiKV live-refresh scripts.
# TIFORTH_LIVE_REFRESH_WRITE_ARTIFACTS=0 (inspect) or 1 (write).
# TIFORTH_LIVE_REFRESH_DRY_RUN=0 (execute) or 1 (print commands only).
# Returns:
#   0  parse success
#   2  invalid arguments
#  10  help rendered
TIFORTH_LIVE_REFRESH_WRITE_ARTIFACTS=0
TIFORTH_LIVE_REFRESH_DRY_RUN=0

tiforth_live_refresh_parse_mode_flags() {
  local write_artifacts=0
  local dry_run=0

  if [[ $# -eq 1 && ( ${1:-} == "-h" || ${1:-} == "--help" ) ]]; then
    usage
    return 10
  fi

  if [[ $# -gt 2 ]]; then
    usage >&2
    return 2
  fi

  local arg
  for arg in "$@"; do
    case "$arg" in
      --write-artifacts)
        if [[ $write_artifacts -eq 1 ]]; then
          echo "Duplicate argument: $arg" >&2
          usage >&2
          return 2
        fi
        write_artifacts=1
        ;;
      --dry-run)
        if [[ $dry_run -eq 1 ]]; then
          echo "Duplicate argument: $arg" >&2
          usage >&2
          return 2
        fi
        dry_run=1
        ;;
      -h|--help)
        echo "Unknown argument: $arg" >&2
        usage >&2
        return 2
        ;;
      *)
        echo "Unknown argument: $arg" >&2
        usage >&2
        return 2
        ;;
    esac
  done

  TIFORTH_LIVE_REFRESH_WRITE_ARTIFACTS=$write_artifacts
  TIFORTH_LIVE_REFRESH_DRY_RUN=$dry_run
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

tiforth_live_refresh_print_command() {
  printf 'DRY-RUN:'
  local arg
  for arg in "$@"; do
    printf ' %q' "$arg"
  done
  printf '\n'
}
