#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  scripts/setup-rust-toolchain.sh [<toolchain>]

Examples:
  scripts/setup-rust-toolchain.sh
  scripts/setup-rust-toolchain.sh stable

This helper:
- installs rustup if it is missing
- installs the requested Rust toolchain (default: stable)
- ensures the rustfmt component is present
USAGE
}

if [[ ${1:-} == "-h" || ${1:-} == "--help" ]]; then
  usage
  exit 0
fi

if [[ $# -gt 1 ]]; then
  usage >&2
  exit 2
fi

toolchain=${1:-stable}

if [[ ! "$toolchain" =~ ^[A-Za-z0-9._-]+$ ]]; then
  echo "Invalid toolchain value: $toolchain" >&2
  exit 1
fi

if ! command -v rustup >/dev/null 2>&1; then
  if ! command -v curl >/dev/null 2>&1; then
    echo "rustup is missing and curl is not installed; install curl first" >&2
    exit 1
  fi

  tmp_dir=$(mktemp -d)
  installer="$tmp_dir/rustup-init.sh"
  trap 'rm -rf "$tmp_dir"' EXIT

  curl --proto '=https' --tlsv1.2 -fsSL https://sh.rustup.rs -o "$installer"
  sh "$installer" -y --profile minimal --default-toolchain "$toolchain"
fi

if [[ -f "$HOME/.cargo/env" ]]; then
  # shellcheck disable=SC1090
  source "$HOME/.cargo/env"
fi

if ! command -v rustup >/dev/null 2>&1; then
  echo "rustup is still unavailable after installation" >&2
  exit 1
fi

rustup toolchain install "$toolchain" --profile minimal
rustup component add rustfmt --toolchain "$toolchain"

echo "Rust toolchain ready: $toolchain"
echo "Next commands:"
echo "  cargo fmt --all -- --check"
echo "  cargo test --workspace"
