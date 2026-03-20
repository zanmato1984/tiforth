#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  scripts/check-signoffs.sh [<commit-range>]

Examples:
  scripts/check-signoffs.sh
  scripts/check-signoffs.sh origin/main..HEAD
  scripts/check-signoffs.sh origin/main..feature-branch

This helper:
- checks each commit in the range for a `Signed-off-by:` trailer
- exits non-zero when any commit is missing sign-off

Default range:
- `origin/main..HEAD`
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

range=${1:-origin/main..HEAD}

if ! git rev-list "$range" >/dev/null 2>&1; then
  echo "Invalid commit range: $range" >&2
  exit 1
fi

commits=()
while IFS= read -r commit; do
  commits+=("$commit")
done < <(git rev-list --reverse "$range")

if [[ ${#commits[@]} -eq 0 ]]; then
  echo "No commits in range: $range"
  exit 0
fi

missing_signoff=0

for commit in "${commits[@]}"; do
  if ! git show -s --format=%B "$commit" | grep -Eq '^Signed-off-by:[[:space:]]+'; then
    echo "Missing Signed-off-by trailer: $(git show -s --format='%h %s' "$commit")" >&2
    missing_signoff=1
  fi
done

if [[ $missing_signoff -ne 0 ]]; then
  echo "DCO sign-off check failed for range: $range" >&2
  exit 1
fi

echo "DCO sign-off check passed for range: $range (${#commits[@]} commit(s))."
