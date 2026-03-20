#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  scripts/start-issue-worktree.sh <issue-number> [<branch>] [<worktree-path>] [<remote>] [<base-branch>]

Examples:
  scripts/start-issue-worktree.sh 165
  scripts/start-issue-worktree.sh 165 issue-165-start-worktree-helper ../tiforth-issue-165
  scripts/start-issue-worktree.sh #165 issue-165-start-worktree-helper /home/ubuntu/dev/tiforth-issue-165 origin main

This helper:
- finds or uses the repository's base worktree
- checks that the base worktree is clean
- verifies the requested GitHub issue is open (unless `TIFORTH_SKIP_ISSUE_CHECK=1`)
- fast-forwards the base branch
- creates a new issue branch from the base branch
- creates the dedicated issue worktree for that branch
- sets and verifies local `format.signoff=true` in the issue worktree
USAGE
}

detect_base_worktree() {
  local base_branch=$1

  git worktree list --porcelain | awk -v target="refs/heads/$base_branch" '
    /^worktree / { path=$2; branch="" }
    /^branch / { branch=$2 }
    /^$/ {
      if (branch == target) {
        print path
        found=1
        exit 0
      }
    }
    END {
      if (!found && branch == target) {
        print path
      }
    }
  '
}

verify_issue_is_open() {
  local issue_number=$1

  if [[ ${TIFORTH_SKIP_ISSUE_CHECK:-0} == "1" ]]; then
    return 0
  fi

  if ! command -v gh >/dev/null 2>&1; then
    echo "Cannot verify issue #$issue_number because \`gh\` is not installed." >&2
    echo "Install GitHub CLI or rerun with TIFORTH_SKIP_ISSUE_CHECK=1 to bypass verification." >&2
    exit 1
  fi

  local issue_state
  if ! issue_state=$(gh issue view "$issue_number" --json state --jq '.state' 2>/dev/null); then
    echo "Unable to load issue #$issue_number with \`gh issue view\`." >&2
    echo "Ensure the issue exists and GitHub auth/network are available, or rerun with TIFORTH_SKIP_ISSUE_CHECK=1." >&2
    exit 1
  fi

  if [[ "$issue_state" != "OPEN" ]]; then
    echo "Issue #$issue_number is $issue_state; start-issue-worktree expects an open issue." >&2
    exit 1
  fi
}

if [[ ${1:-} == "-h" || ${1:-} == "--help" ]]; then
  usage
  exit 0
fi

if [[ $# -gt 5 ]]; then
  usage >&2
  exit 2
fi

issue_input=${1:-}
branch_input=${2:-}
worktree_input=${3:-}
remote=${4:-origin}
base_branch=${5:-main}

if [[ -z "$issue_input" ]]; then
  usage >&2
  exit 2
fi

issue_number=${issue_input#\#}
if [[ ! "$issue_number" =~ ^[0-9]+$ ]]; then
  echo "Issue number must be numeric (examples: 165 or #165): $issue_input" >&2
  exit 1
fi

verify_issue_is_open "$issue_number"

base_worktree=$(detect_base_worktree "$base_branch")
if [[ -z "$base_worktree" ]]; then
  echo "Unable to detect the $base_branch worktree; pass a base worktree by running from a repository that has one" >&2
  exit 1
fi

if [[ -n $(git -C "$base_worktree" status --porcelain) ]]; then
  echo "Base worktree has uncommitted changes: $base_worktree" >&2
  exit 1
fi

if [[ $(git -C "$base_worktree" branch --show-current) != "$base_branch" ]]; then
  git -C "$base_worktree" switch "$base_branch"
fi

git -C "$base_worktree" fetch "$remote"
git -C "$base_worktree" pull --ff-only "$remote" "$base_branch"

branch=${branch_input:-issue-$issue_number}
if ! git -C "$base_worktree" check-ref-format --branch "$branch" >/dev/null 2>&1; then
  echo "Invalid branch name: $branch" >&2
  exit 1
fi

if git -C "$base_worktree" show-ref --verify --quiet "refs/heads/$branch"; then
  echo "Local branch already exists: $branch" >&2
  exit 1
fi

if git -C "$base_worktree" show-ref --verify --quiet "refs/remotes/$remote/$branch"; then
  echo "Remote branch already exists: $remote/$branch" >&2
  exit 1
fi

if [[ -n "$worktree_input" ]]; then
  if [[ "$worktree_input" = /* ]]; then
    issue_worktree=$worktree_input
  else
    issue_worktree="$(pwd)/$worktree_input"
  fi
else
  repo_name=$(basename "$base_worktree")
  repo_parent=$(cd "$base_worktree/.." && pwd)
  issue_worktree="$repo_parent/$repo_name-issue-$issue_number"
fi

if [[ -e "$issue_worktree" ]]; then
  echo "Issue worktree path already exists: $issue_worktree" >&2
  exit 1
fi

git -C "$base_worktree" worktree add -b "$branch" "$issue_worktree" "$base_branch"

git -C "$issue_worktree" config --local format.signoff true
if [[ $(git -C "$issue_worktree" config --local --get format.signoff) != "true" ]]; then
  echo "Failed to set format.signoff=true in issue worktree: $issue_worktree" >&2
  exit 1
fi

echo
echo "Created issue worktree:"
echo "  issue: #$issue_number"
echo "  branch: $branch"
echo "  path: $issue_worktree"
echo "  format.signoff: true"
echo
echo "Next:"
echo "  cd \"$issue_worktree\""
echo "  git status --short --branch"
