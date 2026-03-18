#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/cleanup-finished-worktree.sh <issue-worktree> [<main-worktree>] [<remote>] [<base-branch>]

Examples:
  scripts/cleanup-finished-worktree.sh .worktrees/issue-100
  scripts/cleanup-finished-worktree.sh .worktrees/issue-100 /home/ubuntu/dev/tiforth origin main

This helper:
- checks that the issue worktree is clean
- finds or uses the repository's main worktree
- fetches and fast-forwards the local base branch
- verifies the issue branch is merged
- removes the issue worktree
- deletes the merged local branch
- prunes stale worktree metadata
EOF
}

canonical_path() {
  local path=$1
  (cd "$path" >/dev/null 2>&1 && pwd)
}

detect_main_worktree() {
  local probe_worktree=$1
  local base_branch=$2

  git -C "$probe_worktree" worktree list --porcelain | awk -v target="refs/heads/$base_branch" '
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

if [[ ${1:-} == "-h" || ${1:-} == "--help" ]]; then
  usage
  exit 0
fi

issue_worktree_input=${1:-}
main_worktree_input=${2:-}
remote=${3:-origin}
base_branch=${4:-main}

if [[ -z "$issue_worktree_input" ]]; then
  usage >&2
  exit 2
fi

issue_worktree=$(canonical_path "$issue_worktree_input") || {
  echo "Unable to resolve issue worktree: $issue_worktree_input" >&2
  exit 1
}

if [[ -n "$main_worktree_input" ]]; then
  main_worktree=$(canonical_path "$main_worktree_input") || {
    echo "Unable to resolve main worktree: $main_worktree_input" >&2
    exit 1
  }
else
  main_worktree=$(detect_main_worktree "$issue_worktree" "$base_branch")
fi

if [[ -z "$main_worktree" ]]; then
  echo "Unable to detect the $base_branch worktree; pass it explicitly as the second argument" >&2
  exit 1
fi

if [[ "$issue_worktree" == "$main_worktree" ]]; then
  echo "Issue worktree and main worktree must be different paths" >&2
  exit 1
fi

issue_branch=$(git -C "$issue_worktree" branch --show-current)

if [[ -z "$issue_branch" ]]; then
  echo "Unable to detect the issue branch for $issue_worktree" >&2
  exit 1
fi

if [[ -n $(git -C "$issue_worktree" status --porcelain) ]]; then
  echo "Issue worktree has uncommitted changes: $issue_worktree" >&2
  exit 1
fi

if [[ -n $(git -C "$main_worktree" status --porcelain) ]]; then
  echo "Main worktree has uncommitted changes: $main_worktree" >&2
  exit 1
fi

if [[ $(git -C "$main_worktree" branch --show-current) != "$base_branch" ]]; then
  git -C "$main_worktree" switch "$base_branch"
fi

git -C "$main_worktree" fetch "$remote"
git -C "$main_worktree" pull --ff-only "$remote" "$base_branch"

if ! git -C "$main_worktree" merge-base --is-ancestor "$issue_branch" "$remote/$base_branch"; then
  echo "Branch $issue_branch is not merged into $remote/$base_branch" >&2
  exit 1
fi

cd "$main_worktree"
git worktree remove "$issue_worktree"
git branch -d "$issue_branch"
git worktree prune
git status --short --branch
