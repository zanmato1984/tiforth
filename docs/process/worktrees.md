# Worktree Rule

Every issue must use its own local git worktree.

This is a project-wide rule, not a preference.

## Why

Running multiple issue-scoped tasks in one directory causes avoidable problems:

- branch confusion
- mixed staged changes
- accidental cross-issue commits
- agent runs stepping on each other
- hard-to-review PRs

`tiforth` expects parallel issue work, so isolation is required.

## Rule

- one issue or PR branch per local worktree
- do not run two active issue tasks in the same directory
- before starting an agent on an issue, create or switch to that issue's dedicated worktree
- stacked PRs may still use separate worktrees even when one branch builds on another

## Recommended Shape

Examples:

- `<repo>-issue-2`
- `<repo>-issue-3`
- branch `issue-2-evidence-checkpoint`
- branch `issue-3-broken-pipeline-checkpoint`

## Minimum Workflow

1. Start from the intended base branch.
2. Create a dedicated branch for the issue.
3. Create a dedicated worktree for that branch.
4. Run coding agents only inside that worktree.
5. Open the PR from that branch.

## Completion Cleanup

When an issue is done or its PR is merged, finish by restoring local checkout hygiene.

Minimum cleanup:

1. Remove the dedicated issue worktree after its changes are merged or otherwise preserved.
2. Delete the merged local issue branch unless another open PR still needs it.
3. Return the primary repository worktree to `main`.
4. Fast-forward that `main` worktree to `origin/main`.
5. Confirm `git status --short --branch` is clean before ending the task.

Do not silently discard unrelated local changes just to make cleanup pass. If a dirty worktree or branch blocks cleanup, move that state into its own branch or worktree first, or leave a clear blocker note in the issue or PR.

For the ordinary clean case, prefer `scripts/cleanup-finished-worktree.sh <issue-worktree>` so the fetch, fast-forward, merge check, worktree removal, branch deletion, prune, and final status confirmation stay consistent.

## Notes

- Shared source-of-truth docs may be touched by multiple issues, but each issue still edits them from its own worktree.
- If a task begins in the wrong directory, stop and move it to a dedicated worktree before continuing.
- Coordination or synthesis work may use its own separate worktree as well.
- A completed issue session should not leave a merged issue branch checked out locally or leave the primary `main` worktree behind `origin/main`.
