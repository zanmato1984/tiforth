# Issues And PRs

`tiforth` uses issues as the unit of planned work and PRs as the unit of merged change.

This file defines how they should connect.

## Core Rules

- one active workstream should map to one primary issue
- one primary issue should use one dedicated local git worktree
- every PR must link at least one issue in its body
- if a PR fully resolves its primary issue, use `Closes #...`
- if a PR is partial, stacked, exploratory, or only related, use `Refs #...`

## Issue Rules

Use issues to define:

- problem statement
- scope
- non-goals
- deliverables
- exit criteria

Good issue titles are specific and actionable.

Examples:

- `design: host memory admission ABI for tiforth`
- `design: tiforth dependency boundary over broken-pipeline-rs`
- `milestone-1: first Arrow-bound operator and expression slice`

## PR Rules

### 1. Always link the issue

A PR body should always include one of these forms:

- `Closes #8`
- `Refs #9`

Use `Closes #...` when merging the PR to `main` should automatically close the issue.

Use `Refs #...` when the PR is:

- only part of the issue
- a stacked/intermediate checkpoint
- a supporting PR that should not close the issue by itself

### 2. Prefer one primary issue per PR

A PR may reference multiple issues, but it should have one clear primary issue.

Recommended pattern:

- one `Closes #...` line for the primary issue
- optional `Refs #...` lines for related issues

### 3. Keep PR scope tight

A PR should not silently combine unrelated issues.

If two issues both need substantial changes, prefer two PRs.

### 4. Use docs-first when the contract is not settled

For design-heavy work, first land a docs/design PR that clarifies the contract or boundary.

Only then start the implementation PR.

## Merge Expectations

When an issue is fully resolved by a merged PR:

- prefer automatic closure via `Closes #...`
- if an issue is closed manually, leave a short summary comment pointing to the merged PR

## Recommended PR Body Skeleton

```md
## Summary

## Scope

## Notes

Closes #
```

For partial work:

```md
## Summary

## Scope

## Notes

Refs #
```

## Relationship To Worktrees

Issue and PR rules are tied to the worktree rule.

Before starting issue work:

1. create or switch to the issue's dedicated branch
2. create or switch to the issue's dedicated worktree
3. do the work there
4. open the PR from that branch

See also: `docs/process/worktrees.md`
