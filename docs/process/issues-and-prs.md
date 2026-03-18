# Issues And PRs

`tiforth` uses GitHub issues as the only unit of tracked work and PRs as the unit of merged change.

Repo docs are for stable knowledge, not for day-to-day execution tracking.

## Core Rules

- one active workstream should map to one primary issue
- one primary issue should use one dedicated local git worktree
- every PR must link at least one issue in its body
- every PR must declare documentation impact in its body
- every PR with in-scope `inventory/` evidence should declare inventory impact in its body
- keep live status, checklists, blockers, and handoff notes in GitHub issues, issue comments, and PRs
- do not create repo-local plan or status files for execution tracking
- if a PR fully resolves its primary issue, use `Closes #...`
- if a PR is partial, stacked, exploratory, or only related, use `Refs #...`

## Issue Rules

Use issues to define:

- problem statement
- scope
- non-goals
- deliverables
- exit criteria
- current checklist, blockers, and handoff notes

Good issue titles are specific and actionable.

Examples:

- `design: host memory admission ABI for tiforth`
- `design: tiforth dependency boundary over broken-pipeline-rs`
- `milestone-1: first Arrow-bound operator and expression slice`

## Relationship To Docs

- Keep live execution tracking in GitHub.
- Use `docs/` only for durable outputs such as architecture, contracts, semantic specs, and accepted decisions.
- When issue work settles a design conclusion that must outlive the thread, move that conclusion into the appropriate doc instead of treating the doc as a running status log.

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

### 3. Declare documentation impact

Each PR body should make documentation impact explicit.

Use one of these forms:

- `Docs-Impact: updated`
- `Docs-Impact: none - <reason>`

If a PR changes semantics, boundaries, top-level structure, or workflow-relevant implementation surfaces, it should normally update docs instead of using `none`.

See also: `docs/process/documentation-updates.md`

### 4. Declare inventory impact when needed

If a PR adds, renames, regenerates, or intentionally skips relevant checked-in `inventory/` evidence, make that decision explicit in the PR body.

Use one of these forms:

- `Inventory-Impact: updated`
- `Inventory-Impact: none - <reason>`

Use `updated` when the PR changes the checked-in inventory evidence itself.

Use `none` when inventory evidence is out of scope or when nearby slice / adapter / harness changes intentionally leave the checked-in artifacts alone and explain why.

See also: `docs/process/inventory-refresh.md`

### 5. Keep PR scope tight

A PR should not silently combine unrelated issues.

If two issues both need substantial changes, prefer two PRs.

### 6. Use docs-first when the contract is not settled

For design-heavy work, first land a docs/design PR that clarifies the contract or boundary.

Only then start the implementation PR.

## Merge Expectations

When an issue is fully resolved by a merged PR:

- prefer automatic closure via `Closes #...`
- if an issue is closed manually, leave a short summary comment pointing to the merged PR

## Post-Merge Local Cleanup

After the PR merges:

1. remove the dedicated local issue worktree
2. delete the merged local issue branch unless another open PR still needs it
3. return the primary local worktree to `main`
4. fast-forward that `main` worktree to `origin/main`
5. confirm the local `main` worktree is clean before ending the task

If unrelated local changes block cleanup, preserve that state in its own branch or worktree, or leave a short blocker note in the issue or PR instead of discarding it.

## Recommended PR Body Skeleton

```md
## Summary

## Scope

## Documentation Impact

Docs-Impact: updated

## Inventory Impact

Inventory-Impact: updated

## Notes

Closes #
```

For partial work:

```md
## Summary

## Scope

## Documentation Impact

Docs-Impact: updated

## Inventory Impact

Inventory-Impact: updated

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
