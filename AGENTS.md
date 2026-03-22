# AGENTS

This file is a table of contents for future agents. The source of truth lives under `docs/`, plus the linked GitHub issues and PRs.

## Startup Focus Control

Every session starts in this order:

1. `AGENTS.md`
2. `docs/process/focus-driven-execution.md`
3. the GitHub `program:` meta issue named there
4. the active `epic:` issue named by that meta issue
5. the current concrete issue named by that meta issue

Do not treat `docs/vision.md` `Next Checkpoint` notes or other forward-looking
docs text as live priority unless the `program:` meta issue explicitly promotes
that direction.

## Read In This Order For Stable Repo Context

1. `README.md`
2. `docs/vision.md`
3. `docs/architecture.md`
4. `docs/contracts/data.md`
5. `docs/contracts/runtime.md`
6. `docs/spec/type-system.md`
7. `docs/process/documentation-updates.md`
8. `docs/process/focus-driven-execution.md`
9. `docs/process/issues-and-prs.md`
10. `docs/process/worktrees.md`
11. `docs/process/inventory-artifact-naming.md`
12. `docs/process/inventory-refresh.md`
13. `docs/decisions/README.md`

## Where Things Go

- `docs/`: intended architecture, contracts, semantic spec, and accepted decisions
- `inventory/`: extracted catalogs, donor analysis, drift reports
- `tests/`: harness definitions, fixtures, and case coverage
- `adapters/`: boundary docs for engine integration points
- `scripts/`: local workflow helpers for repeatable repository maintenance tasks
- `docs/process/`: project workflow and execution conventions
  - `worktrees.md`: one worktree per active issue/workstream
  - `issues-and-prs.md`: how execution tracking lives in GitHub issues and PRs
  - `documentation-updates.md`: when implementation work must update docs
  - `inventory-artifact-naming.md`: how checked-in `inventory/` evidence should be named
  - `inventory-refresh.md`: when `inventory/` evidence belongs in git and when PRs should refresh it
- `docs/decisions/`: long-lived architectural decisions that should outlive issue threads

## Operating Rules

- Treat `docs/` as the source of truth for stable design knowledge.
- Track live execution state in GitHub issues, issue comments, and PRs. Do not add repo-local plan or status tracking docs for project progress.
- Use `docs/process/focus-driven-execution.md` plus the GitHub `program:` meta issue as the startup control layer for choosing current work.
- Keep exactly one active top-level epic and one current concrete issue at a time.
- If the current issue is blocked by a missing accepted boundary, open the blocking docs-first or bootstrap issue inside the same active epic instead of jumping to another epic.
- If a PR changes semantics, boundaries, top-level structure, or contributor workflow, update the corresponding docs or explain `Docs-Impact: none - <reason>` in the PR body.
- When a PR adds, renames, regenerates, or intentionally skips in-scope `inventory/` evidence, follow `docs/process/inventory-refresh.md` and declare `Inventory-Impact: ...` in the PR body.
- Do not add concrete operators or functions until their specs and harness coverage exist.
- Preserve Apache Arrow as the data-contract direction and broken-pipeline ideas as runtime inspiration, but do not copy donor implementations by default.
- Record unresolved decisions as TODOs with context or as follow-up issues instead of forcing premature choices.
- When a design conclusion must outlive an issue thread, record it under `docs/decisions/` rather than in a plan file.
- Do not add a build system, package manager, CI workflow, or implementation runtime unless an accepted issue or decision explicitly requires it.
- Every issue must use its own local git worktree. Do not run multiple issue-scoped tasks in the same directory. See `docs/process/worktrees.md`.
- If no open issue exists, first open a bootstrap issue for the next concrete repository-backed task, then create its dedicated worktree.
- For the ordinary clean setup case, prefer `scripts/start-issue-worktree.sh <issue-number>` before starting issue work.
- After an issue PR merges, remove its dedicated local worktree and merged local branch, then leave the primary `main` worktree checked out, up to date, and clean before ending the session. If unrelated local changes block that cleanup, preserve them in their own branch or worktree or report the blocker instead of discarding them.
- Every PR must link its primary issue. Use `Closes #...` when merge should close the issue; use `Refs #...` when the PR is partial or stacked. See `docs/process/issues-and-prs.md`.
- Every commit must include a DCO trailer: `Signed-off-by:`.
- Set local sign-off defaults per worktree: `git config --local format.signoff true`, then verify `git config --local --get format.signoff` outputs `true`.
- Use `git commit -s` for new commits. If a commit is missing sign-off, fix it with `git commit --amend -s --no-edit`.
- Before `git push`, before opening/updating a PR, and before merge, run `scripts/check-signoffs.sh origin/main..HEAD`; that helper validates that every commit in `origin/main..HEAD` has a `Signed-off-by:` trailer.
