# Inventory Check-In And Refresh

Status: issue #76 design checkpoint

Related issues:

- #68 `design: define first differential expression slice and drift report format`
- #70 `docs: define inventory artifact naming conventions`
- #76 `docs: define inventory check-in and regeneration policy`

## Purpose

This note defines when `inventory/` artifacts belong in git, when they should be refreshed, and how PRs should explain inventory impact.

`inventory/` is for durable reviewable evidence, not raw local run output.

## Check-In Rule

Check an inventory artifact into git when all of these are true:

- the artifact captures evidence that reviewers should be able to inspect later without replaying one local run
- the content is normalized enough to stay readable and diffable in git
- the artifact is small enough for ordinary review and does not depend on timestamps, host-local paths, or other unstable noise
- the artifact fits a documented inventory family, or the PR explicitly introduces that family through docs-first updates

Do not check raw or unstable captures into git when any of these are true:

- the content is mostly local execution noise such as timestamps, planner traces, or environment-specific paths
- the artifact is too large or too volatile for meaningful review
- the output includes secrets, credentials, or other sensitive engine details
- the artifact shape is not yet documented well enough to tell whether later refreshes are comparable

When evidence starts in a raw local form, normalize it into a durable inventory artifact or keep it in issue / PR discussion until a follow-on docs issue defines the right checked-in shape.

## Artifact Classes

### Review-First Notes

Examples:

- drift reports
- coverage gaps
- compatibility notes
- donor catalogs

These are durable repository evidence. They are normally checked in and updated in place when their meaning changes.

### Normalized Generated Captures

Examples:

- stable per-case result records
- structured normalized evidence derived from harness runs

These may be checked in when their fields are intentionally stable and their schema is documented. Until executable tooling exists, the issue or PR introducing them should explain the human-level generation path well enough for reviewers to understand what changed.

### Raw Local Captures

Examples:

- ad hoc SQL transcripts
- engine-native trace dumps
- temporary debug output

These are working evidence, not checked-in inventory. Summarize or normalize them before they enter `inventory/`.

## Refresh Triggers

Refresh checked-in inventory when a PR changes any source-of-truth input that would alter the artifact's meaning, including:

- the compared slice, case IDs, input refs, or projection refs
- the adapter boundary or engine set the artifact depends on
- the normalized schema or status fields that the artifact records
- the documented conclusion of a coverage gap, drift note, or compatibility checkpoint

Refresh is usually not required when changes stay outside the artifact's semantic scope, for example:

- local runtime bookkeeping that never reaches the documented differential or inventory surface
- unrelated docs or tests that do not change the evidence the artifact summarizes
- follow-on work that intentionally leaves a future artifact family undefined

If a nearby change might reasonably make a reviewer ask "should inventory have moved too?", answer that in the PR body even when the checked-in artifacts stay unchanged.

## PR Expectation

When `inventory/` evidence is in scope, make the PR body explicit with one of these forms:

- `Inventory-Impact: updated`
- `Inventory-Impact: none - <reason>`

Use `updated` when the PR adds, renames, regenerates, or manually revises checked-in inventory artifacts.

Use `none` when inventory evidence is out of scope or when the PR intentionally leaves relevant artifacts unchanged and explains why.

This inventory note complements `Docs-Impact: ...`; it does not replace documentation-impact reporting.

## Relationship To Naming

`docs/process/inventory-artifact-naming.md` defines what checked-in inventory artifacts should be called.

This note defines whether evidence belongs in git at all, when it should be refreshed, and how PRs should describe that decision.

## Boundary For Now

This policy does not yet define:

- executable regeneration tooling or required commands
- every future inventory schema
- mandatory subdirectories for artifact families
- retention rules for external large artifacts or databases

Those remain follow-on issues once harness execution produces more evidence than the current review-first checkpoint set.
