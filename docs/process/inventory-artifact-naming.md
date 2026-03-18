# Inventory Artifact Naming

Status: issue #70 design checkpoint

Related issues:

- #68 `design: define first differential expression slice and drift report format`
- #70 `docs: define inventory artifact naming conventions`

## Purpose

This note defines how checked-in evidence under `inventory/` should be named.

The goal is to keep extracted catalogs, engine notes, drift reports, and coverage gaps easy to review in git without inventing tooling or turning inventory into design authority.

## Scope

These rules apply to new or renamed inventory artifacts that live under `inventory/`.

This note defines filenames only. Check-in and refresh policy lives in `docs/process/inventory-refresh.md`.

They do not yet define:

- every artifact schema
- a required subdirectory taxonomy for every future artifact family

## Filename Pattern

Inventory artifact filenames should use this pattern:

`<subject>-<artifact-kind>.<ext>`

### `subject`

`subject` names the evidence scope from broadest to narrowest using stable repository vocabulary.

Use subject parts such as:

- shared slice IDs from `tests/conformance/` or `tests/differential/`
- engine names such as `tidb`, `tiflash`, or `tikv`
- donor or source identifiers such as `legacy`
- pairwise comparison markers using `-vs-`

Subject parts should describe the semantic or evidence scope, not the mechanics of one local run.

Do not put these in `subject`:

- issue or PR numbers
- dates or timestamps
- author names
- unstable labels such as `new`, `latest`, `final`, or `v2`

### `artifact-kind`

`artifact-kind` identifies the inventory family.

Current preferred kinds are:

- `function-catalog`
- `operator-catalog`
- `compat-notes`
- `case-results`
- `drift-report`
- `coverage-gap`

If a later artifact does not fit one of those kinds cleanly, add or revise the kind through a follow-on docs issue instead of inventing a one-off filename.

### `ext`

Use extensions that match the artifact's intended review surface:

- `.md` for review-first narrative evidence, summaries, and note-style artifacts
- `.json` for structured captures that later tooling can consume directly

If a later checkpoint needs another extension such as `.jsonl` or `.csv`, document that explicitly in the issue or PR that introduces it.

## Layout Rule

Keep inventory artifacts at the top level of `inventory/` unless a later issue defines a concrete need for family subdirectories.

For now, the filename itself should carry the primary classification. That keeps the directory flat while the artifact set is still small.

## Adoption Boundary

This convention applies going forward to new inventory artifacts and to older artifacts when a follow-on issue touches them.

Early checkpoint files such as `inventory/language-constraints.md`, `inventory/language-scorecard.md`, and `inventory/memory-accounting-blocker.md` are historical evidence and remain grandfathered until a later issue chooses to rename or retire them.

## Examples

- `legacy-function-catalog.md`
- `legacy-operator-catalog.md`
- `tiflash-compat-notes.md`
- `first-expression-slice-tidb-case-results.json`
- `first-expression-slice-tidb-vs-tiflash-drift-report.md`
- `first-expression-slice-coverage-gap.md`

## Current Example

The renamed `inventory/first-expression-slice-tidb-vs-tiflash-drift-report.md` file is the first concrete example of this convention.

It keeps the checked-in artifact aligned with the differential slice name from `tests/differential/first-expression-slice.md`, the engine pair it compares, and the artifact family it represents.
