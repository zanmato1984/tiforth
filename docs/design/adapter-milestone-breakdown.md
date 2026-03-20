# Adapter Milestone Breakdown

Status: issue #88 design checkpoint

Verified: 2026-03-19

Related issues:

- #68 `design: define first differential expression slice and drift report format`
- #72 `design: define first differential adapter request/response surface`
- #88 `design: define adapter milestone breakdown for first differential slice`
- #80 `design: define the next thin end-to-end slice after milestone-1 projection`
- #218 `design: define first TiKV differential adapter request/response surface`
- #220 `adapter: execute first-expression-slice through TiKV`

## Question

How should the first executable differential adapter work break down into tracked issues and harness checkpoints now that the shared TiDB-versus-TiFlash slice is already documented?

## Inputs Considered

- `docs/architecture.md`
- `docs/design/next-thin-end-to-end-slice.md`
- `tests/differential/README.md`
- `tests/differential/first-expression-slice.md`
- `adapters/first-expression-slice.md`
- `adapters/tidb/README.md`
- `adapters/tiflash/README.md`
- `adapters/tikv/README.md`
- `tests/differential/first-expression-slice-artifacts.md`
- `docs/process/issues-and-prs.md`
- `docs/process/inventory-refresh.md`

## Design Summary

The first executable adapter milestone should not land as one large cross-engine issue.

Break it into three reviewable checkpoints after the already-accepted shared docs:

1. one TiDB adapter issue that executes every documented `first-expression-slice` case into the normalized `case result` carrier
2. one TiFlash adapter issue that does the same for TiFlash
3. one differential harness issue that runs both adapters, compares the paired results, and checks in the first `case-results` plus `drift-report` evidence

This keeps each issue narrow, preserves the one-issue-per-worktree rule, and makes it clear when inventory evidence should start moving in git.

## Why This Breakdown

- each engine bridge can be reviewed on its own without mixing two SQL paths, drift aggregation, and inventory refresh into one PR
- the shared request and response contract from `adapters/first-expression-slice.md` is already stable enough to anchor single-engine work first
- the first checked-in differential evidence should appear only when both adapters exist and a harness issue can compare them directly
- TiKV should stay deferred until the TiDB-versus-TiFlash path proves the request surface, normalization rules, and artifact refresh workflow
- once that pairwise checkpoint exists, TiKV should move as its own follow-on boundary issue; issue #218 now defines that first request/response checkpoint in `adapters/first-expression-slice-tikv.md`

## Milestone Order

### Milestone 0: Shared Slice Definition

This is already fixed by the existing docs checkpoint set:

- `tests/differential/first-expression-slice.md`
- `adapters/first-expression-slice.md`
- `tests/differential/first-expression-slice-artifacts.md`
- `docs/design/next-thin-end-to-end-slice.md`

No new adapter implementation issue should redefine those shared identifiers before the first executable checkpoint exists.

### Milestone 1: TiDB Single-Engine Adapter Checkpoint

The first new tracked issue should be TiDB-only.

Recommended issue shape:

- title pattern: `adapter: execute first-expression-slice through TiDB`
- scope: accept the shared request carrier, derive TiDB-native execution, and return one normalized `case result`
- exit criteria: every documented `case_id` returns either normalized `rows`, normalized `arithmetic_overflow`, `engine_error`, or explicit `adapter_unavailable`
- non-goals: TiFlash execution, pairwise drift aggregation, checked-in differential inventory, and request-surface expansion beyond the current slice

The checkpoint is complete when reviewers can inspect one engine bridge in isolation and see that it preserves the shared slice vocabulary instead of introducing TiDB-owned semantics.

A single-engine checkpoint may land as a narrow engine-specific crate under `crates/` that owns request validation, engine-native SQL lowering, and normalized `case result` shaping behind a runner boundary, while leaving live connection and orchestration concerns adapter-local.

### Milestone 2: TiFlash Single-Engine Adapter Checkpoint

The second tracked issue should mirror the TiDB shape for TiFlash.

Recommended issue shape:

- title pattern: `adapter: execute first-expression-slice through TiFlash`
- scope: accept the same shared request carrier and return the same normalized `case result` fields for TiFlash
- exit criteria: every documented `case_id` yields normalized `rows`, normalized `arithmetic_overflow`, `engine_error`, or explicit `adapter_unavailable`
- non-goals: changing shared case IDs, redefining the error vocabulary, or widening the slice to TiKV or new expression families

This issue should stay symmetrical with the TiDB checkpoint so later differential comparison is mostly a harness concern rather than a negotiation between two differently shaped adapters.

A TiFlash single-engine checkpoint may land as a narrow engine-specific crate under `crates/` that owns request validation, TiFlash-oriented SQL lowering, and normalized `case result` shaping behind a runner boundary, while leaving live connection and orchestration concerns adapter-local.

### Milestone 3: Pairwise Differential Harness Checkpoint

Only after both single-engine checkpoints exist should a follow-on harness issue aggregate them.

Recommended issue shape:

- title pattern: `harness: compare first-expression-slice results for TiDB and TiFlash`
- scope: run each documented case through both adapters, persist one checked-in `case-results` artifact per engine, and emit one checked-in TiDB-versus-TiFlash `drift-report`
- exit criteria: every paired case is classified as `match`, `drift`, or `unsupported` using the documented comparison dimensions only
- PR expectation: `Docs-Impact: updated` if shared docs move, plus `Inventory-Impact: updated` when the checked-in evidence lands or refreshes
- non-goals: new kernel operators, TiKV participation, engine-plan capture, or generalized adapter capability negotiation

This is the first checkpoint where `inventory/` evidence should normally change, because it is the first point where the repository can compare real paired engine outputs against the documented differential slice.

## Harness Checkpoints

The adapter milestone breakdown should use these review checkpoints.

### Single-Engine Checkpoint

One adapter can round-trip the shared request surface into the documented normalized response shape.

Reviewers should expect:

- stable `slice_id`, `case_id`, `input_ref`, and `projection_ref` reuse
- one normalized outcome per case
- explicit `adapter_unavailable` for unsupported documented cases rather than silent omission
- no checked-in pairwise drift report yet

### Pairwise Differential Checkpoint

Both adapters run the same shared cases and produce reviewable paired evidence.

Reviewers should expect:

- checked-in `case-results` artifacts that follow `docs/process/inventory-artifact-naming.md`
- one checked-in TiDB-versus-TiFlash `drift-report`
- explicit `Inventory-Impact: updated` in the PR body
- comparison limited to the dimensions already documented for `first-expression-slice`

### Post-Checkpoint Expansion

Only after the pairwise checkpoint exists should follow-on issues broaden one dimension at a time, for example:

- TiKV executable single-engine adapter implementation on top of `adapters/first-expression-slice-tikv.md` (issue #220 now lands this checkpoint in `crates/tiforth-adapter-tikv`)
- broader error normalization
- a wider expression family
- engine-specific compatibility notes derived from the now-executable slice

Those should remain separate issues rather than being folded back into the first three checkpoints.

## Tracking Rules For Follow-On Issues

- keep one primary issue per adapter or harness checkpoint
- keep one dedicated worktree per issue, even when later checkpoints build on the same slice
- use `Refs #...` for partial or stacked adapter groundwork and `Closes #...` only when one checkpoint is fully complete
- keep inventory updates out of single-engine adapter issues unless they intentionally check in normalized single-engine evidence and say so explicitly

## Result

The documented first executable adapter path now breaks down into one TiDB issue, one TiFlash issue, and one pairwise differential harness issue. Issue #218 captures the first TiKV request/response boundary, issue #220 lands the first TiKV single-engine executable adapter checkpoint in `crates/tiforth-adapter-tikv`, and issue #228 lands the first TiKV compatibility-notes inventory checkpoint for `first-expression-slice`. Pairwise TiKV drift policy and checked-in TiKV single-engine case-results artifacts remain separate follow-on checkpoints unless another accepted design issue deliberately changes the slice or comparison surface.
