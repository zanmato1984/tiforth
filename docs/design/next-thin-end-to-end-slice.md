# Next Thin End-To-End Slice

Status: issue #80 design checkpoint

Verified: 2026-03-18

Related issues:

- #10 `milestone-1: first Arrow-bound operator and expression slice`
- #68 `design: define first differential expression slice and drift report format`
- #72 `design: define first differential adapter request/response surface`
- #80 `design: define the next thin end-to-end slice after milestone-1 projection`

## Question

What should `tiforth` define as the next narrow, end-to-end checkpoint now that the local milestone-1 source -> projection -> sink slice is documented and executable?

## Inputs Considered

- `docs/vision.md`
- `docs/architecture.md`
- `docs/spec/milestone-1-expression-projection.md`
- `tests/conformance/expression-projection-slice.md`
- `tests/differential/first-expression-slice.md`
- `adapters/first-expression-slice.md`
- `inventory/first-expression-slice-tidb-vs-tiflash-drift-report.md`
- `docs/process/inventory-artifact-naming.md`
- `docs/process/inventory-refresh.md`
- issue #80

## Design Summary

The next thin end-to-end slice is the first executable differential harness over `first-expression-slice`.

That slice should:

- start from the shared `case_id`, `input_ref`, and `projection_ref` definitions in `tests/differential/first-expression-slice.md`
- submit one documented case at a time through the shared request boundary from `adapters/first-expression-slice.md`
- execute that case independently against the TiDB and TiFlash adapters
- normalize one `case result` record per engine and aggregate those records into the drift-report carrier from `inventory/first-expression-slice-tidb-vs-tiflash-drift-report.md`
- check in the resulting evidence using the naming and refresh rules under `docs/process/`

This keeps the follow-on checkpoint end to end without widening the shared kernel beyond the current milestone-1 expression-projection slice.

## Why This Slice Next

- it extends coverage outward into adapters, normalized evidence, and drift review without inventing new expression semantics
- it proves that the already-documented differential case IDs, adapter request surface, and artifact carriers are executable as one coherent path
- it surfaces the first real TiDB-versus-TiFlash semantic drift before later issues broaden operator families or kernel responsibilities

## Boundary

The next slice remains intentionally narrow:

- engines: `TiDB` and `TiFlash` only
- semantic family: the existing `column`, `literal<int32>`, and `add<int32>` projection cases from `tests/differential/first-expression-slice.md`
- normalized outcomes: rows plus the first small error vocabulary already documented for overflow and adapter availability
- artifacts: one `case result` per engine and case, plus one aggregated drift report for the engine pair

The next slice does **not** add:

- new kernel operators or broader shared kernel APIs
- TiKV participation
- local runtime-event, admission-event, or ownership-trace comparison in the differential path
- adapter-shared SQL text, planner hints, credentials, or environment provisioning rules

## End-To-End Path

1. select one documented `case_id` from `tests/differential/first-expression-slice.md`
2. build the shared adapter request with `slice_id`, `case_id`, `spec_refs[]`, `input_ref`, and `projection_ref`
3. execute that request against one engine adapter at a time
4. capture one normalized `case result` record per engine
5. compare the paired engine records using the documented dimensions from `inventory/first-expression-slice-tidb-vs-tiflash-drift-report.md`
6. emit checked-in `case-results` and `drift-report` artifacts that follow the inventory naming and refresh policy

This is the smallest next checkpoint that crosses shared docs, adapters, and reviewable evidence while still reusing the already-settled milestone-1 semantic core.

## Completion Signal

The next thin slice is ready to implement when a follow-on issue can target all of these outcomes together:

- each first-slice `case_id` can produce either a normalized engine result or an explicit `adapter_unavailable` outcome
- one TiDB-versus-TiFlash drift report can compare those results with the currently documented dimensions only
- the resulting artifacts are reviewable and check-in friendly without relying on kernel-local runtime or admission snapshots
- no new operator or function implementation is needed to exercise the slice end to end

## Follow-On Boundary

Once this differential slice exists as executable evidence, later issues may use it to justify either:

- broadening differential coverage to more engines or expression families, or
- proposing the next shared-kernel expansion boundary with real cross-engine evidence already in hand

Until then, the next thin end-to-end slice should grow harness and adapter execution around the current semantic core rather than widening that core itself.
