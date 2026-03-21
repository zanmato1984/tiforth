# First Union Slice TiKV Live Runner Boundary

Status: issue #372 design checkpoint, issue #374 executable checkpoint

Verified: 2026-03-21

Related issues:

- #340 `docs: define first-union-slice differential artifact carriers`
- #368 `harness: add TiKV first-union-slice executable checkpoints`
- #370 `inventory: add first-union-slice TiKV compatibility notes checkpoint`
- #372 `design: define live TiKV runner boundary for first-union-slice`
- #374 `harness: implement first-union-slice live TiKV runner and refresh workflow`

## Question

What docs-first boundary should govern live TiKV execution and artifact refresh
for `first-union-slice` without changing the existing semantic IDs or checked-in
artifact carriers?

## Inputs Considered

- `docs/vision.md`
- `docs/design/adapter-runtime-orchestration-boundary.md`
- `adapters/first-union-slice-tikv.md`
- `tests/differential/first-union-slice.md`
- `tests/differential/first-union-slice-artifacts.md`
- `docs/process/inventory-refresh.md`
- `inventory/first-union-slice-tikv-case-results.json`
- `inventory/first-union-slice-tidb-vs-tikv-drift-report.md`
- `inventory/first-union-slice-tiflash-vs-tikv-drift-report.md`
- issue #372
- issue #374

## Design Summary

`first-union-slice` has a docs-first live TiKV runner boundary that keeps
semantics and artifact carriers stable while defining three scoped areas:

- environment configuration required to contact TiDB, TiFlash, and TiKV for
  one live run
- orchestration responsibilities for one end-to-end execution and normalization
- optional checked-in artifact refresh behavior that is explicit and reviewable

This boundary governs how live evidence is captured, not what the slice means.
All semantic IDs, case refs, and normalized carrier fields remain owned by the
existing shared docs and adapter boundary.

## Configuration Boundary

Live `first-union-slice` execution remains environment-driven and adapter-local,
consistent with `docs/design/adapter-runtime-orchestration-boundary.md`.

For a live run that compares TiKV against both existing engines, orchestration
must be able to configure these engine families:

- TiDB connection settings
- TiFlash connection settings
- TiKV connection settings

Per-engine host, port, user, and database settings are required inputs for live
execution. Password and client-binary overrides remain optional.

This boundary does not freeze one DSN format, one SQL client binary, or one
deployment topology; those stay adapter-local implementation details.

## Execution Boundary

Live runner behavior for this slice stays narrow:

1. execute the existing documented `first-union-slice` case IDs
2. capture one normalized TiKV single-engine `case result` artifact
3. render pairwise drift outcomes for `tidb-vs-tikv` and `tiflash-vs-tikv`
4. keep normalization aligned with
   `tests/differential/first-union-slice-artifacts.md`

The runner should fail fast when required environment configuration is missing
and should not silently skip documented cases.

This boundary does not introduce new case IDs, new operation refs, or new
error-class vocabulary.

## Artifact Refresh Boundary

Live execution supports two explicit modes:

- inspect mode: run and print generated normalized artifacts without mutating
  checked-in files
- write mode: overwrite the existing checked-in first-union TiKV artifacts only
  after a successful run

In write mode, refresh scope is limited to the existing first-union carrier
family:

- `inventory/first-union-slice-tikv-case-results.json`
- `inventory/first-union-slice-tidb-vs-tikv-drift-report.md`
- `inventory/first-union-slice-tidb-vs-tikv-drift-report.json`
- `inventory/first-union-slice-tiflash-vs-tikv-drift-report.md`
- `inventory/first-union-slice-tiflash-vs-tikv-drift-report.json`

`inventory/first-union-slice-tikv-compat-notes.md` remains review-first and
manual. Live runner refresh does not auto-regenerate compatibility notes.

## Guardrails

- preserve `slice_id = first-union-slice` and all existing first-union
  `case_id` values
- preserve current normalized field names and pairwise drift carrier families
- keep runner transport/session policy and retry or timeout mechanics
  adapter-local
- keep inventory refresh behavior explicit in PR metadata per
  `docs/process/inventory-refresh.md`

## Executable Checkpoint

Issue #374 and issue #384 land executable live-runner wiring inside this boundary:

- `crates/tiforth-harness-differential/src/first_union_slice_tikv_live.rs`
- `crates/tiforth-harness-differential/src/bin/first_union_slice_tikv_live.rs`
- `scripts/refresh-first-union-tikv-live-artifacts.sh`
- `scripts/refresh-first-tikv-live-artifacts.sh`

Those executable checkpoints keep the existing first-union semantic IDs,
normalized carrier schema, and TiKV artifact family unchanged.

## Follow-On

This boundary still does not broaden live runner scope to temporal, decimal, or
other slice families.

## Result

`tiforth` now has both a durable docs-first boundary and executable live-runner
wiring for TiKV `first-union-slice` orchestration and artifact refresh, with
stable semantics and stable artifact carriers preserved.
