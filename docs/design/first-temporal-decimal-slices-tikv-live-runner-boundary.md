# First Temporal And Decimal TiKV Live Runner Boundary

Status: issue #376 design checkpoint

Verified: 2026-03-21

Related issues:

- #264 `design: define first TiKV temporal date32 adapter request/response surface`
- #266 `adapter: execute first-temporal-date32-slice through TiKV`
- #270 `harness: add first-temporal-date32-slice TiKV pairwise drift artifacts`
- #278 `design: define first TiKV decimal128 adapter request/response surface`
- #284 `harness: execute first-decimal128 TiKV single-engine and pairwise artifacts`
- #290 `design: define TiKV adapter boundary for first-temporal-timestamp-tz-slice`
- #306 `checkpoint: implement TiKV first-temporal-timestamp-tz executable differential slice`
- #374 `harness: implement first-union-slice live TiKV runner and refresh workflow`
- #376 `design: define live TiKV temporal and decimal runner refresh boundary`

## Question

What docs-first boundary should govern live TiKV runner configuration,
execution, and artifact refresh for the existing first temporal and decimal
slices while keeping stable semantic IDs and carrier schemas unchanged?

## Inputs Considered

- `docs/vision.md`
- `docs/design/adapter-runtime-orchestration-boundary.md`
- `docs/design/first-union-slice-tikv-live-runner-boundary.md`
- `adapters/first-temporal-date32-slice-tikv.md`
- `adapters/first-temporal-timestamp-tz-slice-tikv.md`
- `adapters/first-decimal128-slice-tikv.md`
- `tests/differential/first-temporal-date32-slice-artifacts.md`
- `tests/differential/first-temporal-timestamp-tz-slice-artifacts.md`
- `tests/differential/first-decimal128-slice-artifacts.md`
- `docs/process/inventory-refresh.md`
- issue #376

## Design Summary

The first temporal-and-decimal live TiKV boundary is configuration-first and
carrier-stable:

- keep the existing first-slice `slice_id`, `case_id`, and operation refs
- keep existing checked-in artifact families and JSON/Markdown carrier schemas
- define one explicit inspect/write refresh mode boundary for live execution
- keep connection/session/retry/cancellation policy adapter-local

This checkpoint defines durable boundaries only; it does not add executable live
runner code in this issue.

## Slice Scope

This boundary applies only to these existing TiKV checkpoints:

- `first-temporal-date32-slice`
- `first-temporal-timestamp-tz-slice`
- `first-decimal128-slice`

It does not broaden temporal/decimal semantics beyond already accepted case
sets.

## Configuration Boundary

Live execution remains environment-driven and adapter-local, consistent with
`docs/design/adapter-runtime-orchestration-boundary.md`.

For each live run, orchestration must be able to configure:

- TiDB connection settings
- TiFlash connection settings
- TiKV connection settings

Per-engine host, port, user, and database settings are required. Password and
client-binary overrides remain optional.

This boundary does not freeze one DSN format, one client binary, or one
cluster/deployment topology.

## Execution Boundary

For each in-scope slice, live execution should:

1. run the existing documented first-slice case IDs unchanged
2. capture one normalized TiKV single-engine `case-results` artifact
3. render `tidb-vs-tikv` and `tiflash-vs-tikv` pairwise drift reports
4. keep normalization and drift-report fields aligned with the current artifact
   carrier docs

Missing required runner configuration should fail fast and should not silently
skip documented cases.

## Artifact Refresh Boundary

Live execution should expose two explicit modes:

- inspect mode: execute and print normalized artifacts without mutating
  checked-in files
- write mode: overwrite only the existing checked-in artifact families listed
  below after a successful run

Write-mode refresh scope by slice:

- `first-temporal-date32-slice`:
  - `inventory/first-temporal-date32-slice-tikv-case-results.json`
  - `inventory/first-temporal-date32-slice-tidb-vs-tikv-drift-report.md`
  - `inventory/first-temporal-date32-slice-tidb-vs-tikv-drift-report.json`
  - `inventory/first-temporal-date32-slice-tiflash-vs-tikv-drift-report.md`
  - `inventory/first-temporal-date32-slice-tiflash-vs-tikv-drift-report.json`
- `first-temporal-timestamp-tz-slice`:
  - `inventory/first-temporal-timestamp-tz-slice-tikv-case-results.json`
  - `inventory/first-temporal-timestamp-tz-slice-tidb-vs-tikv-drift-report.md`
  - `inventory/first-temporal-timestamp-tz-slice-tidb-vs-tikv-drift-report.json`
  - `inventory/first-temporal-timestamp-tz-slice-tiflash-vs-tikv-drift-report.md`
  - `inventory/first-temporal-timestamp-tz-slice-tiflash-vs-tikv-drift-report.json`
- `first-decimal128-slice`:
  - `inventory/first-decimal128-slice-tikv-case-results.json`
  - `inventory/first-decimal128-slice-tidb-vs-tikv-drift-report.md`
  - `inventory/first-decimal128-slice-tidb-vs-tikv-drift-report.json`
  - `inventory/first-decimal128-slice-tiflash-vs-tikv-drift-report.md`
  - `inventory/first-decimal128-slice-tiflash-vs-tikv-drift-report.json`

Compatibility notes remain review-first artifacts and are not auto-regenerated
by live runner refresh.

## Guardrails

- preserve existing first-slice semantic IDs and operation refs
- preserve current carrier field names and drift-classification vocabulary
- keep runner orchestration details adapter-local
- keep inventory impact explicit in PR metadata per
  `docs/process/inventory-refresh.md`

## Deferred Follow-On

Follow-on implementation issues may add executable temporal/decimal live runner
modules and refresh scripts within this boundary.

Broader temporal-family semantics and decimal-family expansion remain separate
follow-on scope.

## Result

`tiforth` now has a durable docs-first live-runner configuration/execution/
refresh boundary for first temporal (`date32`, `timestamp_tz(us)`) and decimal
(`decimal128`) TiKV checkpoints while preserving stable semantics and carrier
schemas.
