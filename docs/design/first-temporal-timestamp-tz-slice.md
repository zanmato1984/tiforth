# First Temporal Timestamp-Timezone Semantic Slice

Status: issue #280 design checkpoint, issue #288 local executable kernel checkpoint, issue #304 differential harness checkpoint

Verified: 2026-03-20

Related issues:

- #174 `design: define first temporal semantic slice boundary`
- #176 `docs: define first temporal date32 coverage and adapter checkpoints`
- #178 `milestone-1: implement first executable temporal date32 slice in local kernel`
- #280 `design: define first timezone-aware timestamp semantic slice checkpoint`
- #288 `milestone-1: implement first executable temporal timestamp_tz(us) slice in local kernel`
- #304 `harness: execute first timestamp_tz(us) differential artifacts`

## Question

What is the next shared temporal semantic slice beyond `date32` that can fix
one timezone-aware timestamp normalization and ordering boundary without
widening the current runtime-state model?

## Inputs Considered

- `docs/spec/type-system.md`
- `docs/contracts/data.md`
- `docs/contracts/runtime.md`
- `docs/design/first-temporal-semantic-slice.md`
- `tests/conformance/first-temporal-timestamp-tz-slice.md`
- `tests/differential/first-temporal-timestamp-tz-slice.md`
- `adapters/first-temporal-timestamp-tz-slice.md`
- issue #280

## Design Summary

The first timezone-aware timestamp checkpoint is a narrow slice that starts docs-first and now includes local executable kernel conformance:

- admitted temporal logical family: `timestamp_tz(us)` only
- admitted expression family for this slice: passthrough `column(index)` over
  `timestamp_tz(us)` input columns
- admitted predicate family for this slice:
  `is_not_null(column(index))` over `timestamp_tz(us)` input
- admitted ordering probe for this slice:
  `order-by(column(index), asc, nulls_last)` under a stable `ordering_ref`
- no temporal arithmetic, cast, extract, truncation, or timezone-negotiation
  families in this checkpoint

This keeps the first timezone-aware temporal step aligned with existing
projection and filter families while making one ordering boundary explicit.

## Type, Nullability, And Ordering Semantics

For this first timestamp-timezone slice:

- `column(index)` preserves logical type `timestamp_tz(us)` and source
  nullability
- `is_not_null(column(index))` derives logical type `boolean` with
  `nullable = false`
- ordering over non-null `timestamp_tz(us)` values compares normalized UTC
  instants
- canonical ordering for this checkpoint is ascending normalized UTC instant
  with nulls last
- out-of-range `index` remains an execution error
- `timestamp` without timezone and non-`us` timestamp units are execution
  errors in this checkpoint

## Normalization Boundary For The Slice

For this first timestamp-timezone checkpoint, shared normalization is narrow:

- adapters may ingest engine-native timestamp representations with explicit
  offsets or timezone context
- shared row comparison normalizes non-null timestamp values to signed UTC
  epoch-microsecond integers
- equivalent instants represented with different offsets normalize to the same
  epoch-microsecond value
- this checkpoint does not require preserving original offset tokens in the
  normalized differential carrier

## Data-Contract Boundary For The Slice

The shared data handoff boundary for this first timestamp-timezone slice is:

- shared temporal handoff for this slice admits `timestamp_tz(us)` arrays
- this slice still does not require one shared timezone-session negotiation API
  or runtime timezone-state surface
- adapters may keep engine-local session and timezone setup details, but shared
  differential comparison is over normalized UTC epoch-microsecond values plus
  nullability
- reserve-before-allocate admission and claim-carrying handoff behavior remain
  unchanged from `docs/contracts/data.md`

## Coverage Anchor Docs

Issue #280 provides the first docs-first coverage-anchor set for this
checkpoint:

- conformance checkpoint doc:
  `tests/conformance/first-temporal-timestamp-tz-slice.md`
- differential checkpoint doc:
  `tests/differential/first-temporal-timestamp-tz-slice.md`
- adapter boundary doc: `adapters/first-temporal-timestamp-tz-slice.md`

Issue #288 adds first executable local conformance coverage for `column(index)` passthrough and `is_not_null(column(index))` in:

- `crates/tiforth-kernel/tests/temporal_timestamp_tz_slice.rs`

Before kernel or adapter expansion claims broader timezone-aware timestamp
support, follow-on issues should preserve these anchors or replace them
explicitly.

## Out Of Scope For This Checkpoint

- `timestamp` without timezone semantics
- timestamp units other than `us`
- timezone-name canonicalization policy and timezone-database negotiation
- temporal arithmetic, cast, extract, and truncation semantics
- new runtime states or scheduler behavior

## Result

The first timezone-aware timestamp checkpoint is now explicit and narrow:
`timestamp_tz(us)` column passthrough, `is_not_null` predicate coverage, and one ascending-ordering boundary with normalized UTC epoch-microsecond comparison. Local executable shared-kernel conformance for passthrough and predicate cases now exists through `crates/tiforth-kernel/tests/temporal_timestamp_tz_slice.rs`. The first executable TiDB-versus-TiFlash differential harness checkpoint now also exists through `crates/tiforth-adapter-tidb`, `crates/tiforth-adapter-tiflash`, and `crates/tiforth-harness-differential/src/first_temporal_timestamp_tz_slice.rs` with checked-in paired artifacts under `inventory/`. Broader timestamp family behavior remains follow-on work.
