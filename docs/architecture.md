# Architecture

The current proposal is intentionally layered so specs and harnesses can mature before kernels grow beyond the current milestone-1 slices.

## Proposed Layers

### 1. Spec

Defines names, signatures, semantics, edge cases, and compatibility notes for operators, functions, and types.

### 2. Data Contract

Defines the in-memory representation and batch boundaries. Directionally Arrow-native.

### 3. Kernel

Execution primitives that implement spec-defined behavior over the data contract. This layer is currently limited to narrow milestone-1 slices backed by accepted docs and local tests.

### 4. Runtime

Defines how kernels are scheduled, chained, canceled, instrumented, and backpressured. For the current milestone-1 slices, `tiforth` directly adopts the Arrow-bound runtime contract from `broken-pipeline-rs`; `tiforth`-owned operators and expressions attach on top of that contract rather than replacing it.

### 5. Adapters

Engine-facing translation layers for TiDB, TiFlash, and TiKV. Adapters should map engine concepts onto shared specs and contracts rather than owning semantics.

### 6. Harnesses

Conformance, differential, and performance harnesses that exercise the stack and report drift.

The first documented differential checkpoint is the TiDB-versus-TiFlash expression slice in `tests/differential/first-expression-slice.md`.

## Formal Repository Shape

Issue #396 now fixes the project's formal top-level organization in
`docs/design/formal-project-layout-and-top-level-shared-surfaces.md`.

That checkpoint formalizes the current repository shape as one top-level
directory per durable concern:

- `docs/`
- `tests/`
- `inventory/`
- `adapters/`
- `crates/`
- `scripts/`

It also formalizes the current long-lived workspace-crate categories as:

- `tiforth-kernel` for shared kernel data, runtime, admission, ownership, and
  executable-slice surfaces
- `tiforth-adapter-<engine>` for engine-local execution and translation
- `tiforth-harness-<kind>` for harness execution, comparison, and artifact
  rendering

This means future growth should prefer adding modules under those existing
categories instead of adding one new top-level directory or one new crate per
slice.

Issue #401 now re-evaluates post-reorganization adapter and harness carrier
extraction in
`docs/design/post-reorganization-shared-carrier-extraction-boundary.md`.

That follow-on keeps the workspace-crate categories unchanged: the next
implementation-sharing step, if one is needed, should stay inside existing
adapter `engine/` modules or inside `crates/tiforth-harness-differential`
rather than introducing a new cross-workspace support crate.

## Current Repository Bias

This reboot started in layers 1, 2, 4, 5, and 6. Layer 3 now enters only through minimal milestone-1 slices that are justified by docs and local tests.

That bias first landed as the executable expression differential harness fixed by `docs/design/next-thin-end-to-end-slice.md` and its issue-scoped breakdown in `docs/design/adapter-milestone-breakdown.md`.

`docs/design/kernel-expansion-acceptance.md` then fixed the gate for later layer-3 growth after that differential checkpoint existed.

Since then, later issue-scoped checkpoints have widened the current repository shape through the first post-gate filter boundary, the first temporal `date32` and `timestamp_tz(us)` slices, the first `decimal128` and float64-ordering slices, the first `uint64` unsigned arithmetic slice, the first JSON passthrough-and-predicate checkpoint, and the first nested struct, map, and union passthrough checkpoints while keeping the shared kernel intentionally narrow and docs-first.

`docs/design/first-post-gate-kernel-boundary.md` fixed the first post-gate layer-3 expansion as one narrow filter boundary, and later accepted follow-on docs extended that same checkpoint style without widening into a general shared-kernel API.

`docs/design/first-in-contract-exchange-slice.md` now fixes the first post-milestone in-contract runtime exchange checkpoint as one narrow single-producer and single-consumer local queue boundary with required conformance and differential coverage, again without widening current shared-kernel semantics.

## Current Minimal Kernel Boundaries

The currently useful shared-kernel boundaries under `crates/tiforth-kernel` are:

- the milestone-1 expression-projection slice
- the first post-gate filter slice for `is_not_null(column(index))`
- the first temporal `date32` executable conformance extension for `column(index)` passthrough and `is_not_null(column(index))`
- the first temporal `timestamp_tz(us)` executable conformance extension for `column(index)` passthrough and `is_not_null(column(index))`
- the first decimal `decimal128` executable conformance extension for `column(index)` passthrough and `is_not_null(column(index))`
- the first float64 NaN and infinity ordering executable conformance extension for `column(index)` passthrough and `is_not_null(column(index))`
- the first unsigned arithmetic `uint64` executable conformance extension for passthrough `column(index)`, `literal<uint64>(value)`, `add<uint64>(lhs, rhs)`, and `is_not_null(column(index))`
- the first JSON executable conformance extension for passthrough `column(index)` and `is_not_null(column(index))` over canonical JSON value-token carriers
- the first nested struct passthrough checkpoint for `struct<a:int32, b:int32?>`
- the first nested map passthrough checkpoint for `map<int32, int32?>`
- the first nested union passthrough checkpoint for `dense_union<i:int32, n:int32?>`

Those boundaries are intentionally narrow:

- one static Arrow batch source, one projection or filter pipe, and one collecting sink for the local executable slices
- expression evaluation only for `column(index)`, `literal<int32>(value)`, and `add<int32>(lhs, rhs)` in the milestone-1 slice, plus narrow `uint64` follow-on coverage for `literal<uint64>(value)` and `add<uint64>(lhs, rhs)`
- filter predicate evaluation for `is_not_null(column(index))` with `int32` predicate input in the first filter slice, `date32` and `timestamp_tz(us)` predicate input in the temporal slices, `decimal128` predicate input in the first decimal slice, `float64` predicate input in the first float64 slice, `uint64` predicate input in the first unsigned slice, `utf8` predicate input in the first collation slice, and `json` predicate input in the first JSON slice
- nested follow-on coverage reuses existing expression family scope through passthrough `column(index)` over `struct<a:int32, b:int32?>`, `map<int32, int32?>`, and `dense_union<i:int32, n:int32?>`; nested predicates, casts, and ordering remain out of scope
- direct attachment to the adopted upstream `SourceOperator`, `PipeOperator`, and `SinkOperator` traits, with expressions and filter predicates kept as operator-local evaluators and schema helpers
- reserve-first admission around operator-owned output materialization
- governed-batch handoff and live-claim tracking through the source -> projection or filter -> sink paths
- local execution snapshots and checked-in fixtures that keep rows, errors, and ownership outcomes reviewable

These boundaries are justified by `docs/spec/milestone-1-expression-projection.md`, `docs/spec/first-filter-is-not-null.md`, `docs/spec/type-system.md`, `docs/contracts/data.md`, `docs/contracts/runtime.md`, `tests/conformance/expression-projection-slice.md`, `tests/conformance/first-filter-is-not-null-slice.md`, `docs/design/first-temporal-semantic-slice.md`, `tests/conformance/first-temporal-date32-slice.md`, `docs/design/first-temporal-timestamp-tz-slice.md`, `tests/conformance/first-temporal-timestamp-tz-slice.md`, `docs/design/first-decimal-semantic-slice.md`, `tests/conformance/first-decimal128-slice.md`, `docs/design/first-float64-ordering-slice.md`, `tests/conformance/first-float64-ordering-slice.md`, `docs/design/first-unsigned-arithmetic-slice.md`, `tests/conformance/first-unsigned-arithmetic-slice.md`, `docs/design/first-json-semantic-slice.md`, `tests/conformance/first-json-slice.md`, `docs/design/first-struct-aware-handoff-slice.md`, `tests/conformance/first-struct-slice.md`, `docs/design/first-map-aware-handoff-slice.md`, `tests/conformance/first-map-slice.md`, `docs/design/first-union-aware-handoff-slice.md`, and `tests/conformance/first-union-slice.md`.

They are not yet a general shared kernel API. They exist to prove end-to-end paths where shared specs, admission rules, handoff ownership, and local harness evidence all line up before broader operators or adapter-driven execution are introduced.

## Architectural Rules

- Specs own semantics.
- Contracts own boundaries.
- Adapters should be thin.
- Harnesses should be able to test specs independently of future kernels.

## Kernel Expansion Gate

After the current milestone-1 slices, layer 3 should grow only when:

- the first thin end-to-end differential slice already exists as reviewable evidence
- the proposed kernel growth is one narrow boundary with docs and harness coverage already named
- the resulting issue can show which shared surfaces change and which stay out of scope

## Differential Drift-Report Policy

- machine-readable JSON `drift-report` sidecars are required for differential slices that check in `drift-report` evidence
- the shared engine-pair carrier boundary lives in `tests/differential/drift-report-carrier.md`
- the accepted policy record is `docs/decisions/0002-drift-report-sidecar-policy.md`
- differential evidence remains one engine pair (or one explicitly documented parity subject) per checked-in drift-report artifact; merged multi-pair summaries remain out of scope until a follow-on decision says otherwise
