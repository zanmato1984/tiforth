# Data Contract

Direction: use Apache Arrow concepts as the default memory and data interchange contract.

This is a direction, not a frozen ABI.

## Working Principles

- columnar batches are the unit of exchange
- schema is explicit and travels with enough metadata to validate semantics
- nullability is first-class
- vectors should be usable by harnesses and future kernels without engine-specific wrappers
- zero-copy reuse is preferred when ownership rules allow it

## Scope

This contract should eventually cover:

- scalar columns
- selection or validity state
- batch schema metadata
- basic encoding expectations
- handoff rules between runtime stages

## Non-Goals

- inventing a new memory format when Arrow already covers the need
- freezing every physical detail before the first harness exists
- encoding engine-specific quirks directly into the shared contract

## Milestone-1 Allocation Contract

For milestone 1, `tiforth` does **not** need to route its internal Arrow allocations directly through a host allocator.

The required contract is:

- before allocating internal memory for Arrow-backed operator state or output, `tiforth` estimates the additional bytes required for the intended peak live growth and reports that request to the host
- the request is for additional admitted bytes, not a retroactive accounting correction after allocation has already happened
- the host either admits or denies that reservation request as an all-or-nothing control point
- if the host denies the request, `tiforth` must fail that allocation or execution path or spill and retry under operator policy, and it must not allocate first
- if the host admits the request, `tiforth` may allocate internally and then materialize ordinary Arrow buffers
- if actual growth uses fewer admitted bytes than reserved, `tiforth` returns the excess through `shrink`
- if later growth needs more bytes, `tiforth` must obtain another reservation before that growth happens
- when admitted buffers or operator state stop being resident, `tiforth` returns those bytes through `shrink` or final `release`

This keeps host memory control explicit while allowing milestone-1 data construction to use `tiforth`-owned internal allocation paths.

Detailed admission semantics live in `docs/design/host-memory-admission-abi.md`.
Detailed batch handoff and ownership rationale lives in `docs/design/arrow-batch-handoff-ownership.md`.
Detailed dictionary-encoding rationale lives in `docs/design/dictionary-encoding-boundary.md`.

### Milestone-1 Projection `Int32Array` Estimate Rule

For the current milestone-1 Rust projection slice, computed `Int32Array` outputs use one concrete reserve-first estimate and one concrete retained-byte reconciliation rule:

- before materialization, each computed projection column reserves `rows * 4 + ceil(rows / 8)` bytes
- that estimate covers one `i32` value slot per row plus one worst-case validity bitmap bit per row
- after materialization, the retained live bytes for that column reconcile to `rows * 4`, plus `ceil(rows / 8)` only when the finished output still has at least one live null bit to carry
- if the finished output has no nulls, the producer `shrink`s exactly the reserved bitmap bytes before sealing the batch for handoff
- if the finished output has one or more nulls, milestone 1 keeps the full estimate attached to the emitted claim and performs no pre-emit `shrink`

This fixes the current milestone-1 rule for `literal<int32>` and `add<int32>` projection outputs without claiming it as a universal Arrow formula for later operators or types.

The current local executable evidence lines up with that rule:

- direct non-null literal and other non-null computed outputs reserve the full estimate, then `shrink` the bitmap portion before emit
- direct `NULL` literals and nullable computed `add<int32>` outputs keep the full estimate because the emitted array still carries a validity bitmap
- multi-computed outputs apply the same rule independently per computed projection column, so one emitted batch may carry both a shrunk non-null claim and an unshrunk nullable claim at the same time

## Dictionary Encoding Boundary

Dictionary encoding is a physical Arrow representation choice, not a new shared semantic type family.

For the shared contract, that means:

- specs, harnesses, and adapters reason about the underlying logical value type and nullability rather than treating `dictionary<index, values>` as a distinct semantic type
- the current milestone-1 shared-kernel and local conformance slice do not accept dictionary-encoded arrays as a required stage-handoff format
- when an adapter, source, or engine-native surface encounters dictionary-backed data for a milestone-1 case, it should normalize that data to the equivalent decoded logical array before the data enters the current shared slice or checked-in differential evidence
- the current milestone-1 expression-projection slice must not emit dictionary-encoded output arrays, so its governed claims describe the ordinary decoded Arrow buffers that remain reachable after handoff
- if a later issue allows dictionary-backed arrays to cross the shared contract directly, that issue should define the first affected slice, the supported logical families, and how ownership claims attach to dictionary values buffers versus index buffers when those lifetimes can diverge

## Milestone-1 Canonical Batch Envelope

Milestone 1 keeps the adopted upstream Arrow batch surface. `tiforth` does **not** introduce a second public data payload type above `broken_pipeline::traits::arrow::Batch`.

The canonical semantic envelope for one stage handoff is:

- `batch`: the adopted upstream Arrow `Batch`, currently `Arc<RecordBatch>`
- `batch_id`: a producer-local monotonic identifier for observability and debugging
- `origin`: the producing query, stage, and operator identity
- `claims[]`: zero or more ownership claims that keep governed bytes live while the batch remains reachable

This is a semantic envelope, not yet a frozen Rust struct or FFI wire layout. The metadata may travel in sidecar runtime state rather than inside the `RecordBatch` itself, so long as tests, adapters, and future kernels can reason about the same fields.

Each claim should identify one independently releasable ownership unit. At minimum, a claim needs equivalent information to:

- the consumer handle whose admitted balance currently covers those bytes
- the exact admitted bytes that remain live because of this batch
- a debug label or ownership domain that lets tests and adapters explain the claim

A claim must not aggregate bytes whose lifetimes can diverge independently. If one owned buffer may be dropped, forwarded, or spilled while another remains live, those bytes should be tracked as separate claims.

## Ownership Transfer Across Stage Handoff

Stage handoff transfers the semantic envelope, not just the raw `RecordBatch`.

- before emission, a producer stage must finish all reserve-first accounting for any newly materialized governed buffers
- if construction reserved conservative peak bytes, the producer must `shrink` down to the exact retained live bytes before sealing the batch for handoff
- on successful handoff, responsibility for the attached claims moves from the producing task's local scope to the live batch envelope; no fresh host admission decision is required merely because the next stage receives the batch
- downstream stages may forward incoming claims unchanged when they reuse incoming columns or buffers without copying
- downstream stages must create new claims only for newly materialized governed buffers that they own
- a stage may drop an incoming claim from its outgoing batch only when the corresponding bytes are no longer reachable from the outgoing batch or any retained local state

This allows zero-copy passthrough for shared Arrow arrays while keeping exactly one live ownership claim for every governed buffer that remains reachable.

## Milestone-1 Local Runtime Carrier

For the current Rust kernel slice, local runtime state carries the semantic batch envelope through:

- `GovernedBatch`: the adopted upstream `Batch` payload plus local `batch_id`, `origin`, and per-column live claims while the batch remains reachable
- `LocalExecutionSnapshot.runtime_events[]`: batch lifecycle records that let local tests and harness scaffolding correlate `batch_id`, `origin`, and aggregate claim counts across emit, handoff, release, and terminal outcomes

This settles the local Rust-side carrier for milestone 1 without freezing a later adapter or FFI layout.

## `shrink` And `release` For Live Batch Claims

- `shrink` applies only while `tiforth` still owns the bytes locally or after those bytes have been detached from every live batch and retained state
- once a claim is sealed into an emitted batch envelope, milestone 1 treats that claim's byte count as fixed until the claim is either forwarded unchanged or finally released
- `release` is terminal and may happen only after the consumer has no live local state and no live batch claims remaining
- the final `release` for batch-tied bytes happens when the last batch carrying that claim is dropped by a downstream stage, sink, or teardown path
- double release, shrinking a live claim in place, or releasing bytes still reachable from a live batch is a contract violation

## Open Questions

- TODO: define the first later slice, if any, that allows dictionary-encoded arrays to cross the shared contract without prior normalization
- TODO: specify required support for nested types, if any, in the first milestone
- TODO: specify decimal and temporal metadata requirements
- TODO: decide how spill or off-heap behavior is represented, if at all, given that spill is operator-managed rather than transparent inside Arrow allocation paths
- TODO: decide what later adapter-visible or serialized carrier should expose full `batch_id`, `origin`, and `claims[]` detail beyond the current local `GovernedBatch` state and `LocalExecutionSnapshot` event records
- TODO: decide whether any later milestone needs direct host-allocator-backed Arrow buffers or imported immutable buffer bridges beyond the reserve-first, claim-carrying milestone-1 contract

## Initial Boundary

For milestone 1, this document now fixes the semantic batch envelope, ownership-transfer rules, current local Rust-side carrier, and the rule that current shared slices normalize dictionary-backed data before contract handoff. Later adapter-visible layouts, richer claim serialization, direct dictionary-aware handoff, and imported-buffer work remain open.
