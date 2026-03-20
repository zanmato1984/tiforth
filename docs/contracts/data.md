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
Detailed first dictionary-aware handoff slice rationale lives in `docs/design/first-dictionary-aware-handoff-slice.md`.
Detailed spill and retry runtime mapping rationale lives in `docs/design/spill-retry-runtime-mapping.md`.
Detailed nested plus decimal and temporal metadata boundary rationale lives in `docs/design/milestone-1-nested-decimal-temporal-boundary.md`.
Detailed first nested-aware handoff slice rationale lives in `docs/design/first-nested-aware-handoff-slice.md`.
Detailed first struct-aware handoff slice rationale lives in `docs/design/first-struct-aware-handoff-slice.md`.
Detailed first temporal semantic slice rationale lives in `docs/design/first-temporal-semantic-slice.md`.
Detailed first decimal semantic slice rationale lives in `docs/design/first-decimal-semantic-slice.md`.

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
- the current milestone-1 executable projection slice does not require dictionary-encoded arrays as stage-handoff input
- when an adapter, source, or engine-native surface encounters dictionary-backed data outside the first dictionary-aware slice below, it should normalize that data to the equivalent decoded logical array before the data enters the current executable slice or checked-in differential evidence
- the current milestone-1 expression-projection slice must not emit dictionary-encoded output arrays, so its governed claims describe the ordinary decoded Arrow buffers that remain reachable after handoff
- the first dictionary-aware shared handoff slice is defined in `docs/design/first-dictionary-aware-handoff-slice.md`: passthrough of `dictionary<int32, int32>` through `column(index)` with separate index-domain and values-domain claim ownership units when lifetimes can diverge

## Milestone-1 Nested And Decimal/Temporal Metadata Boundary

Milestone 1 keeps nested, decimal, and temporal families outside the current shared execution boundary.

For the shared data contract, that means:

- stage handoff in the current shared slice does not require direct support for nested arrays (`list`, `large_list`, `fixed_size_list`, `struct`, `map`, `union`, or nested combinations)
- decimal and temporal arrays are not required inputs or outputs for the milestone-1 executable projection slice or first differential artifacts
- milestone-1 shared data handling does not require interpreting decimal precision and scale metadata or temporal unit and timezone metadata
- when adapters or sources encounter those out-of-scope families for a milestone-1 request, they should either normalize to the current supported logical slice before handoff or surface an explicit unsupported outcome under the current adapter contract
- the first nested-aware shared handoff checkpoint beyond milestone 1 is defined in `docs/design/first-nested-aware-handoff-slice.md`: passthrough of `list<int32>` through `column(index)` with separate parent-domain and child-domain claim ownership units when lifetimes can diverge
- the first struct-aware shared handoff checkpoint beyond milestone 1 is defined in `docs/design/first-struct-aware-handoff-slice.md`: passthrough of `struct<a:int32, b:int32?>` through `column(index)` with separate parent-domain and per-child-domain claim ownership units when lifetimes can diverge

This boundary keeps milestone-1 contracts aligned with the current `int32` semantic core without pretending broader family support already exists.

## First Struct Follow-On Checkpoint

Issue #226 now fixes one post-milestone nested checkpoint in
`docs/design/first-struct-aware-handoff-slice.md`.

For that first struct slice, shared data-contract scope is intentionally
narrow:

- shared nested handoff admits `struct<a:int32, b:int32?>` arrays only
- this checkpoint is limited to passthrough `column(index)` over that struct shape
- claim tracking keeps parent struct validity buffers and each child field
  payload in separate ownership units
- shared differential comparison for this slice uses canonical object carriers
  with stable field order plus explicit top-level and child-field nullability
- nested predicates, nested compute families, and broader nested logical types
  remain out of scope until follow-on issues

## First Temporal Follow-On Checkpoint

Issue #174 now fixes one post-milestone temporal checkpoint in
`docs/design/first-temporal-semantic-slice.md`.

For that first temporal slice, shared data-contract scope is intentionally
narrow:

- shared temporal handoff admits logical `date32` arrays only
- shared handoff does not require batch-carried timezone metadata in this slice
- adapters may keep engine-local date and timezone session settings, but shared
  differential comparison for this slice is over normalized `date32` day-domain
  values plus nullability
- other temporal logical families remain out of scope until a follow-on issue

## First Decimal Follow-On Checkpoint

Issue #189 now fixes one post-milestone decimal checkpoint in
`docs/design/first-decimal-semantic-slice.md`.

For that first decimal slice, shared data-contract scope is intentionally
narrow:

- shared decimal handoff admits logical `decimal128` arrays only
- decimal field precision and scale metadata must remain explicit and preserved
  across passthrough and first-slice filter outputs
- shared differential comparison for this slice is over canonical decimal
  string values plus nullability, without implicit rescaling
- other decimal logical families remain out of scope until a follow-on issue

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

## First In-Contract Exchange Ownership Follow-On

Issue #169 now fixes one post-milestone exchange checkpoint in
`docs/design/first-in-contract-exchange-slice.md`.

For that first exchange slice, buffered incoming claims remain attached to their
batch envelope while queued, and exchange-owned resident buffers follow the same
reserve-before-allocate plus claim-lifecycle rules already defined above.

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

## Milestone-1 Spill Representation Boundary

- operator-managed spill is outside Arrow's transparent allocator behavior and outside the canonical live-batch envelope while the spilled bytes are non-resident
- `claims[]` describe only admitted resident bytes that remain reachable from live batches or retained local state
- spill metadata and rehydration buffers that remain resident are still governed bytes and must follow reserve-before-allocate admission
- rehydrating spilled state is a fresh reserve-before-allocate path and may attach new claims only for newly resident governed bytes
- a stage must not keep claim bytes attached to an outgoing batch for buffers that were fully spilled and are no longer reachable from that batch

## Open Questions

- TODO: extend nested-family shared slices beyond the first `list<int32>` and first `struct<a:int32, b:int32?>` passthrough checkpoints, including `map`, `union`, and nested compute semantics
- TODO: extend temporal support beyond the first `date32` checkpoint to unit/timezone-sensitive families
- TODO: extend decimal support beyond the first `decimal128` checkpoint, including arithmetic, cast or rescale policy, and decimal-family expansion
- TODO: decide how later off-heap state, if any, should be represented beyond the milestone-1 spilled-bytes-outside-live-envelope boundary
- TODO: decide what later adapter-visible or serialized carrier should expose full `batch_id`, `origin`, and `claims[]` detail beyond the current local `GovernedBatch` state and `LocalExecutionSnapshot` event records
- TODO: decide whether any later milestone needs direct host-allocator-backed Arrow buffers or imported immutable buffer bridges beyond the reserve-first, claim-carrying milestone-1 contract

## Initial Boundary

For milestone 1, this document now fixes the semantic batch envelope, ownership-transfer rules, current local Rust-side carrier, and the normalization-first dictionary boundary for the executable projection slice; it also names the first post-milestone-1 dictionary-aware handoff checkpoint for passthrough `dictionary<int32, int32>` columns, the first post-milestone-1 nested-aware handoff checkpoint for passthrough `list<int32>` columns, the first post-milestone-1 struct-aware handoff checkpoint for passthrough `struct<a:int32, b:int32?>` columns, the first post-milestone in-contract exchange ownership checkpoint under `docs/design/first-in-contract-exchange-slice.md`, the first temporal semantic checkpoint under `docs/design/first-temporal-semantic-slice.md`, and the first decimal semantic checkpoint under `docs/design/first-decimal-semantic-slice.md`. Additional nested-family expansion, broader decimal and temporal shared slices, richer adapter-visible claim serialization, and imported-buffer work remain open.
