# Milestone-1 Arrow Batch Handoff And Ownership

Status: issue #19 design checkpoint

Verified: 2026-03-17

Related issues:

- #8 `design: host memory admission ABI for tiforth`
- #10 `milestone-1: first Arrow-bound operator and expression slice`
- #19 `design: define milestone-1 Arrow batch handoff and memory-ownership contract`

## Question

Once a milestone-1 operator materializes governed Arrow buffers, what exactly crosses a runtime stage boundary, who owns those bytes after handoff, and when must `shrink` or `release` happen?

## Inputs Considered

- `docs/contracts/data.md`
- `docs/contracts/runtime.md`
- `docs/design/host-memory-admission-abi.md`
- `docs/spec/milestone-1-expression-projection.md`
- `tests/conformance/expression-projection-slice.md`
- issue #19

## Design Summary

Milestone 1 settles the contract as follows:

- the data payload remains the adopted upstream Arrow `Batch`, currently `Arc<RecordBatch>`
- the canonical semantic handoff envelope is `batch + batch_id + origin + claims[]`
- `claims[]` carry the live admitted ownership units that keep governed bytes alive across stage boundaries
- successful handoff transfers those claims with the batch; it does not perform a fresh host admission decision and it does not require a fifth host-ABI operation
- `shrink` happens before handoff or after bytes detach from every live batch; `release` happens only when no live batch or retained state still references the bytes
- tests and adapters must be able to observe admit, deny, emit, handoff, shrink, release, and terminal runtime outcomes
- adapters must at least distinguish `memory_admission_denied`, `memory_allocation_failed`, and `ownership_contract_violation`

## Canonical Envelope

The canonical milestone-1 handoff envelope is semantic, not a frozen public wrapper type.

Required fields:

- `batch`: adopted upstream Arrow `Batch`
- `batch_id`: producer-local monotonic identifier for correlation
- `origin`: producing query, stage, and operator identity
- `claims[]`: independently releasable ownership units

Each claim ties live bytes back to one admitted consumer balance. Claims must be fine-grained enough that bytes with different future lifetimes are not forced to release together.

## Ownership Transfer Rules

- a producing stage finishes reserve-first accounting before it emits the batch
- any conservative over-reservation is `shrink`ed down to the exact retained live bytes before the batch is sealed for handoff
- newly materialized governed buffers add new claims under the producer's consumers
- reused zero-copy input buffers forward their incoming claims unchanged
- successful handoff moves claim responsibility from the producer's local scope to the live batch envelope
- downstream stages may forward incoming claims unchanged or add new claims, but they may drop claims only when the referenced bytes are no longer reachable from outgoing batches or retained state

This keeps ownership stable without inventing a second runtime batch payload type.

## `shrink` And `release`

- `shrink` is valid for over-reservation, compaction, spill, or free only while `tiforth` still owns the bytes locally or after those bytes detach from every live batch
- `release` is terminal and must wait until no live batch claim or retained operator state still references the consumer's bytes
- the final holder of a live batch claim triggers the release path on normal sink completion, downstream discard, cancellation, or error teardown
- shrinking or releasing bytes that remain reachable from a live batch is an ownership-contract violation

## Observable Events

Milestone 1 requires observable meanings for at least:

- `consumer_opened`
- `reserve_admitted`
- `reserve_denied`
- `consumer_shrunk`
- `batch_emitted`
- `batch_handed_off`
- `batch_released`
- `consumer_released`
- terminal runtime outcome: `finished`, `cancelled`, or `error`

The exact event carrier may differ across local tests, harness snapshots, or later adapter APIs, but the semantics above should not drift.

## Minimal Adapter-Visible Errors

- `memory_admission_denied`: host denied reserve-before-allocate growth
- `memory_allocation_failed`: local allocation or Arrow materialization failed after admission succeeded
- `ownership_contract_violation`: `tiforth` attempted an illegal claim lifecycle transition

This note does not broaden operator-specific compute errors; those remain ordinary operator failures.

## Not Settled Here

This design intentionally does **not** settle:

- the concrete Rust struct, FFI wire layout, or snapshot schema used to carry `batch_id`, `origin`, and `claims[]`
- imported immutable foreign-buffer bridges or direct host-allocator-backed Arrow buffers
- full metrics-field registries for every host embedding
- spill-specific operator algorithms beyond the ownership and accounting rules they must obey

## Follow-Up After Issue #19

The current milestone-1 kernel slice still needs a separate implementation issue to replace the issue #10 build-time-only release behavior with claim-carrying handoff and final-drop release hooks.

That follow-up should implement this contract, not reopen it.
