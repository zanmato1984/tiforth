# Milestone-1 Operator And Expression Attachment To The Adopted Runtime Contract

Status: issue #86 design checkpoint

Verified: 2026-03-18

Related issues:

- #9 `design: tiforth dependency boundary over broken-pipeline-rs`
- #10 `milestone-1: first Arrow-bound operator and expression slice`
- #19 `design: define milestone-1 Arrow batch handoff and memory-ownership contract`
- #21 `milestone-1: implement claim-carrying batch handoff in tiforth-kernel`
- #78 `design: define current milestone-1 kernel boundary`
- #86 `design: define milestone-1 operator and expression attachment to adopted runtime contract`
- #123 `design: define milestone-1 spill and retry runtime mapping`

## Question

How should milestone-1 `tiforth` operators and expressions attach to the adopted `broken-pipeline-rs` Arrow-bound runtime contract without renaming upstream runtime states?

## Inputs Considered

- `README.md`
- `docs/architecture.md`
- `docs/contracts/data.md`
- `docs/contracts/runtime.md`
- `docs/spec/milestone-1-expression-projection.md`
- `crates/tiforth-kernel/src/lib.rs`
- `crates/tiforth-kernel/src/operators.rs`
- `crates/tiforth-kernel/src/expr.rs`
- `crates/tiforth-kernel/src/projection.rs`
- `crates/tiforth-kernel/src/snapshot.rs`
- `crates/tiforth-kernel/tests/expression_projection.rs`
- issue #86

## Design Summary

For milestone 1, `tiforth` attaches to the adopted runtime by implementing the upstream operator traits directly over `TiforthTypes` and by keeping expressions as operator-internal evaluators instead of runtime participants.

The current attachment pattern is:

- runtime-entered operators implement `SourceOperator<TiforthTypes>`, `PipeOperator<TiforthTypes>`, or `SinkOperator<TiforthTypes>` directly and return upstream `OpOutput<TiforthBatch>` values
- expression nodes such as `Expr` and `ProjectionExpr` stay inside `tiforth` operator logic for schema derivation and batch evaluation; they do not own scheduling, continuation, or handoff states
- `RuntimeContext` is milestone-1 attachment glue carried through upstream `TaskContext`; it supplies admission control, batch-claim tracking, and event recording without renaming `TaskStatus` or `OpOutput`
- `TiforthBatch`, `OwnershipToken`, and `LocalExecutionSnapshot` remain `tiforth`-owned data, ownership, and observability helpers around the adopted runtime payload rather than replacement runtime contracts

This keeps the upstream runtime vocabulary stable while letting `tiforth` own the semantics that are specific to its kernel slice.

## Operators Enter Through Upstream Traits

The runtime-facing operator surface for milestone 1 is the upstream `TiforthTypes` specialization:

- `StaticRecordBatchSource` implements `SourceOperator<TiforthTypes>`
- `ProjectionPipe` implements `PipeOperator<TiforthTypes>`
- `CollectSink` implements `SinkOperator<TiforthTypes>`
- runtime-visible return values stay `OpOutput<TiforthBatch>`
- scheduler and context access stay `TaskContext`

Milestone 1 should not introduce parallel traits such as a `tiforth`-renamed source, pipe, sink, or task-status API for this same slice.

## Expressions Stay Inside Operators

`Expr` and `ProjectionExpr` are not runtime protocol nodes.

They define:

- field derivation over input schemas
- row-wise batch evaluation used by projection operators
- the current milestone-1 type and nullability behavior for `column`, `literal<int32>`, and `add<int32>`

They do **not**:

- yield or block independently
- receive scheduler callbacks
- carry alternate runtime-state enums

Any future expression family should continue to attach through operator implementations unless a later accepted issue explicitly changes the shared contract.

## Local Glue Travels Through `TaskContext`

When milestone-1 operators need `tiforth`-specific state at runtime, they should recover it from the upstream typed context carrier instead of wrapping or replacing the runtime protocol.

For the current slice, `RuntimeContext` is that local glue and it is the required `TaskContext<TiforthTypes>` payload. It owns:

- admission controller access
- claim creation and governed-batch tracking
- runtime event recording for local harness evidence

## Tiforth-Owned Helpers Surround The Payload, Not The Protocol

`TiforthBatch` and `OwnershipToken` add ownership bookkeeping around the adopted `Batch` payload so the source -> projection -> sink slice can preserve live claims.

`LocalExecutionSnapshot` and `LocalExecutionFixture` translate local recorders into stable conformance evidence, with adapter-visible export bounded by `docs/design/adapter-visible-runtime-event-carrier.md`.

These helpers are part of `tiforth`'s kernel, data, and harness boundaries, but they are not shared replacements for upstream task control, scheduling, or handoff enums.

## Upstream-Owned Meanings Stay Upstream-Owned

Milestone 1 continues to adopt by name and meaning:

- `TaskStatus::{Continue, Blocked, Yield, Finished, Cancelled}`
- `OpOutput::{PipeSinkNeedsMore, PipeEven, SourcePipeHasMore, Blocked, PipeYield, PipeYieldBack, Finished, Cancelled}`

`Tiforth` may classify its own compute, admission, and ownership errors, but it should surface them through the adopted result and terminal paths instead of inventing alternate runtime states.

## Why This Boundary Holds For Milestone 1

- it matches the current executable kernel shape without inventing a wider API than the repo can test
- it keeps expressions pure and reviewable while operators remain the only runtime-reentered units
- it leaves adapter and harness growth room without forcing a shared `tiforth` runtime facade too early
- the existing `tiforth-kernel` crate-root re-export of `ArrowTypes` and `Batch` already covers the current ergonomic need without hiding that the underlying contract stays upstream-owned
- it preserves the accepted dependency boundary from `docs/design/broken-pipeline-boundary.md` and the current kernel boundary from `docs/architecture.md`

## Deferred Questions

- how later runtime-expansion work should attach exchange implementations beyond the first in-contract slice in `docs/design/first-in-contract-exchange-slice.md`
- whether a later milestone needs a shared callback or streaming event API after the current fixture-translation boundary
- whether a later milestone needs a broader operator-construction surface once kernels expand beyond the current source -> projection -> sink slice

## Result

For milestone 1, the shared runtime contract ends at the adopted upstream Arrow operator and task vocabulary. `Tiforth` begins at operator implementation, expression evaluation, admission, ownership tracking, and local observability layered on top of that vocabulary. This is the boundary later docs and implementation work should assume unless another accepted issue changes it.
