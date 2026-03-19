# First In-Contract Exchange Slice

Status: issue #169 design checkpoint

Verified: 2026-03-19

Related issues:

- #92 `design: define adapter-local runtime orchestration boundary`
- #125 `design: define milestone-1 exchange runtime mapping boundary`
- #147 `design: define first differential filter slice and adapter boundary for is_not_null`
- #169 `design: define first in-contract exchange slice boundary`

## Question

What is the first shared runtime slice that should bring exchange in-contract
without replacing adopted `broken-pipeline-rs` runtime-state meanings or
widening milestone-1 semantics?

## Inputs Considered

- `docs/architecture.md`
- `docs/contracts/data.md`
- `docs/contracts/runtime.md`
- `docs/design/exchange-runtime-mapping.md`
- `docs/design/adapter-runtime-orchestration-boundary.md`
- `tests/conformance/expression-projection-slice.md`
- `tests/conformance/first-filter-is-not-null-slice.md`
- `tests/differential/first-expression-slice.md`
- `tests/differential/first-filter-is-not-null-slice.md`
- issue #169

## Design Summary

The first in-contract exchange slice is one narrow local queue boundary:

- path shape: `source -> projection or filter -> exchange -> sink`
- one producer stage and one consumer stage
- in-process, no network transport
- no repartition, merge, or row reordering

This keeps exchange scoped as a runtime handoff and backpressure boundary while
reusing the current semantic families and claim-carrying batch envelope.

## Runtime Boundary

For this first slice, exchange:

- accepts and emits the canonical semantic batch envelope from
  `docs/contracts/data.md` (adopted `Batch` payload plus `batch_id`, `origin`,
  and live `claims[]`)
- preserves schema, row values, and row order within and across forwarded
  batches
- may retain batches in a bounded queue when downstream cannot yet accept more
- does not split, merge, or rematerialize buffered batches

## Backpressure Mapping On Adopted Runtime States

The first in-contract exchange slice must map backpressure through adopted
runtime meanings only:

- immediate forward handoff continues to use existing pipe-forward outcomes
  (for example, `PipeEven`) without introducing exchange-specific states
- queue saturation or downstream unavailability maps to adopted blocked
  semantics (`Blocked` via await or resume wiring), not a new exchange state
- downstream demand while no batch is currently ready to emit still maps through
  existing "needs more input" semantics (`PipeSinkNeedsMore`)
- upstream completion with an empty exchange queue maps to ordinary `Finished`
  behavior
- cancellation and terminal errors propagate through adopted `Cancelled` or
  error outcomes

## Admission And Ownership Rules

Exchange keeps reserve-first and claim-carrying ownership rules unchanged:

- exchange-local memory growth (queue metadata or other exchange-owned resident
  state) must reserve before allocation
- forwarded incoming batch claims remain attached while batches are buffered
- exchange may create new claims only for newly materialized exchange-owned
  resident buffers
- a buffered batch claim may be dropped only when that batch is no longer
  reachable from outgoing batches or retained exchange state
- cancellation, drain teardown, and terminal errors must release any remaining
  exchange-owned claims before final consumer release

## Required Harness Coverage

The first in-contract exchange slice requires named coverage before any broader
runtime expansion:

- conformance coverage in `tests/conformance/first-exchange-slice.md`
- differential coverage in `tests/differential/first-exchange-slice.md`

That coverage must include at least:

- passthrough single-batch schema and value preservation
- passthrough multi-batch FIFO ordering
- bounded-queue blocked then resumed handoff
- upstream-finished drain behavior
- cancellation teardown with buffered-batch claim release
- parity checks that exchange-enabled runs preserve the established
  `first-expression-slice` and `first-filter-is-not-null-slice` case outcomes

## Out Of Scope For This Checkpoint

- repartitioning strategies or hash-range exchange semantics
- multi-producer or multi-consumer exchange topologies
- cross-node or adapter-transport protocols
- exchange-managed spill policy beyond existing reserve-first and claim rules
- new shared callback-streaming event APIs

## Result

`tiforth` now has a first post-milestone exchange checkpoint that is specific
enough to implement and test without widening semantic families or replacing
adopted runtime states.
