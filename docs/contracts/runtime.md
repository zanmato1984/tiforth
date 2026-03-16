# Runtime Contract

Direction: use broken-pipeline ideas as inspiration for the runtime protocol.

This means explicit stage boundaries, observable handoff points, and a runtime model designed around interruption, backpressure, and partial failure. It does not mean copying donor runtime code or preserving donor APIs.

## Working Principles

- work moves through explicit pipeline fragments or stages
- batch handoff is observable and measurable
- backpressure is a first-class concern
- cancellation and error propagation must be part of the contract
- memory admission must be explicit before internal growth
- scheduling choices should be separable from semantic correctness

## Scope

This contract should eventually describe:

- stage inputs and outputs
- pull, push, or mixed handoff semantics
- lifecycle events
- cancellation behavior
- metrics and tracing hooks
- resource ownership at runtime boundaries

## Non-Goals

- locking the scheduler design in this skeleton
- embedding engine-specific control flow into shared runtime contracts
- pretending the data contract and runtime contract can be designed independently

## Milestone-1 Admission Contract

For milestone 1:

- before allocating operator-owned mutable state or Arrow-backed output, `tiforth` reports intended memory to the host
- host admission is a precondition for continued execution on that path
- if the host denies the request, `tiforth` fails the pending allocation or execution path and does not allocate speculatively
- after admission, `tiforth` may allocate internally; direct host allocator routing is not required for milestone 1
- spill remains operator-managed above Arrow internals

## Open Questions

- TODO: choose the primary handoff model for the first harness slice
- TODO: define which runtime events must be observable in tests
- TODO: define error taxonomy and propagation guarantees
- TODO: define memory ownership transfer rules between stages, including reservation state for internally allocated buffers and any future imported external buffers
- TODO: define how the host reservation or admission boundary is presented to the runtime (per query, stage, or operator) and what request, deny, shrink, and release guarantees are required when the host lives in Go, C++, or Rust
- TODO: define operator-managed spill triggers and handoff rules; do not assume transparent spill inside Arrow allocation paths
- TODO: decide how exchange, spill, and retry behaviors fit into the contract once operator-managed spill is made explicit
- TODO: decide what part of the runtime is shared versus adapter-owned
- TODO: decide whether any later milestone needs direct host allocator routing as a separate capability beyond reserve-first admission control

## Initial Boundary

For now, the runtime contract exists to constrain harness and adapter design. It should become precise before any real kernel scheduling code is introduced.
