# Runtime Contract

Direction: use broken-pipeline ideas as inspiration for the runtime protocol.

This means explicit stage boundaries, observable handoff points, and a runtime model designed around interruption, backpressure, and partial failure. It does not mean copying donor runtime code or preserving donor APIs.

## Working Principles

- work moves through explicit pipeline fragments or stages
- batch handoff is observable and measurable
- backpressure is a first-class concern
- cancellation and error propagation must be part of the contract
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

## Open Questions

- TODO: choose the primary handoff model for the first harness slice
- TODO: define which runtime events must be observable in tests
- TODO: define error taxonomy and propagation guarantees
- TODO: define memory ownership transfer rules between stages
- TODO: decide how exchange, spill, and retry behaviors fit into the contract
- TODO: decide what part of the runtime is shared versus adapter-owned

## Initial Boundary

For now, the runtime contract exists to constrain harness and adapter design. It should become precise before any real kernel scheduling code is introduced.
