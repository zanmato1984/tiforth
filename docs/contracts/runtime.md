# Runtime Contract

<<<<<<< HEAD
Direction: `tiforth` directly adopts and exposes the Arrow-bound runtime contract provided by `broken-pipeline-rs`.

`tiforth` is not inventing an independent runtime contract for milestone 1. It is an operator and expression library layered on top of the adopted `broken-pipeline-rs` runtime substrate.

This means explicit stage boundaries, observable handoff points, and a runtime model designed around interruption, backpressure, and partial failure. It does not mean preserving source-level compatibility with the original C++ `broken-pipeline`, nor treating the current C++ surface as the active design center.

## Broken-Pipeline Semantics To Preserve
=======
Direction: `tiforth` adopts and exposes the Arrow-bound runtime contract from `broken-pipeline-rs` as its primary shared runtime contract.

`tiforth` is not defining an independent runtime contract here. The shared runtime surface is the Arrow-bound Broken Pipeline contract already present in the upstream Rust port. `tiforth`'s role is to provide operators, expressions, and higher-level composition on top of that adopted contract.

`broken-pipeline-schedule` sits outside the shared contract. Within `tiforth`, it is reserved for local testing and harness work only, and this is not expected to change.

The original C++ Broken Pipeline repository remains relevant only as provenance and lineage for the adopted model. It is not the active design center for `tiforth`.

## Adopted Runtime Shape
>>>>>>> b14e856 (docs: adopt broken-pipeline-rs runtime framing)

- small-step, resumable task execution that a scheduler can re-enter
- explicit runtime control states for continue, blocked, yield, finished, and cancelled work, or close semantic equivalents already present in the adopted contract
- explicit stage boundaries with observable handoff points
<<<<<<< HEAD
- end-of-stream drain or flush behavior as part of the contract
- scheduler-agnostic correctness, so runtime semantics do not depend on one particular executor implementation

## Current Adoption Stance

- `broken-pipeline-rs` is the adopted runtime substrate
- `tiforth` provides operators and expressions on top of that substrate
- `broken-pipeline-schedule` is test / harness-only, and this is not expected to become the production contract center later
- the original C++ `broken-pipeline` is lineage and semantic reference only
=======
- Arrow-bound batch handoff as the primary data-bearing runtime surface
- end-of-stream drain or flush behavior as part of the runtime contract
- host-orchestration hooks around the main streaming path when preparation or finalization work needs different scheduling
- scheduler-agnostic correctness, so runtime semantics do not depend on one particular executor implementation

## `tiforth` Role

- provide operators, expressions, and related semantics on top of the adopted Broken Pipeline runtime contract
- keep `tiforth` semantics aligned with the adopted Arrow-bound runtime states instead of inventing a parallel runtime API
- add harness assertions and adapter guidance without redefining runtime ownership
- keep scheduler helpers and local orchestration out of the shared runtime surface
>>>>>>> b14e856 (docs: adopt broken-pipeline-rs runtime framing)

## Shared Contract Surface

<<<<<<< HEAD
- work moves through explicit pipeline fragments or stages
- batch handoff is observable and measurable
- backpressure is a first-class concern
- cancellation and error propagation must be part of the contract
- memory admission must be explicit before internal growth
- scheduling choices should be separable from semantic correctness
=======
This adopted contract should eventually describe, in `tiforth` terms but not as a new independent runtime API:
>>>>>>> b14e856 (docs: adopt broken-pipeline-rs runtime framing)

- Arrow-bound stage inputs and outputs
- pull, push, or mixed handoff semantics used by the adopted runtime
- lifecycle events and runtime control states
- cancellation, drain, and error propagation behavior
- metrics and tracing hooks required by harnesses and adapters
- resource ownership at runtime boundaries

`broken-pipeline-schedule` is not part of this shared contract. Its ready-made schedulers and helpers are for local testing and harness use only.

## Non-Goals

<<<<<<< HEAD
- defining a second runtime contract parallel to `broken-pipeline-rs`
- treating `broken-pipeline-schedule` as a production scheduler contract
- preserving source-level compatibility with the original C++ `broken-pipeline`
=======
- inventing a `tiforth`-only runtime contract above `broken-pipeline-rs`
- treating `broken-pipeline-schedule` as a production or shared-contract dependency
- centering the original C++ Broken Pipeline repository as the active design source
- locking the scheduler design in this skeleton
- embedding engine-specific control flow into shared runtime contracts
>>>>>>> b14e856 (docs: adopt broken-pipeline-rs runtime framing)
- pretending the data contract and runtime contract can be designed independently

## Milestone-1 Admission Contract

For milestone 1:

- before allocating operator-owned mutable state or Arrow-backed output, `tiforth` reports intended memory to the host
- host admission is a precondition for continued execution on that path
- if the host denies the request, `tiforth` fails the pending allocation or execution path and does not allocate speculatively
- after admission, `tiforth` may allocate internally; direct host allocator routing is not required for milestone 1
- spill remains operator-managed above Arrow internals

## Open Questions

<<<<<<< HEAD
- TODO: choose the primary handoff model for the first harness slice
- TODO: define the first harness-visible runtime state machine for blocked, yield, drain, finish, cancel, and error transitions
- TODO: define which runtime events must be observable in tests
- TODO: define error taxonomy and propagation guarantees
- TODO: define memory ownership transfer rules between stages, including reservation state for internally allocated buffers and any future imported external buffers
- TODO: define how the host reservation or admission boundary is presented to the runtime (per query, stage, or operator) and what request, deny, shrink, and release guarantees are required when the host lives in Go, C++, or Rust
- TODO: define operator-managed spill triggers and handoff rules; do not assume transparent spill inside Arrow allocation paths
- TODO: decide how exchange, spill, and retry behaviors fit into the contract once operator-managed spill is made explicit
- TODO: decide what part of the runtime is shared versus adapter-owned
- TODO: decide which adopted `broken-pipeline-rs` surfaces are directly exposed versus wrapped or re-exported by `tiforth`
- TODO: keep `broken-pipeline-schedule` test/harness-only and prevent scheduler-layer details from leaking into the shared contract

## Initial Boundary

For now, the runtime contract exists to constrain harness, adapter, and operator design. It should become precise before any real kernel scheduling code is introduced.
=======
- TODO: record the exact Arrow-bound upstream types and states that `tiforth` adopts as its shared contract reference
- TODO: define how `tiforth` operators and expressions attach to the adopted contract without renaming its runtime states
- TODO: define which runtime events must be observable in tests when exercising the adopted contract
- TODO: define error taxonomy and propagation guarantees expected by adapters while staying compatible with the adopted runtime shape
- TODO: define memory ownership and accounting hooks between adopted runtime boundaries and `tiforth` operators
- TODO: define how exchange, spill, and retry behaviors map onto the adopted runtime contract
- TODO: decide what adapter-specific orchestration stays outside the shared contract

## Initial Boundary

For now, this contract exists to constrain harness and adapter design around the adopted `broken-pipeline-rs` Arrow-bound runtime surface. It should become more precise by recording adopted upstream types and operator hooks before any real kernel scheduling code is introduced.
>>>>>>> b14e856 (docs: adopt broken-pipeline-rs runtime framing)
