# Runtime Contract

Direction: `tiforth` adopts and exposes the Arrow-bound runtime contract from `broken-pipeline-rs` as its primary shared runtime contract.

`tiforth` is not defining an independent runtime contract here. The shared runtime surface is the Arrow-bound Broken Pipeline contract already present in the upstream Rust port. `tiforth`'s role is to provide operators, expressions, and higher-level composition on top of that adopted contract.

`broken-pipeline-schedule` sits outside the shared contract. Within `tiforth`, it is reserved for local testing and harness work only, and this is not expected to change.

The original C++ Broken Pipeline repository remains relevant only as provenance and lineage for the adopted model. It is not the active design center for `tiforth`.

## Adopted Runtime Shape

- small-step, resumable task execution that a scheduler can re-enter
- explicit runtime control states for continue, blocked, yield, finished, and cancelled work, or close semantic equivalents already present in the adopted contract
- explicit stage boundaries with observable handoff points
- Arrow-bound batch handoff as the primary data-bearing runtime surface
- end-of-stream drain or flush behavior as part of the runtime contract
- host-orchestration hooks around the main streaming path when preparation or finalization work needs different scheduling
- scheduler-agnostic correctness, so runtime semantics do not depend on one particular executor implementation

## `tiforth` Role

- provide operators, expressions, and related semantics on top of the adopted Broken Pipeline runtime contract
- keep `tiforth` semantics aligned with the adopted Arrow-bound runtime states instead of inventing a parallel runtime API
- add harness assertions and adapter guidance without redefining runtime ownership
- keep scheduler helpers and local orchestration out of the shared runtime surface

## Shared Contract Surface

This adopted contract should eventually describe, in `tiforth` terms but not as a new independent runtime API:

- Arrow-bound stage inputs and outputs
- pull, push, or mixed handoff semantics used by the adopted runtime
- lifecycle events and runtime control states
- cancellation, drain, and error propagation behavior
- metrics and tracing hooks required by harnesses and adapters
- resource ownership at runtime boundaries
- memory admission and accounting hooks required before governed internal growth

`broken-pipeline-schedule` is not part of this shared contract. Its ready-made schedulers and helpers are for local testing and harness use only.

## Host Memory Admission Boundary

For milestone 1, runtime-visible memory governance follows the reserve-first design from issue #2 and issue #8.

- `tiforth` opens a host-visible memory consumer for each operator or runtime-owned memory domain that needs distinct attribution or spillability
- before governed allocation or reallocation, `tiforth` calls the host admission ABI to reserve the additional bytes required for the intended peak live growth
- if the host denies the request, `tiforth` does not allocate on that path; it either spills and retries within operator policy or returns an execution error
- after admission succeeds, `tiforth` may allocate internally through its own Rust and Arrow allocation paths
- when live bytes drop but the consumer remains active, `tiforth` reports the decrease via `shrink`
- when the consumer finishes, is cancelled, or errors out, `tiforth` `release`s all remaining admitted bytes
- the admission ABI is synchronous, callback-free, and does not add a new shared runtime state beyond success, spill-and-retry within the operator, or error propagation

Detailed admission semantics live in `docs/design/host-memory-admission-abi.md`.

## Non-Goals

- inventing a `tiforth`-only runtime contract above `broken-pipeline-rs`
- treating `broken-pipeline-schedule` as a production or shared-contract dependency
- centering the original C++ Broken Pipeline repository as the active design source
- locking the scheduler design in this skeleton
- embedding engine-specific control flow into shared runtime contracts
- pretending the data contract and runtime contract can be designed independently
- requiring transparent spill inside Arrow allocation internals
- requiring direct host-allocator routing for every Arrow growth path in milestone 1

## Open Questions

- TODO: record the exact Arrow-bound upstream types and states that `tiforth` adopts as its shared contract reference
- TODO: define how `tiforth` operators and expressions attach to the adopted contract without renaming its runtime states
- TODO: define which runtime events must be observable in tests when exercising the adopted contract
- TODO: define error taxonomy and propagation guarantees expected by adapters, including memory-admission denial and internal allocation failure
- TODO: define ownership transfer rules between runtime stages for already-admitted buffers and claimed batches
- TODO: define how exchange, spill, and retry behaviors map onto the adopted runtime contract
- TODO: decide what adapter-specific orchestration stays outside the shared contract

## Initial Boundary

For now, this contract exists to constrain harness and adapter design around the adopted `broken-pipeline-rs` Arrow-bound runtime surface and the reserve-first admission boundary. It should become more precise by recording adopted upstream types, operator hooks, and observable memory-governance events before any real kernel scheduling code is introduced.
