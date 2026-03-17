# Milestone-1 Arrow Batch Handoff Cases

Spec sources:

- `docs/contracts/data.md`
- `docs/contracts/runtime.md`
- `docs/design/host-memory-admission-abi.md`

## Canonical Cases

- `envelope shape`: a runtime-observable handoff consists of the adopted Arrow `Batch` payload plus `batch_id`, origin metadata, and zero or more live ownership claims
- `passthrough forwarding`: a stage that reuses an incoming Arrow array without copying forwards the incoming claim unchanged and does not reserve again for that reuse alone
- `computed claim sealing`: a stage that materializes a governed output buffer reserves first, `shrink`s to the exact retained bytes before emit, then seals the remaining bytes into a live batch claim
- `mixed claim set`: one output batch may carry both forwarded incoming claims and newly created stage-owned claims
- `final release`: the last downstream holder of a live batch claim triggers `batch_released` and the resulting `consumer_released`; the producing stage must not release that consumer earlier just because local materialization finished
- `deny before emit`: if admission denies a governed growth step, `tiforth` emits a deny outcome and no batch or new claim for that denied step
- `teardown release`: cancellation or error teardown releases any claims still attached to in-flight batches before the query reports final completion
- `ownership violation`: shrinking or releasing bytes that remain reachable from a live batch surfaces as `ownership_contract_violation`

## Initial Test Shape

Milestone 1 now has a stable local Rust-side snapshot carrier, `tiforth_kernel::LocalExecutionSnapshot`, exercised in `crates/tiforth-kernel/tests/expression_projection.rs` for computed handoff, forwarded-claim passthrough, deny-before-emit, and final-drop release in the current projection slice.

This file remains the higher-level case plan until adapter-visible fixtures or broader harness carriers are defined beyond the current local Rust snapshot.
