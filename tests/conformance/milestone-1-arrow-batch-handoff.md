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
- `tracked handoff`: a receiving stage rejects a batch as `ownership_contract_violation` when no matching tracked emit record exists for that handoff

## Initial Test Shape

Milestone 1 now has a stable local Rust-side snapshot carrier, `tiforth_kernel::LocalExecutionSnapshot`, plus an exported local fixture carrier, `tiforth_kernel::LocalExecutionFixture`, exercised in `crates/tiforth-kernel/tests/expression_projection.rs` and backed by checked-in JSON fixture artifacts under `tests/conformance/fixtures/local-execution/` for computed handoff, mixed forwarded-plus-computed claims, forwarded-claim passthrough, forwarded-claim release and shrink ownership violations, untracked-handoff ownership violation, deny-before-emit, and final-drop release in the current projection slice.

The teardown-release case now includes executable mixed-claim cancelled coverage. That checkpoint uses a local explicit cancellation driver which steps the compiled projection runtime until sink handoff is observable and then tears down before the later `finished` step. The driver remains local harness scaffolding rather than shared-contract surface.

Ownership-violation coverage now also includes two forwarded-claim passthrough checkpoints that wait until sink handoff is observable, then attempt either an explicit local early release or an explicit local early shrink through the directly addressed local consumer behind the still-live forwarded claim and expect the same `ownership_contract_violation` output before the later teardown drop releases the batch cleanly. That broader enforcement boundary remains local-only and does not change the shared runtime surface.

The same local coverage now also checks that a downstream receiver rejects an untracked batch handoff before sink collection. That checkpoint uses a local source which bypasses runtime tracking and expects the projection-stage adoption path to surface `ownership_contract_violation` without recording a successful handoff event.

This file remains the higher-level case reference until adapter-visible fixtures or broader harness carriers are defined beyond the current local Rust snapshot.
