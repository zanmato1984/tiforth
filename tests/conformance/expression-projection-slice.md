# Expression Projection Slice Cases

Spec source: `docs/spec/milestone-1-expression-projection.md`

## Canonical Cases

- `column passthrough`: direct column projection preserves values, row count, and output order while forwarding incoming claims without opening a new computed-column consumer
- `add literal`: `add(column(0), literal(1))` produces an `Int32Array` with row-wise addition
- `null propagation`: `add` yields null whenever either operand is null
- `reserve-first deny`: computed projection fails before allocation when admission rejects the estimated bytes
- `computed handoff`: computed projection `shrink`s to the exact retained bytes before emit, then keeps that claim live through source -> projection -> sink handoff until sink drop or teardown
- `runtime path`: the source -> projection -> sink path runs end-to-end through `broken-pipeline`, with `broken-pipeline-schedule` used only in local tests and with observable admit, emit, handoff, release, and terminal events

## Initial Test Shape

Milestone 1 now has local executable coverage in `crates/tiforth-kernel/tests/expression_projection.rs` for:

- computed projection handoff and final-drop release
- mixed forwarded-plus-computed claim handoff in one output batch
- reserve-first denial before emit
- direct-column claim forwarding without opening a new computed-column consumer

Those tests now capture milestone-1 runtime and admission outcomes through `tiforth_kernel::LocalExecutionSnapshot`, while still checking Arrow output values and sink-visible claim counts directly.

For the current local Rust slice, executable assertions should prefer the exported `tiforth_kernel::LocalExecutionFixture` carrier so projection-path fixture checks stay aligned with the contract-named event surface from `docs/contracts/runtime.md`.

The checked-in JSON artifacts for those fixture assertions now live under `tests/conformance/fixtures/local-execution/`, with format notes in `tests/conformance/local-execution-fixtures.md`.

This directory remains the spec-linked case plan while broader harness snapshot formats and adapter-facing fixtures are still being defined.

The claim-carrying batch handoff semantics for this slice are further detailed in `tests/conformance/milestone-1-arrow-batch-handoff.md`.
