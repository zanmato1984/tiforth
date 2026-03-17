# Expression Projection Slice Cases

Spec source: `docs/spec/milestone-1-expression-projection.md`

## Canonical Cases

- `column passthrough`: direct column projection preserves values, row count, and output order while forwarding incoming claims without opening a new computed-column consumer
- `direct literal`: `literal<int32>(value)` materializes one `Int32Array` value per input row, with non-null literals staying non-null and `NULL` literals yielding nullable all-null output
- `missing column`: `column(index)` fails as an execution error before projection emit and sink collection when `index` is out of range for the input schema
- `add literal`: `add(column(0), literal(1))` produces an `Int32Array` with row-wise addition
- `unsupported arithmetic type`: `add<int32>` fails as an execution error before projection emit and sink collection when any input expression resolves to a non-`int32` column
- `null propagation`: `add` yields null whenever either operand is null
- `overflow error`: `add` fails as an execution error before emit when `int32` addition overflows
- `reserve-first deny`: computed projection fails before allocation when admission rejects the estimated bytes
- `computed handoff`: computed projection `shrink`s to the exact retained bytes before emit, then keeps that claim live through source -> projection -> sink handoff until sink drop or teardown
- `runtime path`: the source -> projection -> sink path runs end-to-end through `broken-pipeline`, with `broken-pipeline-schedule` used only in local tests and with observable admit, emit, handoff, release, and terminal events

## Initial Test Shape

Milestone 1 now has local executable coverage in `crates/tiforth-kernel/tests/expression_projection.rs` for:

- computed projection handoff and final-drop release
- direct `literal<int32>` projection for both non-null and `NULL` outputs
- mixed forwarded-plus-computed claim handoff in one output batch
- reserve-first denial before emit
- missing-column execution error before projection output collection
- unsupported-arithmetic-type execution error before projection output collection
- nullable `add<int32>` output with null propagation through runtime handoff
- `add<int32>` overflow execution error before sink collection
- direct-column claim forwarding without opening a new computed-column consumer
- mixed-claim cancelled teardown after sink handoff via a local explicit cancellation driver
- forwarded-claim ownership violations after sink handoff via local explicit early-release and early-shrink checkpoints against the directly addressed local consumer behind that live claim
- claimed-source ownership violation when `ProjectionRuntimeContext` is missing before any source emit or sink collection
- untracked source-to-projection handoff ownership violation before batch adoption and sink collection

The same local slice also preserves explicit local fixture checkpoints for a direct `NULL` literal computed-output path. That checkpoint confirms the milestone-1 no-shrink path: the projection-output consumer reserves the full nullable `Int32Array` estimate, keeps all admitted bytes attached to the emitted claim, and releases that unchanged retained size only after the sink-owned batch drops.

The same local slice also preserves explicit local fixture checkpoints for a direct non-null literal computed-output path. That checkpoint confirms the milestone-1 shrink path for computed literals: the projection-output consumer reserves the estimated `Int32Array` bytes, shrinks to the exact retained size before emit, and releases only the live retained bytes after the sink-owned batch drops.

The same local slice now also preserves explicit local fixture checkpoints for a nullable computed `add<int32>` path. That checkpoint confirms the milestone-1 no-shrink path for computed arrays with propagated nulls: the projection-output consumer reserves the full nullable `Int32Array` estimate, keeps those admitted bytes attached to the emitted claim through sink handoff, and releases that unchanged retained size only after the sink-owned batch drops.

Those tests now capture milestone-1 runtime and admission outcomes through `tiforth_kernel::LocalExecutionSnapshot`, while still checking Arrow output values and sink-visible claim counts directly.

The local Rust slice now covers a true `cancelled` terminal checkpoint for mixed-claim teardown. That coverage uses a local explicit cancellation driver which steps the compiled projection runtime until sink handoff is observable and then tears down before the later `finished` step.

The same local slice now also covers two `ownership_contract_violation` checkpoints for forwarded-claim passthrough. Those checkpoints wait until sink handoff is observable, then attempt either an explicit local early release or an explicit local early shrink through the directly addressed local consumer behind the still-live forwarded claim before dropping the sink-owned batch and recording the terminal error. The preserved fixture output stays local to this Rust-side enforcement path rather than redefining any shared runtime surface.

The same local slice also covers an untracked-handoff `ownership_contract_violation` checkpoint. That checkpoint uses a local source that bypasses runtime tracking, expects the projection receiver to reject the batch before sink collection, and preserves the resulting terminal error through the same local fixture carrier.

The same local slice also covers a missing-runtime-context `ownership_contract_violation` checkpoint for claimed source batches. That checkpoint exercises the existing guard in `StaticRecordBatchSource::new_claimed`, expects the claimed local source to reject the batch before any source emit, and preserves the resulting terminal error through the same local fixture carrier.

The same local slice now also covers an unsupported-arithmetic-type execution-error checkpoint. That checkpoint drives `add<int32>` against a boolean source column, expects the projection receiver to reject the batch before any projection output emit or sink collection, and preserves the resulting terminal error through the same local fixture carrier.

For the current local Rust slice, executable assertions should prefer the exported `tiforth_kernel::LocalExecutionFixture` carrier so projection-path fixture checks stay aligned with the contract-named event surface from `docs/contracts/runtime.md`.

The checked-in JSON artifacts for those fixture assertions now live under `tests/conformance/fixtures/local-execution/`, with format notes in `tests/conformance/local-execution-fixtures.md`.

This directory remains the spec-linked case reference while broader harness snapshot formats and adapter-facing fixtures are still being defined.

The claim-carrying batch handoff semantics for this slice are further detailed in `tests/conformance/milestone-1-arrow-batch-handoff.md`.
