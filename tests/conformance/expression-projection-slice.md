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

The initial executable coverage may stay local to the Rust crate while this directory continues to hold the spec-linked case plan.

The claim-carrying batch handoff semantics for this slice are further detailed in `tests/conformance/milestone-1-arrow-batch-handoff.md`.
