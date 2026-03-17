# Expression Projection Slice Cases

Spec source: `docs/spec/milestone-1-expression-projection.md`

## Canonical Cases

- `column passthrough`: direct column projection preserves values, row count, and output order without computed-column admission events
- `add literal`: `add(column(0), literal(1))` produces an `Int32Array` with row-wise addition
- `null propagation`: `add` yields null whenever either operand is null
- `reserve-first deny`: computed projection fails before allocation when admission rejects the estimated bytes
- `runtime path`: the source -> projection -> sink path runs end-to-end through `broken-pipeline`, with `broken-pipeline-schedule` used only in local tests

## Initial Test Shape

The initial executable coverage may stay local to the Rust crate while this directory continues to hold the spec-linked case plan.
