# Milestone-1 Expression And Projection Slice

Status: issue #10 scaffold

Related issues:

- #10 `milestone-1: first Arrow-bound operator and expression slice`
- #8 `design: host memory admission ABI for tiforth`
- #9 `design: tiforth dependency boundary over broken-pipeline-rs`

## Scope

This spec defines the first executable `tiforth` slice:

- one static Arrow batch source
- one projection pipe operator
- one collecting sink for local tests
- expression evaluation for `column`, `literal<int32>`, and `add<int32>`

Filter is deferred unless a later issue extends this slice explicitly.

## Batch Contract

- input and output batches use Arrow `RecordBatch`
- projection preserves row count and output expression order
- direct column projection reuses the referenced input column without copying

## Expression Semantics

### `column(index)`

- references the input column at `index`
- when projected directly, it reuses the existing Arrow array
- field metadata comes from the referenced input field, except that the projection output name replaces the input field name

### `literal<int32>(value)`

- materializes an `Int32Array` with one entry per input row
- `NULL` literal yields a nullable all-null array
- non-null literal yields a non-null `Int32Array`

### `add<int32>(lhs, rhs)`

- evaluates row-wise over the input batch
- both operands must be `int32` expressions in this slice
- nulls propagate: if either side is null for a row, the result is null for that row
- overflow is an execution error in this slice

## Runtime Boundary

- the executable path uses the adopted `broken-pipeline` core runtime contract directly
- `broken-pipeline-schedule` is allowed only in local tests and harness execution
- this slice does not define a `tiforth`-owned replacement runtime API

## Admission Boundary

The minimal slice exercises reserve-first admission around computed output-column construction:

- open a projection-output consumer before building a computed `Int32Array`
- `try_reserve` the estimated bytes before builder growth
- `shrink` any conservative over-reservation after materialization
- `release` the build-time reservation when the computed column is materialized

This keeps the issue #10 slice honest about reserve-before-allocate behavior without yet claiming to solve full cross-stage ownership accounting for live output buffers. That remaining ownership transfer problem stays as follow-up work.

## Deferred Work

- filter semantics and operators
- non-`int32` arithmetic expressions
- spill-aware operators
- batch-lifetime admission tracking across stage handoff and sink ownership
