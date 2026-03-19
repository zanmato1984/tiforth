# First Filter Semantic Slice: `is_not_null(column(index))`

Status: issue #139 spec checkpoint

Related issues:

- #137 `design: choose first post-gate kernel expansion boundary`
- #139 `spec: define first filter semantic slice for is_not_null(column(index))`

## Scope

This spec defines the first post-gate filter semantic slice selected by
`docs/design/first-post-gate-kernel-boundary.md`.

In scope for this slice:

- one row-filter operator boundary over Arrow batches
- one predicate family: `is_not_null(column(index))`
- deterministic keep/drop semantics with stable retained-row order

Out of scope for this slice:

- general boolean expression trees (`and`, `or`, `not`)
- comparison predicates (`eq`, `lt`, `gt`, ...)
- filter predicates over non-`int32` logical families
- join, aggregate, sort, exchange, or other operator families

## Input And Output Shape

- input uses one Arrow `RecordBatch` plus the semantic handoff envelope from
  `docs/contracts/data.md`
- output uses one Arrow `RecordBatch` with the same schema fields and field
  order as input
- output row count is `0..=input_row_count`
- retained rows preserve original input row order
- retained rows preserve source values for every output column

This first filter slice does not rename, reorder, or compute new columns.

## Predicate Semantics

### `is_not_null(column(index))`

- `column(index)` references one input column by zero-based index
- for each input row:
  - predicate is `true` when the referenced column value is not null
  - predicate is `false` when the referenced column value is null
- rows with predicate `true` are kept
- rows with predicate `false` are dropped

For this first slice, `is_not_null` is a two-valued predicate at row-selection
time (`true` or `false` only). A null column value maps to `false`.

## Error Semantics

- out-of-range `column(index)` is an execution error
- unsupported predicate input type is an execution error
- reserve-first admission denial for filter-owned materialization is a
  `memory_admission_denied` runtime error path under `docs/contracts/runtime.md`

For this first slice, supported predicate input type is `int32` only.

## Row-Retention Truth Table

For one predicate `is_not_null(column(k))`:

| `column(k)` value | predicate | output row |
| --- | --- | --- |
| non-null `int32` | `true` | kept |
| null `int32` | `false` | dropped |

## Contract And Harness Links

- type behavior: `docs/spec/type-system.md`
- shared data/runtime boundaries: `docs/contracts/data.md`,
  `docs/contracts/runtime.md`
- conformance checkpoint cases: `tests/conformance/first-filter-is-not-null-slice.md`
- differential checkpoint shape:
  `tests/differential/first-filter-is-not-null-slice.md`
- adapter request/response boundary:
  `adapters/first-filter-is-not-null-slice.md`

## Deferred Work

- predicate families beyond `is_not_null(column(index))`
- non-`int32` predicate coverage
- adapter and differential execution expansion for this filter slice
