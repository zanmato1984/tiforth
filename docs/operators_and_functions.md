# Operator and function fundamentals

This doc describes TiForth’s operator model, built-in operators, and how expressions/functions interact with
Arrow compute.

## 1) Operator model

Public interfaces: `include/tiforth/pipeline/op/op.h` (also via `include/tiforth/tiforth.h`).

TiForth defines three operator kinds:

- `pipeline::SourceOp`: produces batches (`Source`)
- `pipeline::PipeOp`: transforms batches (`Pipe`) and flushes at end-of-stream (`Drain`)
- `pipeline::SinkOp`: consumes batches (`Sink`) and may schedule backend work (`Backend`)

### Output protocol

Operators communicate with `pipeline::OpOutput`:

- `PipeSinkNeedsMore`: sink/pipe is ready for more input
- `PipeEven`: pipe produced one output batch for this input
- `SourcePipeHasMore`: source/drain produced an output batch and may have more
- `Finished`: operator/channel finished (no end-of-stream `nullptr` batch sentinel)
- `Blocked(resumer)`: operator cannot progress until the host resumes the `Resumer`
- `PipeYield` / `PipeYieldBack`: cooperative yield within a channel
- `Cancelled`: cancelled

### End-of-stream (EOS)

EOS is represented as `pipeline::OpOutput::Finished()` from the upstream source. There is no `nullptr` batch EOS marker.

Rules:

- `SourceOp::Source` returns `Finished()` when input is exhausted.
- `PipeOp::Drain` is called after the source finishes to flush buffered output; it should eventually return `Finished()`.
- `PipeOp::Pipe` / `SinkOp::Sink` may be called with `std::nullopt` to resume after a `Blocked` or `PipeYield`.
  Operators should typically return `PipeSinkNeedsMore()` in this case.

## 2) Built-in transform operators

Public headers live under `include/tiforth/operators/`.

### Pass-through

- `tiforth::PassThroughPipeOp` (`pass_through.h`)
- Behavior: forwards input batch as-is.

### Filter

- `tiforth::FilterPipeOp` (`filter.h`)
- Input: any schema
- Output: same schema (rows filtered)
- Predicate: `tiforth::Expr` evaluated via Arrow compute; must produce a boolean array of length `num_rows`.
- Null handling: uses Arrow `FilterOptions::DROP` (rows with null predicate are dropped).
- Compilation: predicate is compiled once on the first batch schema and reused.

### Projection

- `tiforth::ProjectionPipeOp` (`projection.h`)
- Input: any schema
- Output: new schema with one field per `ProjectionExpr {name, expr}`
- Compilation: expressions compiled once on first batch schema and reused.
- Metadata preservation:
  - direct field refs preserve the input field (including metadata) and only rename it
  - computed outputs are new fields; TiForth currently auto-attaches logical metadata only for:
    - Arrow decimal outputs (marks them `tiforth.logical_type=decimal`)
    - `toMyDate(...)` outputs (marks them `tiforth.logical_type=mydate`)

### Hash aggregation (group-by)

- `tiforth::HashAggState` / `tiforth::HashAggSinkOp` / `tiforth::HashAggResultSourceOp` (`hash_agg.h`)
- Driven by Arrow `Grouper` + grouped `hash_*` kernels (no Acero).
- Breaker shape:
  - `HashAggSinkOp`: per-`ThreadId` partial aggregation (grouper + per-agg kernel states)
  - `HashAggSinkOp::Backend`: single-task merge + finalize barrier
  - `HashAggResultSourceOp` (or `HashAggSinkOp::ImplicitSource`): streams the finalized output
- Current limitations:
  - group-by without keys is not implemented
  - string keys:
    - default is binary semantics
    - single-key grouping can be collation-aware when the key field has `tiforth.string.collation_id` metadata
    - multi-key collation grouping is not implemented

### Hash join (inner join)

- `tiforth::HashJoinPipeOp` (`hash_join.h`)
- Build side:
  - provided as a vector of `RecordBatch` at operator construction time
  - concatenated and indexed on first probe batch
- Join keys:
  - 1 or 2 keys
  - supported key physical types:
    - `int32`
    - `uint64`
    - `decimal128` / `decimal256` (compared by raw fixed-size bytes)
    - `binary` (with `tiforth.logical_type=string` and supported `collation_id` for normalization)
  - for string keys, build/probe must agree on `collation_id`
- Output schema:
  - concatenation of probe schema fields followed by build schema fields
- Join type:
  - common path implements an inner join (unmatched probe rows produce no output)

### Sort

- `tiforth::SortPipeOp` (`sort.h`)
- Current limitations (common path):
  - exactly 1 sort key
  - `ascending=true`, `nulls_first=false` only (ASC with NULLS LAST)
- For string keys:
  - if key field has `tiforth.logical_type=string`, TiForth sorts using collation-aware rules
  - current implementation requires the key array type to be `binary`
- Output:
  - buffers all input, emits one sorted output batch during `Drain()`

## 3) Expressions

Public API: `include/tiforth/expr.h`.

TiForth has a minimal expression IR:

- `FieldRef {name|index}`: reference an input column
- `Literal {Scalar}`: scalar constant
- `Call {function_name, args...}`: call into Arrow compute

You can:

- evaluate directly per batch: `EvalExpr`, `EvalExprAsArray`
- compile/bind once per schema and execute repeatedly:
  - `CompileExpr` -> `tiforth::CompiledExpr`
  - `ExecuteExpr`, `ExecuteExprAsArray`

## 4) Function fundamentals (Arrow compute integration)

### Engine-local function registry

`tiforth::Engine` creates (or accepts) a `arrow::compute::FunctionRegistry` and registers TiForth functions into
it. Operators that evaluate expressions create an `arrow::compute::ExecContext` using:

- the engine’s memory pool
- the engine’s function registry

### Logical-type-driven rewrites (during compilation)

During expression compilation, TiForth inspects argument logical types (from Arrow field metadata and some
physical types) and may rewrite calls:

- Collated string comparisons:
  - calls like `equal/less/...` become `tiforth.collated_equal/tiforth.collated_less/...`
  - a `collation_id` option is derived from `tiforth.string.collation_id`
- Decimal arithmetic:
  - `add/subtract/multiply/divide` become the corresponding `tiforth.decimal_*` functions when at least one argument is
    decimal
  - `tidbDivide/modulo` become `tiforth.decimal_tidb_divide` / `tiforth.decimal_modulo` when at least one argument is
    decimal
- Packed MyTime extraction:
  - when an argument is `tiforth.logical_type=mydate|mydatetime`, TiForth attaches MyTime options
  - for `hour/minute/second` it routes to `tiforth.mytime_hour/_minute/_second` to avoid colliding with Arrow’s
    timestamp extractors
- TiFlash/ClickHouse comparison aliases:
  - `equals/notEquals/lessOrEquals/greaterOrEquals` are normalized to Arrow compute
    `equal/not_equal/less_equal/greater_equal` during compilation (before rewrite decisions)

### TiForth-defined function names (common path)

Registered by `src/tiforth/functions/register.cc`:

- Collated compare:
  - `tiforth.collated_equal`
  - `tiforth.collated_not_equal`
  - `tiforth.collated_less`
  - `tiforth.collated_less_equal`
  - `tiforth.collated_greater`
  - `tiforth.collated_greater_equal`
- Decimal arithmetic:
  - `tiforth.decimal_add`
  - `tiforth.decimal_subtract`
  - `tiforth.decimal_multiply`
  - `tiforth.decimal_divide`
  - `tiforth.decimal_tidb_divide`
  - `tiforth.decimal_modulo`
- Numeric / bitwise arithmetic (TiFlash/TiDB semantics):
  - bitwise: `bitAnd`, `bitOr`, `bitXor`, `bitNot`, `bitShiftLeft`, `bitShiftRight` (returns `uint64`)
  - unary: `abs` (signed int -> unsigned; `abs(INT64_MIN)` errors), `negate` (unsigned -> signed nextSize)
  - division: `intDiv` (division-by-zero errors), `intDivOrZero` (division-by-zero returns 0)
  - modulo: `modulo` (division-by-zero yields NULL; decimal cases are rewritten to `tiforth.decimal_modulo`)
  - other: `gcd`, `lcm` (matches TiFlash behavior; currently errors if either argument is zero)
- Packed MyTime (selected):
  - `toYear`, `toMonth`, `toDayOfMonth`, `toMyDate`
  - `toDayOfWeek`, `toWeek`, `toYearWeek`
  - `tidbDayOfWeek`, `tidbWeekOfYear`, `yearWeek`
  - `tiforth.mytime_hour`, `tiforth.mytime_minute`, `tiforth.mytime_second`
  - `microSecond`
- Logical (TiFlash/TiDB semantics; Arrow boolean output):
  - `and`, `or`, `xor` (varargs, min 2; numeric truthiness; 3VL null semantics)
  - `not`

For the mapping/metadata that drives these rewrites, see `type_mapping_tidb_to_arrow.md`.
