# TiForth architecture

TiForth is an **Arrow-native** compute library: it consumes and produces `arrow::RecordBatch` and relies on
Arrow’s compute runtime for a large part of expression evaluation.

This document describes the main moving pieces and how they fit together. For how to *use* the APIs, see
`pipeline_apis.md`.

## Core concepts

### Data model: `arrow::RecordBatch`

- **Unit of exchange**: TiForth operators process `arrow::RecordBatch` objects.
- **Schema stability**: operators generally assume stable schemas across batches (including field
  metadata) and return `Status::Invalid` on mismatches.
- **End-of-stream (EOS)**: EOS is signaled by the upstream `SourceOp` returning
  `OpOutput::Finished()`.

### Engine: host integration point

Public API: `tiforth::Engine` (`include/tiforth/engine.h`).

An `Engine` is the “host context” for execution. It owns/points to:

- `arrow::MemoryPool*`: all TiForth allocations should flow through the host-provided pool.
- `tiforth::SpillManager`: an abstraction for externalizing intermediate data (default is deny-spill).
- `arrow::compute::FunctionRegistry`: an engine-local registry overlay where TiForth registers its
  semantics-sensitive kernels (collation-aware string comparisons, decimal add parity, packed-MyTime helpers).

### Logical pipeline + task groups: composing and running operators

Public APIs:

- `tiforth::{SourceOp,PipeOp,SinkOp}` (`include/tiforth/broken_pipeline_traits.h`)
- `tiforth::LogicalPipeline` (`include/tiforth/broken_pipeline_traits.h`)
- `bp::Compile(...) -> tiforth::PipelineExec` (`include/broken_pipeline/pipeline_exec.h`)
- `tiforth::{Task,TaskGroup,TaskContext,TaskStatus}` (`include/tiforth/broken_pipeline_traits.h`)

Model:

- A `LogicalPipeline` describes operator wiring (N channels → 1 sink).
- Broken Pipeline compiles a `LogicalPipeline` into a `PipelineExec`.
- Hosts schedule/execute the returned task groups using their own threading/executor.

Execution is **host-driven** and incremental:

- A `Task` is a callable invoked as `task(task_ctx, task_id) -> TaskStatus`.
- `TaskStatus` can be `Continue`, `Yield`, `Finished`, `Cancelled`, or `Blocked` (with an `Awaiter`).
- TiForth does not provide a scheduler/reactor; hosts own orchestration and provide `Resumer` /
  `Awaiter` implementations.

### Operator abstraction

Public API: `tiforth::{SourceOp,PipeOp,SinkOp}` (`include/tiforth/broken_pipeline_traits.h`).

Key points:

- Operators communicate using `OpOutput` (streaming + blocked/yield states).
- `PipeOp::Pipe` / `SinkOp::Sink` may be called with `std::nullopt` to resume after a `Blocked` or `PipeYield`.
- End-of-stream is not represented by a `nullptr` batch; use `Drain()`/`Finished()` to flush/finalize.

TiForth ships a small set of built-in transform operators (filter/projection/join/agg/sort); see
`operators_and_functions.md`.

### Breakers / multi-stage execution

Breaker-style operators (e.g. HashAgg) are expressed using explicit `TaskGroup` boundaries:

- the build phase runs as a normal compiled pipeline stage (optionally parallel by `dop`)
- the breaker `SinkOp::Frontend()` provides a single-task barrier group for merge/finalize
- results are typically exposed by a host-constructed downstream pipeline using a source operator that reads
  from breaker state (often created by `SinkOp::ImplicitSource()`)

### Expressions and functions

Public APIs:

- Expression IR: `tiforth::Expr` (`include/tiforth/expr.h`)
- “Bind once, execute many”: `tiforth::CompiledExpr` (`include/tiforth/compiled_expr.h`)

TiForth expression compilation uses Arrow compute `Expression::Bind` against an input schema and an engine-local
function registry. During compilation TiForth may rewrite certain calls to preserve TiDB/MySQL semantics, driven
by Arrow field metadata (see `type_mapping_tidb_to_arrow.md`).

### Logical types via Arrow field metadata

Public API: `tiforth::LogicalType` helpers (`include/tiforth/type_metadata.h`).

TiForth uses Arrow `Field::metadata()` to preserve semantics that cannot be represented by Arrow’s physical type
alone (notably: decimals, packed date/time, and collated strings).

The metadata contract (keys, values, and the TiDB->Arrow mapping rules) is specified in
`type_mapping_tidb_to_arrow.md`.

## Layering and independence

TiForth is designed to be embedded into arbitrary hosts:

- it depends on Arrow (core + compute)
- it does **not** depend on any host’s internal headers/types
- semantic hooks are expressed as:
  - Arrow field metadata
  - an engine-owned `FunctionRegistry` overlay
  - host-provided `MemoryPool` / `SpillManager`
