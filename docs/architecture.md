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
- **End-of-stream (EOS)**: EOS is signaled by the upstream `pipeline::SourceOp` returning
  `pipeline::OpOutput::Finished()`.

### Engine: host integration point

Public API: `tiforth::Engine` (`include/tiforth/engine.h`).

An `Engine` is the “host context” for execution. It owns/points to:

- `arrow::MemoryPool*`: all TiForth allocations should flow through the host-provided pool.
- `tiforth::SpillManager`: an abstraction for externalizing intermediate data (default is deny-spill).
- `arrow::compute::FunctionRegistry`: an engine-local registry overlay where TiForth registers its
  semantics-sensitive kernels (collation-aware string comparisons, decimal add parity, packed-MyTime helpers).

### Logical pipeline + task groups: composing and running operators

Public APIs:

- `tiforth::pipeline::{SourceOp,PipeOp,SinkOp}` (`include/tiforth/pipeline/op/op.h`)
- `tiforth::pipeline::LogicalPipeline` (`include/tiforth/pipeline/logical_pipeline.h`)
- `tiforth::pipeline::CompileToTaskGroups(...)` (`include/tiforth/pipeline/task_groups.h`)
- `tiforth::task::{Task,TaskGroup,TaskContext,TaskStatus}` (`include/tiforth/task/*`)

Model:

- A `LogicalPipeline` describes operator wiring (N channels → 1 sink).
- TiForth compiles a `LogicalPipeline` into an ordered list of `task::TaskGroup`.
- Hosts schedule/execute the returned task groups using their own threading/executor.

Execution is **host-driven** and incremental:

- A `task::Task` is a callable invoked as `task(task_ctx, task_id) -> task::TaskStatus`.
- `TaskStatus` can be `Continue`, `Yield`, `Finished`, `Cancelled`, or `Blocked` (with an `Awaiter`).
- TiForth does not provide a scheduler/reactor; it only provides `TaskGroup` boundaries and
  `BlockedResumer` helpers for common blocking patterns (IO/await/notify).

### Operator abstraction

Public API: `pipeline::{SourceOp,PipeOp,SinkOp}` (`include/tiforth/pipeline/op/op.h`).

Key points:

- Operators communicate using `pipeline::OpOutput` (streaming + blocked/yield states).
- `PipeOp::Pipe` / `SinkOp::Sink` may be called with `std::nullopt` to resume after a `Blocked` or `PipeYield`.
- End-of-stream is not represented by a `nullptr` batch; use `Drain()`/`Finished()` to flush/finalize.

TiForth ships a small set of built-in transform operators (filter/projection/join/agg/sort); see
`operators_and_functions.md`.

### Breakers / multi-stage execution

Breaker-style operators (e.g. HashAgg) are expressed using explicit `TaskGroup` boundaries:

- the build pipeline runs as a normal `PipelineTask` group (optionally parallel by `dop`)
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
