# Pipeline APIs (C++ and C ABI)

TiForth exposes a small set of stable “host APIs” for building and executing pipelines:

- C++ API:
  - `tiforth::Engine`
  - `tiforth::pipeline::{SourceOp,PipeOp,SinkOp,LogicalPipeline}`
  - `tiforth::pipeline::CompileToTaskGroups(...) -> tiforth::task::TaskGroups`
  - `tiforth::task::{Task,TaskGroup,TaskContext,TaskStatus,Awaiter,Resumer}`
- C ABI (experimental): temporarily disabled after MS26; redesign is a follow-up milestone.

This doc focuses on the execution model and API contracts. For operator details, see
`operators_and_functions.md`.

## C++ API

Include:

- umbrella header: `#include <tiforth/tiforth.h>`
- or individual headers under `include/tiforth/`

### Engine

`tiforth::Engine` is created from `tiforth::EngineOptions`:

- `memory_pool`: required (defaults to `arrow::default_memory_pool()` if you pass none)
- `spill_manager`: optional (defaults to `tiforth::DenySpillManager`)
- `function_registry`: optional (defaults to a fresh registry overlay cloned from Arrow’s global registry)

On creation, the engine registers TiForth functions into its registry.

### Building a logical pipeline

A `tiforth::pipeline::LogicalPipeline` is composed of:

- N **channels** (`LogicalPipeline::Channel`), each with:
  - a `pipeline::SourceOp*`
  - zero or more `pipeline::PipeOp*` in order
- One `pipeline::SinkOp*` shared by all channels.

The host owns all operator objects. The pipeline stores raw pointers, so the host must keep the
operators alive while running the pipeline.

Example (filter + projection; 1 channel):

```cpp
auto engine = tiforth::Engine::Create({}).ValueOrDie();

auto source_op = /* host-defined SourceOp */;

std::vector<std::unique_ptr<tiforth::pipeline::PipeOp>> pipe_ops;
pipe_ops.push_back(std::make_unique<tiforth::FilterPipeOp>(
    engine.get(),
    tiforth::MakeCall("less", {tiforth::MakeFieldRef("a"),
                               tiforth::MakeLiteral(std::make_shared<arrow::Int32Scalar>(10))})));
pipe_ops.push_back(std::make_unique<tiforth::ProjectionPipeOp>(
    engine.get(), std::vector<tiforth::ProjectionExpr>{{"b", tiforth::MakeFieldRef("b")}}));

auto sink_op = /* host-defined SinkOp */;

tiforth::pipeline::LogicalPipeline::Channel channel;
channel.source_op = source_op.get();
for (auto& op : pipe_ops) channel.pipe_ops.push_back(op.get());

tiforth::pipeline::LogicalPipeline logical_pipeline{
    "FilterProjection", {std::move(channel)}, sink_op.get()};
```

### Compilation: `LogicalPipeline -> TaskGroups`

`tiforth::pipeline::CompileToTaskGroups(...)` compiles a `LogicalPipeline` into an *ordered list of*
`tiforth::task::TaskGroup`.

The host is responsible for:

- choosing `dop` (degree of parallelism) and constructing operators accordingly
- executing task groups in order; running tasks within a group in parallel if desired
- providing a `tiforth::task::TaskContext` with resumer/awaiter factories.

Minimal sketch:

```cpp
auto groups = tiforth::pipeline::CompileToTaskGroups(
    tiforth::pipeline::PipelineContext{}, logical_pipeline, /*dop=*/4).ValueOrDie();

// task_ctx.resumer_factory = ...
// task_ctx.any_awaiter_factory = ...
for (const auto& group : groups) {
  // schedule group.GetTask()(task_ctx, task_id) for task_id in [0..group.NumTasks)
  // handle TaskStatus::Continue/Yield/Blocked/Finished
  // run optional group continuation; then group.NotifyFinish(task_ctx)
}
```

## C ABI (tiforth_capi)

C header: `include/tiforth_c/tiforth.h`.

The C ABI is experimental and currently disabled while the public C++ API stabilizes around
`LogicalPipeline + TaskGroups` (MS26). A redesigned C ABI is a follow-up milestone.

### Expressions (C)

The C ABI provides a minimal expression builder:

- `tiforth_expr_field_ref_index`
- `tiforth_expr_literal_int32`
- `tiforth_expr_call`

Pipelines/operators keep references to expressions; you can destroy expression handles after appending operators.

### Data interchange (Arrow C interfaces)

Record batches are exchanged through Arrow’s standard C interfaces:

- Arrow C Data interface: `ArrowSchema` + `ArrowArray` (batch is a struct array)
- Arrow C Stream interface: `ArrowArrayStream`

Ownership is explicit in the header comments:

- “push” APIs consume (`move`) the provided schema/array/stream
- “pull/export” APIs produce owned outputs that the caller must release

### Status

- The old pipeline/task C ABI was tied to removed `Pipeline/Task` wrappers.
- Once the new C ABI is designed, it will mirror the `LogicalPipeline -> TaskGroups` model.
