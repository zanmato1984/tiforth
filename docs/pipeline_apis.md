# Pipeline APIs (C++)

TiForth exposes a small set of stable “host APIs” for building and executing pipelines:

- C++ API:
  - `tiforth::Engine`
  - `tiforth::{SourceOp,PipeOp,SinkOp,Pipeline}`
  - `bp::Compile(...) -> tiforth::PipelineExec`
  - `tiforth::{Task,TaskGroup,TaskContext,TaskStatus,Awaiter,Resumer}`

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

A `tiforth::Pipeline` is composed of:

- N **channels** (`Pipeline::Channel`), each with:
  - a `SourceOp*`
  - zero or more `PipeOp*` in order
- One `SinkOp*` shared by all channels.

The host owns all operator objects. The pipeline stores raw pointers, so the host must keep the
operators alive while running the pipeline.

Example (filter + projection; 1 channel):

```cpp
auto engine = tiforth::Engine::Create({}).ValueOrDie();

auto source_op = /* host-defined SourceOp */;

std::vector<std::unique_ptr<tiforth::PipeOp>> pipe_ops;
pipe_ops.push_back(std::make_unique<tiforth::op::FilterPipeOp>(
    engine.get(),
    tiforth::MakeCall("less", {tiforth::MakeFieldRef("a"),
                               tiforth::MakeLiteral(std::make_shared<arrow::Int32Scalar>(10))})));
pipe_ops.push_back(std::make_unique<tiforth::op::ProjectionPipeOp>(
    engine.get(), std::vector<tiforth::op::ProjectionExpr>{{"b", tiforth::MakeFieldRef("b")}}));

auto sink_op = /* host-defined SinkOp */;

tiforth::Pipeline::Channel channel;
channel.source_op = source_op.get();
for (auto& op : pipe_ops) channel.pipe_ops.push_back(op.get());

tiforth::Pipeline logical_pipeline{
    "FilterProjection", {std::move(channel)}, sink_op.get()};
```

### Compilation: `Pipeline -> PipelineExec`

TiForth uses Broken Pipeline for compilation. Use `bp::Compile(...)` to compile a `Pipeline`
into a `PipelineExec`:

The host is responsible for:

- choosing `dop` (degree of parallelism) and constructing operators accordingly
- orchestrating and executing the compiled plan (task groups)
- providing a `tiforth::TaskContext` with resumer/awaiter factories.

Minimal sketch:

```cpp
auto exec = bp::Compile(logical_pipeline, /*dop=*/4).ValueOrDie();

// task_ctx.context = ... (optional QueryContext pointer)
// task_ctx.resumer_factory = ...
// task_ctx.awaiter_factory = ...
```

The TiForth unit tests and consumer examples include a small helper that flattens a `PipelineExec`
into an ordered `TaskGroups` list for a simple “run groups in order” harness.
