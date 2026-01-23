#pragma once

#include <cstddef>

#include <arrow/result.h>

#include "tiforth/pipeline/logical_pipeline.h"
#include "tiforth/pipeline/pipeline_context.h"
#include "tiforth/task/task_group.h"

namespace tiforth::pipeline {

// Compile a logical pipeline into an ordered sequence of TaskGroups.
//
// Execution model:
// - For each compiled PhysicalPipeline stage:
//   - run all channel SourceOp Frontend groups (deduplicated, in channel order)
//   - run the PipelineTask group (num_tasks = dop)
//   - run all channel SourceOp Backend groups (deduplicated, in channel order)
// - Finally, run the SinkOp Frontend groups and optional Backend group.
//
// The host is responsible for:
// - choosing `dop` and constructing operators accordingly
// - keeping all operator objects alive while running the returned task groups
// - providing a TaskContext with resumer/awaiter factories.
arrow::Result<task::TaskGroups> CompileToTaskGroups(PipelineContext pipeline_ctx,
                                                    const LogicalPipeline& logical_pipeline,
                                                    std::size_t dop);

}  // namespace tiforth::pipeline

