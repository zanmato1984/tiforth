#include "tiforth/pipeline/task_groups.h"

#include <memory>
#include <utility>
#include <vector>

#include <arrow/status.h>
#include <arrow/util/logging.h>

#include "tiforth/pipeline/physical_pipeline.h"
#include "tiforth/pipeline/pipeline_task.h"

namespace tiforth::pipeline {

namespace {

arrow::Status ValidateLogicalPipeline(const LogicalPipeline& logical_pipeline) {
  auto* sink = logical_pipeline.SinkOp();
  if (sink == nullptr) {
    return arrow::Status::Invalid("logical pipeline sink must not be null");
  }

  for (const auto& channel : logical_pipeline.Channels()) {
    if (channel.source_op == nullptr) {
      return arrow::Status::Invalid("logical pipeline channel source must not be null");
    }
    for (const auto* pipe : channel.pipe_ops) {
      if (pipe == nullptr) {
        return arrow::Status::Invalid("logical pipeline pipe op must not be null");
      }
    }
  }

  return arrow::Status::OK();
}

void AppendTaskGroups(task::TaskGroups* dst, task::TaskGroups src) {
  if (dst == nullptr) {
    return;
  }
  dst->reserve(dst->size() + src.size());
  for (auto& group : src) {
    dst->push_back(std::move(group));
  }
}

}  // namespace

arrow::Result<task::TaskGroups> CompileToTaskGroups(PipelineContext pipeline_ctx,
                                                    const LogicalPipeline& logical_pipeline,
                                                    std::size_t dop) {
  if (dop == 0) {
    return arrow::Status::Invalid("dop must be positive");
  }
  ARROW_RETURN_NOT_OK(ValidateLogicalPipeline(logical_pipeline));

  auto pipeline_ctx_sp = std::make_shared<PipelineContext>(std::move(pipeline_ctx));

  auto physical_pipelines = CompilePipeline(*pipeline_ctx_sp, logical_pipeline);

  task::TaskGroups groups;
  groups.reserve(physical_pipelines.size() + 8);

  for (auto& physical_pipeline : physical_pipelines) {
    auto physical_sp = std::make_shared<PhysicalPipeline>(std::move(physical_pipeline));

    std::vector<SourceOp*> sources;
    sources.reserve(physical_sp->Channels().size());
    for (const auto& channel : physical_sp->Channels()) {
      if (channel.source_op == nullptr) {
        return arrow::Status::Invalid("physical pipeline channel source must not be null");
      }
      bool seen = false;
      for (auto* existing : sources) {
        if (existing == channel.source_op) {
          seen = true;
          break;
        }
      }
      if (!seen) {
        sources.push_back(channel.source_op);
      }
    }

    for (auto* source : sources) {
      AppendTaskGroups(&groups, source->Frontend(*pipeline_ctx_sp));
    }

    auto pipeline_task_sp = std::make_shared<PipelineTask>(*pipeline_ctx_sp, *physical_sp, dop);

    task::Task stage_task{
        physical_sp->name(),
        [pipeline_ctx_sp, physical_sp, pipeline_task_sp, dop](
            const task::TaskContext& task_ctx, task::TaskId task_id) -> task::TaskResult {
          if (task_id >= dop) {
            return arrow::Status::Invalid("task_id out of range");
          }
          return (*pipeline_task_sp)(*pipeline_ctx_sp, task_ctx, static_cast<ThreadId>(task_id));
        }};

    groups.emplace_back(physical_sp->name(), std::move(stage_task), dop, /*continuation=*/std::nullopt,
                        /*notify_finish=*/task::TaskGroup::NotifyFinishFunc{});

    for (auto* source : sources) {
      if (auto backend = source->Backend(*pipeline_ctx_sp); backend.has_value()) {
        groups.push_back(std::move(*backend));
      }
    }
  }

  auto* sink = logical_pipeline.SinkOp();
  ARROW_DCHECK(sink != nullptr);

  AppendTaskGroups(&groups, sink->Frontend(*pipeline_ctx_sp));
  if (auto backend = sink->Backend(*pipeline_ctx_sp); backend.has_value()) {
    groups.push_back(std::move(*backend));
  }

  return groups;
}

}  // namespace tiforth::pipeline
