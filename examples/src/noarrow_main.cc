#include <arrow/result.h>
#include <arrow/status.h>

#include <iostream>
#include <optional>
#include <vector>

#include "tiforth/tiforth.h"
#include "task_group_runner.h"

namespace {

class EmptySourceOp final : public tiforth::pipeline::SourceOp {
 public:
  tiforth::pipeline::PipelineSource Source(const tiforth::pipeline::PipelineContext&) override {
    return [](const tiforth::pipeline::PipelineContext&, const tiforth::task::TaskContext&,
              tiforth::pipeline::ThreadId) -> tiforth::pipeline::OpResult {
      return tiforth::pipeline::OpOutput::Finished();
    };
  }
};

class NullSinkOp final : public tiforth::pipeline::SinkOp {
 public:
  tiforth::pipeline::PipelineSink Sink(const tiforth::pipeline::PipelineContext&) override {
    return [](const tiforth::pipeline::PipelineContext&, const tiforth::task::TaskContext&,
              tiforth::pipeline::ThreadId, std::optional<tiforth::pipeline::Batch>)
               -> tiforth::pipeline::OpResult { return tiforth::pipeline::OpOutput::PipeSinkNeedsMore(); };
  }
};

arrow::Status RunTiForthSmoke() {
  ARROW_ASSIGN_OR_RAISE(auto engine, tiforth::Engine::Create(tiforth::EngineOptions{}));
  (void)engine;

  EmptySourceOp source_op;
  NullSinkOp sink_op;

  tiforth::pipeline::LogicalPipeline::Channel channel;
  channel.source_op = &source_op;
  channel.pipe_ops = {};

  tiforth::pipeline::LogicalPipeline logical_pipeline{
      "NoArrowSmoke",
      std::vector<tiforth::pipeline::LogicalPipeline::Channel>{std::move(channel)},
      &sink_op};

  ARROW_ASSIGN_OR_RAISE(auto task_groups,
                        tiforth::pipeline::CompileToTaskGroups(tiforth::pipeline::PipelineContext{},
                                                              logical_pipeline, /*dop=*/1));

  auto task_ctx = tiforth_example::MakeTaskContext();
  return tiforth_example::RunTaskGroupsToCompletion(task_groups, task_ctx);
}

}  // namespace

int main() {
  auto status = RunTiForthSmoke();
  if (!status.ok()) {
    std::cerr << status.ToString() << "\n";
    return 1;
  }
  return 0;
}
