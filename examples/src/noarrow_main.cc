#include <arrow/result.h>
#include <arrow/status.h>

#include <iostream>
#include <optional>
#include <vector>

#include "tiforth/tiforth.h"
#include "task_group_runner.h"

namespace {

class EmptySourceOp final : public tiforth::SourceOp {
 public:
  tiforth::PipelineSource Source() override {
    return [](const tiforth::TaskContext&, tiforth::ThreadId) -> tiforth::OpResult {
      return tiforth::OpOutput::Finished();
    };
  }

  tiforth::TaskGroups Frontend() override { return {}; }
  std::optional<tiforth::TaskGroup> Backend() override { return std::nullopt; }
};

class NullSinkOp final : public tiforth::SinkOp {
 public:
  tiforth::PipelineSink Sink() override {
    return [](const tiforth::TaskContext&, tiforth::ThreadId,
              std::optional<tiforth::Batch>) -> tiforth::OpResult {
      return tiforth::OpOutput::PipeSinkNeedsMore();
    };
  }

  tiforth::TaskGroups Frontend() override { return {}; }
  std::optional<tiforth::TaskGroup> Backend() override { return std::nullopt; }
  std::unique_ptr<tiforth::SourceOp> ImplicitSource() override { return nullptr; }
};

arrow::Status RunTiForthSmoke() {
  ARROW_ASSIGN_OR_RAISE(auto engine, tiforth::Engine::Create(tiforth::EngineOptions{}));
  (void)engine;

  EmptySourceOp source_op;
  NullSinkOp sink_op;

  tiforth::LogicalPipeline::Channel channel;
  channel.source_op = &source_op;
  channel.pipe_ops = {};

  tiforth::LogicalPipeline logical_pipeline{
      "NoArrowSmoke",
      std::vector<tiforth::LogicalPipeline::Channel>{std::move(channel)},
      &sink_op};

  ARROW_ASSIGN_OR_RAISE(auto task_groups,
                        tiforth_example::CompileToTaskGroups(logical_pipeline, /*dop=*/1));

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
