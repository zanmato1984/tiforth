#pragma once

#include "tiforth/pipeline/op/op.h"

namespace tiforth {

class PassThroughPipeOp final : public pipeline::PipeOp {
 public:
  pipeline::PipelinePipe Pipe(const pipeline::PipelineContext&) override {
    return [](const pipeline::PipelineContext&, const task::TaskContext&, pipeline::ThreadId,
              std::optional<pipeline::Batch> input) -> pipeline::OpResult {
      if (!input.has_value()) {
        return pipeline::OpOutput::PipeSinkNeedsMore();
      }
      auto batch = std::move(*input);
      if (batch == nullptr) {
        return arrow::Status::Invalid("pass-through input batch must not be null");
      }
      return pipeline::OpOutput::PipeEven(std::move(batch));
    };
  }

  pipeline::PipelineDrain Drain(const pipeline::PipelineContext&) override {
    return [](const pipeline::PipelineContext&, const task::TaskContext&,
              pipeline::ThreadId) -> pipeline::OpResult { return pipeline::OpOutput::Finished(); };
  }
};

}  // namespace tiforth
