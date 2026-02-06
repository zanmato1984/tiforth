#pragma once

#include "tiforth/traits.h"

namespace tiforth::op {

class PassThroughPipeOp final : public PipeOp {
 public:
  PipelinePipe Pipe() override {
    return [](const TaskContext&, ThreadId, std::optional<Batch> input) -> OpResult {
      if (!input.has_value()) {
        return OpOutput::PipeSinkNeedsMore();
      }
      auto batch = std::move(*input);
      if (batch == nullptr) {
        return arrow::Status::Invalid("pass-through input batch must not be null");
      }
      return OpOutput::PipeEven(std::move(batch));
    };
  }

  PipelineDrain Drain() override {
    return [](const TaskContext&, ThreadId) -> OpResult { return OpOutput::Finished(); };
  }

  std::unique_ptr<SourceOp> ImplicitSource() override { return nullptr; }
};

}  // namespace tiforth::op
