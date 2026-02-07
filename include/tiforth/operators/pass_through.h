// Copyright 2026 TiForth Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
