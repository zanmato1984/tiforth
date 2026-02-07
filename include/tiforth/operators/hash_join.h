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

#include <memory>
#include <string>
#include <vector>

#include "tiforth/traits.h"

namespace arrow {
class MemoryPool;
class RecordBatch;
}  // namespace arrow

namespace tiforth {
class Engine;
}  // namespace tiforth

namespace tiforth::op {
struct JoinKey {
  std::vector<std::string> left;
  std::vector<std::string> right;
};

class HashJoinPipeOp final : public PipeOp {
 public:
  HashJoinPipeOp(const Engine* engine,
                 std::vector<std::shared_ptr<arrow::RecordBatch>> build_batches, JoinKey key,
                 arrow::MemoryPool* memory_pool = nullptr);
  ~HashJoinPipeOp() override;

  PipelinePipe Pipe() override;
  PipelineDrain Drain() override;
  std::unique_ptr<SourceOp> ImplicitSource() override;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace tiforth::op
