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

#include <arrow/compute/exec.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>

#include "tiforth/traits.h"

namespace arrow {
class Array;
class MemoryPool;
class Schema;
}  // namespace arrow

namespace tiforth {
class Engine;
}  // namespace tiforth

namespace tiforth::op {
struct SortKey {
  std::string name;
  bool ascending = true;
  bool nulls_first = false;
};

class SortPipeOp final : public PipeOp {
 public:
  SortPipeOp(const Engine* engine, std::vector<SortKey> keys, arrow::MemoryPool* memory_pool = nullptr);

  PipelinePipe Pipe() override;
  PipelineDrain Drain() override;
  std::unique_ptr<SourceOp> ImplicitSource() override;

 private:
  arrow::Result<std::shared_ptr<arrow::RecordBatch>> SortAll();

  std::vector<SortKey> keys_;
  std::shared_ptr<arrow::Schema> output_schema_;
  std::vector<std::shared_ptr<arrow::RecordBatch>> buffered_;

  bool drained_ = false;

  arrow::compute::ExecContext exec_context_;
};

}  // namespace tiforth::op
