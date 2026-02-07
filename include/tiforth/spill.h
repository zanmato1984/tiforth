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

#include <cstdint>
#include <memory>
#include <optional>

#include <arrow/result.h>
#include <arrow/status.h>

namespace arrow {
class RecordBatch;
class RecordBatchReader;
}  // namespace arrow

namespace tiforth {

struct SpillHandle {
  uint64_t id = 0;
};

class SpillManager {
 public:
  virtual ~SpillManager() = default;

  virtual arrow::Result<std::optional<SpillHandle>> RequestSpill(int64_t bytes_hint) = 0;
  virtual arrow::Status WriteSpill(SpillHandle handle,
                                   std::shared_ptr<arrow::RecordBatch> batch) = 0;
  virtual arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> ReadSpill(SpillHandle handle) = 0;
};

class DenySpillManager final : public SpillManager {
 public:
  arrow::Result<std::optional<SpillHandle>> RequestSpill(int64_t bytes_hint) override {
    (void)bytes_hint;
    return std::nullopt;
  }

  arrow::Status WriteSpill(SpillHandle handle,
                           std::shared_ptr<arrow::RecordBatch> batch) override {
    (void)handle;
    (void)batch;
    return arrow::Status::NotImplemented("spilling is disabled");
  }

  arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> ReadSpill(SpillHandle handle) override {
    (void)handle;
    return arrow::Status::NotImplemented("spilling is disabled");
  }
};

}  // namespace tiforth

