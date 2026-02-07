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

#include <cstddef>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/status.h>

#include "tiforth/traits.h"

namespace tiforth::test {

class VectorSourceOp final : public SourceOp {
 public:
  explicit VectorSourceOp(std::vector<std::shared_ptr<arrow::RecordBatch>> batches)
      : batches_(std::move(batches)) {}

  PipelineSource Source() override {
    return [this](const TaskContext&, ThreadId thread_id) -> OpResult {
      if (thread_id != 0) {
        return arrow::Status::Invalid("VectorSourceOp only supports thread_id=0");
      }
      if (next_ >= batches_.size()) {
        return OpOutput::Finished();
      }
      auto batch = batches_[next_++];
      if (batch == nullptr) {
        return arrow::Status::Invalid("source batch must not be null");
      }
      return OpOutput::SourcePipeHasMore(std::move(batch));
    };
  }

  TaskGroups Frontend() override { return {}; }
  std::optional<TaskGroup> Backend() override { return std::nullopt; }

 private:
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches_;
  std::size_t next_ = 0;
};

class PartitionedVectorSourceOp final : public SourceOp {
 public:
  using Partitions = std::vector<std::vector<std::shared_ptr<arrow::RecordBatch>>>;

  explicit PartitionedVectorSourceOp(std::shared_ptr<const Partitions> partitions)
      : partitions_(std::move(partitions)),
        offsets_(partitions_ != nullptr ? partitions_->size() : 0, 0) {}

  PipelineSource Source() override {
    return [this](const TaskContext&, ThreadId thread_id) -> OpResult {
      if (partitions_ == nullptr) {
        return arrow::Status::Invalid("partitions must not be null");
      }
      if (thread_id >= partitions_->size()) {
        return arrow::Status::Invalid("thread id out of range");
      }
      if (thread_id >= offsets_.size()) {
        return arrow::Status::Invalid("thread offset out of range");
      }

      auto& offset = offsets_[thread_id];
      const auto& batches = (*partitions_)[thread_id];
      if (offset >= batches.size()) {
        return OpOutput::Finished();
      }
      auto batch = batches[offset++];
      if (batch == nullptr) {
        return arrow::Status::Invalid("source batch must not be null");
      }
      return OpOutput::SourcePipeHasMore(std::move(batch));
    };
  }

  TaskGroups Frontend() override { return {}; }
  std::optional<TaskGroup> Backend() override { return std::nullopt; }

 private:
  std::shared_ptr<const Partitions> partitions_;
  std::vector<std::size_t> offsets_;
};

inline std::shared_ptr<PartitionedVectorSourceOp::Partitions> RoundRobinPartition(
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& inputs, std::size_t dop) {
  auto partitions = std::make_shared<PartitionedVectorSourceOp::Partitions>(dop == 0 ? 1 : dop);
  for (std::size_t i = 0; i < inputs.size(); ++i) {
    (*partitions)[i % partitions->size()].push_back(inputs[i]);
  }
  return partitions;
}

class CollectSinkOp final : public SinkOp {
 public:
  using OutputsByThread = std::vector<std::vector<std::shared_ptr<arrow::RecordBatch>>>;

  explicit CollectSinkOp(OutputsByThread* outputs_by_thread)
      : outputs_by_thread_(outputs_by_thread) {}

  PipelineSink Sink() override {
    return [this](const TaskContext&, ThreadId thread_id,
                  std::optional<Batch> input) -> OpResult {
      if (outputs_by_thread_ == nullptr) {
        return arrow::Status::Invalid("outputs_by_thread must not be null");
      }
      if (thread_id >= outputs_by_thread_->size()) {
        return arrow::Status::Invalid("thread_id out of range");
      }
      if (!input.has_value()) {
        return OpOutput::PipeSinkNeedsMore();
      }
      auto batch = std::move(*input);
      if (batch != nullptr) {
        (*outputs_by_thread_)[thread_id].push_back(std::move(batch));
      }
      return OpOutput::PipeSinkNeedsMore();
    };
  }

  TaskGroups Frontend() override { return {}; }
  std::optional<TaskGroup> Backend() override { return std::nullopt; }
  std::unique_ptr<SourceOp> ImplicitSource() override { return nullptr; }

 private:
  OutputsByThread* outputs_by_thread_ = nullptr;
};

inline std::vector<std::shared_ptr<arrow::RecordBatch>> FlattenOutputs(
    CollectSinkOp::OutputsByThread outputs_by_thread) {
  std::vector<std::shared_ptr<arrow::RecordBatch>> out;
  for (auto& per_thread : outputs_by_thread) {
    for (auto& batch : per_thread) {
      out.push_back(std::move(batch));
    }
  }
  return out;
}

}  // namespace tiforth::test
