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
#include <mutex>
#include <optional>
#include <thread>
#include <utility>

#include <arrow/result.h>
#include <arrow/status.h>

#include "tiforth/traits.h"

#include "tiforth/testing/test_blocked_resumer.h"

namespace tiforth::test {

enum class PilotBlockKind {
  kIOIn,
  kIOOut,
  kWaiting,
  kWaitForNotify,
};

enum class PilotErrorPoint {
  kNone,
  kPipe,
  kExecuteIO,
  kAwait,
  kNotify,
};

struct PilotAsyncOptions {
  PilotBlockKind block_kind = PilotBlockKind::kIOIn;
  int32_t block_cycles = 1;

  PilotErrorPoint error_point = PilotErrorPoint::kNone;
  arrow::Status error_status = arrow::Status::IOError("pilot operator error");
};

// A small "pilot" pipe operator to validate TiForth's full blocked-state Task model:
// - Pipe blocks with IO/await/notify states.
// - Unblocks by buffering output and producing it via continuation (Pipe called with nullopt).
// - Can inject an error at a configured phase.
class PilotAsyncPipeOp final : public PipeOp {
 public:
  explicit PilotAsyncPipeOp(PilotAsyncOptions options) : options_(std::move(options)) {
    if (options_.block_cycles <= 0) {
      options_.block_cycles = 1;
    }
  }

  PipelinePipe Pipe() override {
    return [this](const TaskContext&, ThreadId, std::optional<Batch> input) -> OpResult {
      if (options_.error_point == PilotErrorPoint::kPipe) {
        return options_.error_status;
      }

      std::shared_ptr<arrow::RecordBatch> output;
      std::shared_ptr<PilotResumer> resumer;

      {
        std::lock_guard<std::mutex> lock(state_mu_);
        if (async_error_.has_value()) {
          auto status = *async_error_;
          async_error_.reset();
          return status;
        }

        if (!input.has_value()) {
          if (buffered_output_ != nullptr) {
            output = std::move(buffered_output_);
            buffered_output_.reset();
          } else {
            return OpOutput::PipeSinkNeedsMore();
          }
        } else {
          if (state_ != State::kIdle) {
            return arrow::Status::Invalid("pilot operator is not idle");
          }

          auto batch = std::move(*input);
          if (batch == nullptr) {
            return arrow::Status::Invalid("pilot input batch must not be null");
          }

          pending_input_ = std::move(batch);
          remaining_block_cycles_ = options_.block_cycles;

          switch (options_.block_kind) {
            case PilotBlockKind::kIOIn:
              state_ = State::kBlockedIO;
              resumer = std::make_shared<PilotResumer>(this, BlockedKind::kIOIn);
              break;
            case PilotBlockKind::kIOOut:
              state_ = State::kBlockedIO;
              resumer = std::make_shared<PilotResumer>(this, BlockedKind::kIOOut);
              break;
            case PilotBlockKind::kWaiting:
              state_ = State::kBlockedWait;
              resumer = std::make_shared<PilotResumer>(this, BlockedKind::kWaiting);
              break;
            case PilotBlockKind::kWaitForNotify:
              state_ = State::kWaitForNotify;
              resumer = std::make_shared<PilotResumer>(this, BlockedKind::kWaitForNotify);
              break;
          }
        }
      }

      if (output != nullptr) {
        return OpOutput::PipeEven(std::move(output));
      }

      if (resumer != nullptr) {
        resumer->StartAutoResume();
        return OpOutput::Blocked(std::move(resumer));
      }

      return arrow::Status::Invalid("unknown pilot block kind");
    };
  }

  PipelineDrain Drain() override {
    return [](const TaskContext&, ThreadId) -> OpResult { return OpOutput::Finished(); };
  }

  std::unique_ptr<SourceOp> ImplicitSource() override { return nullptr; }

 private:
  enum class State {
    kIdle,
    kBlockedIO,
    kBlockedWait,
    kWaitForNotify,
  };

  class PilotResumer final : public BlockedResumer,
                             public std::enable_shared_from_this<PilotResumer> {
   public:
    PilotResumer(PilotAsyncPipeOp* op, BlockedKind kind) : op_(op), kind_(kind) {}

    BlockedKind kind() const override { return kind_; }

    void StartAutoResume() {
      if (kind_ == BlockedKind::kWaitForNotify) {
        return;
      }
      auto self = shared_from_this();
      std::thread([self]() { self->Drive(); }).detach();
    }

    arrow::Result<std::optional<BlockedKind>> ExecuteIO() override {
      if (op_ == nullptr) {
        return arrow::Status::Invalid("pilot resumer has null operator");
      }
      if (op_->options_.error_point == PilotErrorPoint::kExecuteIO) {
        return op_->options_.error_status;
      }
      if (kind_ != BlockedKind::kIOIn && kind_ != BlockedKind::kIOOut) {
        return arrow::Status::Invalid("pilot resumer kind is not IO");
      }
      std::lock_guard<std::mutex> lock(op_->state_mu_);
      if (op_->state_ != State::kBlockedIO) {
        return arrow::Status::Invalid("pilot operator is not in IO state");
      }
      if (op_->remaining_block_cycles_ <= 0) {
        return arrow::Status::Invalid("pilot operator IO cycle counter underflow");
      }
      if (--op_->remaining_block_cycles_ > 0) {
        return kind_;
      }
      op_->BufferAndResetLocked();
      return std::nullopt;
    }

    arrow::Result<std::optional<BlockedKind>> Await() override {
      if (op_ == nullptr) {
        return arrow::Status::Invalid("pilot resumer has null operator");
      }
      if (op_->options_.error_point == PilotErrorPoint::kAwait) {
        return op_->options_.error_status;
      }
      if (kind_ != BlockedKind::kWaiting) {
        return arrow::Status::Invalid("pilot resumer kind is not Waiting");
      }
      std::lock_guard<std::mutex> lock(op_->state_mu_);
      if (op_->state_ != State::kBlockedWait) {
        return arrow::Status::Invalid("pilot operator is not in await state");
      }
      if (op_->remaining_block_cycles_ <= 0) {
        return arrow::Status::Invalid("pilot operator await cycle counter underflow");
      }
      if (--op_->remaining_block_cycles_ > 0) {
        return kind_;
      }
      op_->BufferAndResetLocked();
      return std::nullopt;
    }

    arrow::Status Notify() override {
      if (op_ == nullptr) {
        return arrow::Status::Invalid("pilot resumer has null operator");
      }
      if (op_->options_.error_point == PilotErrorPoint::kNotify) {
        return op_->options_.error_status;
      }
      if (kind_ != BlockedKind::kWaitForNotify) {
        return arrow::Status::Invalid("pilot resumer kind is not WaitForNotify");
      }
      std::lock_guard<std::mutex> lock(op_->state_mu_);
      if (op_->state_ != State::kWaitForNotify) {
        return arrow::Status::Invalid("pilot operator is not waiting for notify");
      }
      op_->BufferAndResetLocked();
      return arrow::Status::OK();
    }

   private:
    void Drive() {
      while (true) {
        arrow::Result<std::optional<BlockedKind>> step =
            (kind_ == BlockedKind::kWaiting) ? Await() : ExecuteIO();
        if (!step.ok()) {
          if (op_ != nullptr) {
            op_->RecordAsyncError(step.status());
          }
          Resume();
          return;
        }
        if (!step->has_value()) {
          Resume();
          return;
        }
        std::this_thread::yield();
      }
    }

    PilotAsyncPipeOp* op_ = nullptr;
    BlockedKind kind_;
  };

  void BufferAndResetLocked() {
    buffered_output_ = std::move(pending_input_);
    pending_input_.reset();
    remaining_block_cycles_ = 0;
    state_ = State::kIdle;
  }

  void RecordAsyncError(const arrow::Status& status) {
    std::lock_guard<std::mutex> lock(state_mu_);
    async_error_ = status;
    pending_input_.reset();
    buffered_output_.reset();
    remaining_block_cycles_ = 0;
    state_ = State::kIdle;
  }

  PilotAsyncOptions options_;
  State state_ = State::kIdle;

  std::mutex state_mu_;
  std::optional<arrow::Status> async_error_;
  int32_t remaining_block_cycles_ = 0;
  std::shared_ptr<arrow::RecordBatch> pending_input_;
  std::shared_ptr<arrow::RecordBatch> buffered_output_;
};

}  // namespace tiforth::test
