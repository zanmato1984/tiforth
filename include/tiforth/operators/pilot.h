#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>

#include <arrow/result.h>
#include <arrow/status.h>

#include "tiforth/pipeline/op/op.h"
#include "tiforth/task/blocked_resumer.h"

namespace tiforth {

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
class PilotAsyncPipeOp final : public pipeline::PipeOp {
 public:
  explicit PilotAsyncPipeOp(PilotAsyncOptions options) : options_(std::move(options)) {
    if (options_.block_cycles <= 0) {
      options_.block_cycles = 1;
    }
  }

  pipeline::PipelinePipe Pipe(const pipeline::PipelineContext&) override {
    return [this](const pipeline::PipelineContext&, const task::TaskContext&, pipeline::ThreadId,
                  std::optional<pipeline::Batch> input) -> pipeline::OpResult {
      if (options_.error_point == PilotErrorPoint::kPipe) {
        return options_.error_status;
      }

      if (!input.has_value()) {
        if (buffered_output_ != nullptr) {
          auto out = std::move(buffered_output_);
          buffered_output_.reset();
          return pipeline::OpOutput::PipeEven(std::move(out));
        }
        return pipeline::OpOutput::PipeSinkNeedsMore();
      }

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
          return pipeline::OpOutput::Blocked(std::make_shared<PilotResumer>(this, task::BlockedKind::kIOIn));
        case PilotBlockKind::kIOOut:
          state_ = State::kBlockedIO;
          return pipeline::OpOutput::Blocked(std::make_shared<PilotResumer>(this, task::BlockedKind::kIOOut));
        case PilotBlockKind::kWaiting:
          state_ = State::kBlockedWait;
          return pipeline::OpOutput::Blocked(
              std::make_shared<PilotResumer>(this, task::BlockedKind::kWaiting));
        case PilotBlockKind::kWaitForNotify:
          state_ = State::kWaitForNotify;
          return pipeline::OpOutput::Blocked(
              std::make_shared<PilotResumer>(this, task::BlockedKind::kWaitForNotify));
      }
      return arrow::Status::Invalid("unknown pilot block kind");
    };
  }

  pipeline::PipelineDrain Drain(const pipeline::PipelineContext&) override {
    return [](const pipeline::PipelineContext&, const task::TaskContext&,
              pipeline::ThreadId) -> pipeline::OpResult { return pipeline::OpOutput::Finished(); };
  }

 private:
  enum class State {
    kIdle,
    kBlockedIO,
    kBlockedWait,
    kWaitForNotify,
  };

  class PilotResumer final : public task::BlockedResumer {
   public:
    PilotResumer(PilotAsyncPipeOp* op, task::BlockedKind kind) : op_(op), kind_(kind) {}

    void Resume() override { resumed_.store(true, std::memory_order_release); }
    bool IsResumed() const override { return resumed_.load(std::memory_order_acquire); }

    task::BlockedKind kind() const override { return kind_; }

    arrow::Result<std::optional<task::BlockedKind>> ExecuteIO() override {
      if (op_ == nullptr) {
        return arrow::Status::Invalid("pilot resumer has null operator");
      }
      if (op_->options_.error_point == PilotErrorPoint::kExecuteIO) {
        return op_->options_.error_status;
      }
      if (op_->state_ != State::kBlockedIO) {
        return arrow::Status::Invalid("pilot operator is not in IO state");
      }
      if (kind_ != task::BlockedKind::kIOIn && kind_ != task::BlockedKind::kIOOut) {
        return arrow::Status::Invalid("pilot resumer kind is not IO");
      }
      if (op_->remaining_block_cycles_ <= 0) {
        return arrow::Status::Invalid("pilot operator IO cycle counter underflow");
      }
      if (--op_->remaining_block_cycles_ > 0) {
        return kind_;
      }
      op_->BufferAndReset();
      return std::nullopt;
    }

    arrow::Result<std::optional<task::BlockedKind>> Await() override {
      if (op_ == nullptr) {
        return arrow::Status::Invalid("pilot resumer has null operator");
      }
      if (op_->options_.error_point == PilotErrorPoint::kAwait) {
        return op_->options_.error_status;
      }
      if (op_->state_ != State::kBlockedWait) {
        return arrow::Status::Invalid("pilot operator is not in await state");
      }
      if (kind_ != task::BlockedKind::kWaiting) {
        return arrow::Status::Invalid("pilot resumer kind is not Waiting");
      }
      if (op_->remaining_block_cycles_ <= 0) {
        return arrow::Status::Invalid("pilot operator await cycle counter underflow");
      }
      if (--op_->remaining_block_cycles_ > 0) {
        return kind_;
      }
      op_->BufferAndReset();
      return std::nullopt;
    }

    arrow::Status Notify() override {
      if (op_ == nullptr) {
        return arrow::Status::Invalid("pilot resumer has null operator");
      }
      if (op_->options_.error_point == PilotErrorPoint::kNotify) {
        return op_->options_.error_status;
      }
      if (op_->state_ != State::kWaitForNotify) {
        return arrow::Status::Invalid("pilot operator is not waiting for notify");
      }
      if (kind_ != task::BlockedKind::kWaitForNotify) {
        return arrow::Status::Invalid("pilot resumer kind is not WaitForNotify");
      }
      op_->BufferAndReset();
      return arrow::Status::OK();
    }

   private:
    PilotAsyncPipeOp* op_ = nullptr;
    task::BlockedKind kind_;
    std::atomic_bool resumed_{false};
  };

  void BufferAndReset() {
    buffered_output_ = std::move(pending_input_);
    pending_input_.reset();
    remaining_block_cycles_ = 0;
    state_ = State::kIdle;
  }

  PilotAsyncOptions options_;
  State state_ = State::kIdle;

  int32_t remaining_block_cycles_ = 0;
  std::shared_ptr<arrow::RecordBatch> pending_input_;
  std::shared_ptr<arrow::RecordBatch> buffered_output_;
};

}  // namespace tiforth
