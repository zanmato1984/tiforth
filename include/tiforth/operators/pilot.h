#pragma once

#include <cstdint>
#include <memory>
#include <utility>

#include <arrow/result.h>
#include <arrow/status.h>

#include "tiforth/operators.h"

namespace tiforth {

enum class PilotBlockKind {
  kIOIn,
  kIOOut,
  kWaiting,
  kWaitForNotify,
};

enum class PilotErrorPoint {
  kNone,
  kTransform,
  kExecuteIO,
  kAwait,
  kNotify,
};

struct PilotAsyncTransformOptions {
  PilotBlockKind block_kind = PilotBlockKind::kIOIn;
  int32_t block_cycles = 1;

  PilotErrorPoint error_point = PilotErrorPoint::kNone;
  arrow::Status error_status = arrow::Status::IOError("pilot operator error");
};

// A small "pilot" operator to validate TiForth's full blocked-state Task model:
// - Transform blocks with IO/await/notify states.
// - Unblocks by buffering output and resuming via TryOutput().
// - Can inject an error at a configured phase.
class PilotAsyncTransformOp final : public TransformOp {
 public:
  explicit PilotAsyncTransformOp(PilotAsyncTransformOptions options)
      : options_(std::move(options)) {
    if (options_.block_cycles <= 0) {
      options_.block_cycles = 1;
    }
  }

 protected:
  arrow::Result<OperatorStatus> TryOutputImpl(
      std::shared_ptr<arrow::RecordBatch>* batch) override {
    if (batch == nullptr) {
      return arrow::Status::Invalid("batch output must not be null");
    }
    if (buffered_output_ == nullptr) {
      batch->reset();
      return OperatorStatus::kNeedInput;
    }
    *batch = std::move(buffered_output_);
    return OperatorStatus::kHasOutput;
  }

  arrow::Result<OperatorStatus> TransformImpl(
      std::shared_ptr<arrow::RecordBatch>* batch) override {
    if (batch == nullptr) {
      return arrow::Status::Invalid("batch input must not be null");
    }
    if (options_.error_point == PilotErrorPoint::kTransform) {
      return options_.error_status;
    }
    if (state_ != State::kIdle) {
      return arrow::Status::Invalid("pilot operator is not idle");
    }

    // End-of-stream marker is forwarded as-is (no blocking).
    if (*batch == nullptr) {
      return OperatorStatus::kHasOutput;
    }

    pending_input_ = std::move(*batch);
    batch->reset();
    remaining_block_cycles_ = options_.block_cycles;

    switch (options_.block_kind) {
      case PilotBlockKind::kIOIn:
        state_ = State::kBlockedIO;
        return OperatorStatus::kIOIn;
      case PilotBlockKind::kIOOut:
        state_ = State::kBlockedIO;
        return OperatorStatus::kIOOut;
      case PilotBlockKind::kWaiting:
        state_ = State::kBlockedWait;
        return OperatorStatus::kWaiting;
      case PilotBlockKind::kWaitForNotify:
        state_ = State::kWaitForNotify;
        return OperatorStatus::kWaitForNotify;
    }
    return arrow::Status::Invalid("unknown pilot block kind");
  }

  arrow::Result<OperatorStatus> ExecuteIOImpl() override {
    if (options_.error_point == PilotErrorPoint::kExecuteIO) {
      return options_.error_status;
    }
    if (state_ != State::kBlockedIO) {
      return arrow::Status::Invalid("pilot operator is not in IO state");
    }
    if (remaining_block_cycles_ <= 0) {
      return arrow::Status::Invalid("pilot operator IO cycle counter underflow");
    }
    if (--remaining_block_cycles_ > 0) {
      return options_.block_kind == PilotBlockKind::kIOOut ? OperatorStatus::kIOOut
                                                          : OperatorStatus::kIOIn;
    }
    BufferAndReset();
    return OperatorStatus::kNeedInput;
  }

  arrow::Result<OperatorStatus> AwaitImpl() override {
    if (options_.error_point == PilotErrorPoint::kAwait) {
      return options_.error_status;
    }
    if (state_ != State::kBlockedWait) {
      return arrow::Status::Invalid("pilot operator is not in await state");
    }
    if (remaining_block_cycles_ <= 0) {
      return arrow::Status::Invalid("pilot operator await cycle counter underflow");
    }
    if (--remaining_block_cycles_ > 0) {
      return OperatorStatus::kWaiting;
    }
    BufferAndReset();
    return OperatorStatus::kNeedInput;
  }

  arrow::Status NotifyImpl() override {
    if (options_.error_point == PilotErrorPoint::kNotify) {
      return options_.error_status;
    }
    if (state_ != State::kWaitForNotify) {
      return arrow::Status::Invalid("pilot operator is not waiting for notify");
    }
    BufferAndReset();
    return arrow::Status::OK();
  }

 private:
  enum class State {
    kIdle,
    kBlockedIO,
    kBlockedWait,
    kWaitForNotify,
  };

  void BufferAndReset() {
    buffered_output_ = std::move(pending_input_);
    pending_input_.reset();
    remaining_block_cycles_ = 0;
    state_ = State::kIdle;
  }

  PilotAsyncTransformOptions options_;
  State state_ = State::kIdle;

  int32_t remaining_block_cycles_ = 0;
  std::shared_ptr<arrow::RecordBatch> pending_input_;
  std::shared_ptr<arrow::RecordBatch> buffered_output_;
};

}  // namespace tiforth

