#include "tiforth/pipeline_exec.h"

#include <utility>

#include <arrow/status.h>
#include <arrow/util/logging.h>

namespace tiforth {

PipelineExec::PipelineExec(SourceOpPtr source, TransformOps transforms, SinkOpPtr sink)
    : source_(std::move(source)), transforms_(std::move(transforms)), sink_(std::move(sink)) {}

PipelineExec::~PipelineExec() = default;

arrow::Result<OperatorStatus> PipelineExec::FetchBatch(std::shared_ptr<arrow::RecordBatch>* batch,
                                                      std::size_t* start_transform_op_index) {
  if (batch == nullptr) {
    return arrow::Status::Invalid("batch output must not be null");
  }
  if (start_transform_op_index == nullptr) {
    return arrow::Status::Invalid("start_transform_op_index must not be null");
  }
  if (source_ == nullptr) {
    return arrow::Status::Invalid("source must not be null");
  }
  if (sink_ == nullptr) {
    return arrow::Status::Invalid("sink must not be null");
  }

  ARROW_ASSIGN_OR_RAISE(auto op_status, sink_->Prepare());
  if (op_status != OperatorStatus::kNeedInput) {
    switch (op_status) {
      case OperatorStatus::kIOIn:
      case OperatorStatus::kIOOut:
        ARROW_DCHECK(io_op_ == nullptr);
        io_op_ = sink_.get();
        return op_status;
      case OperatorStatus::kWaiting:
        ARROW_DCHECK(awaitable_ == nullptr);
        awaitable_ = sink_.get();
        return op_status;
      case OperatorStatus::kWaitForNotify:
        ARROW_DCHECK(waiting_for_notify_ == nullptr);
        waiting_for_notify_ = sink_.get();
        return op_status;
      default:
        return op_status;
    }
  }

  for (std::size_t i = transforms_.size(); i > 0; --i) {
    const auto& transform = transforms_[i - 1];
    if (transform == nullptr) {
      return arrow::Status::Invalid("transform must not be null");
    }

    ARROW_ASSIGN_OR_RAISE(op_status, transform->TryOutput(batch));
    if (op_status == OperatorStatus::kHasOutput) {
      // The batch has already passed transforms_[0..i-1], continue from i.
      *start_transform_op_index = i;
      return OperatorStatus::kHasOutput;
    }
    if (op_status != OperatorStatus::kNeedInput) {
      switch (op_status) {
        case OperatorStatus::kIOIn:
        case OperatorStatus::kIOOut:
          ARROW_DCHECK(io_op_ == nullptr);
          io_op_ = transform.get();
          return op_status;
        case OperatorStatus::kWaiting:
          ARROW_DCHECK(awaitable_ == nullptr);
          awaitable_ = transform.get();
          return op_status;
        case OperatorStatus::kWaitForNotify:
          ARROW_DCHECK(waiting_for_notify_ == nullptr);
          waiting_for_notify_ = transform.get();
          return op_status;
        default:
          return op_status;
      }
    }
  }

  *start_transform_op_index = 0;
  ARROW_ASSIGN_OR_RAISE(op_status, source_->Read(batch));
  if (op_status == OperatorStatus::kFinished) {
    batch->reset();
    // Treat end-of-stream as an "output" so transforms/sink can flush/finish.
    return OperatorStatus::kHasOutput;
  }
  switch (op_status) {
    case OperatorStatus::kIOIn:
    case OperatorStatus::kIOOut:
      ARROW_DCHECK(io_op_ == nullptr);
      io_op_ = source_.get();
      return op_status;
    case OperatorStatus::kWaiting:
      ARROW_DCHECK(awaitable_ == nullptr);
      awaitable_ = source_.get();
      return op_status;
    case OperatorStatus::kWaitForNotify:
      ARROW_DCHECK(waiting_for_notify_ == nullptr);
      waiting_for_notify_ = source_.get();
      return op_status;
    default:
      return op_status;
  }
}

arrow::Result<OperatorStatus> PipelineExec::Execute() {
  if (awaitable_ != nullptr || io_op_ != nullptr || waiting_for_notify_ != nullptr) {
    return arrow::Status::Invalid(
        "pipeline exec is blocked; call ExecuteIO/Await/Notify before Execute");
  }

  std::shared_ptr<arrow::RecordBatch> batch;
  std::size_t start_transform_op_index = 0;

  ARROW_ASSIGN_OR_RAISE(auto op_status, FetchBatch(&batch, &start_transform_op_index));
  if (op_status != OperatorStatus::kHasOutput) {
    return op_status;
  }

  for (std::size_t i = start_transform_op_index; i < transforms_.size(); ++i) {
    const auto& transform = transforms_[i];
    if (transform == nullptr) {
      return arrow::Status::Invalid("transform must not be null");
    }

    ARROW_ASSIGN_OR_RAISE(op_status, transform->Transform(&batch));
    if (op_status != OperatorStatus::kHasOutput) {
      switch (op_status) {
        case OperatorStatus::kIOIn:
        case OperatorStatus::kIOOut:
          ARROW_DCHECK(io_op_ == nullptr);
          io_op_ = transform.get();
          return op_status;
        case OperatorStatus::kWaiting:
          ARROW_DCHECK(awaitable_ == nullptr);
          awaitable_ = transform.get();
          return op_status;
        case OperatorStatus::kWaitForNotify:
          ARROW_DCHECK(waiting_for_notify_ == nullptr);
          waiting_for_notify_ = transform.get();
          return op_status;
        default:
          return op_status;
      }
    }
  }

  ARROW_ASSIGN_OR_RAISE(op_status, sink_->Write(std::move(batch)));
  switch (op_status) {
    case OperatorStatus::kIOIn:
    case OperatorStatus::kIOOut:
      ARROW_DCHECK(io_op_ == nullptr);
      io_op_ = sink_.get();
      return op_status;
    case OperatorStatus::kWaiting:
      ARROW_DCHECK(awaitable_ == nullptr);
      awaitable_ = sink_.get();
      return op_status;
    case OperatorStatus::kWaitForNotify:
      ARROW_DCHECK(waiting_for_notify_ == nullptr);
      waiting_for_notify_ = sink_.get();
      return op_status;
    default:
      return op_status;
  }
}

arrow::Result<OperatorStatus> PipelineExec::ExecuteIO() {
  if (io_op_ == nullptr) {
    return arrow::Status::Invalid("pipeline exec has no IO operator");
  }
  if (awaitable_ != nullptr || waiting_for_notify_ != nullptr) {
    return arrow::Status::Invalid("pipeline exec is not in IO state");
  }

  ARROW_ASSIGN_OR_RAISE(const auto op_status, io_op_->ExecuteIO());
  switch (op_status) {
    case OperatorStatus::kIOIn:
    case OperatorStatus::kIOOut:
      return op_status;
    case OperatorStatus::kWaiting:
      ARROW_DCHECK(awaitable_ == nullptr);
      awaitable_ = io_op_;
      io_op_ = nullptr;
      return op_status;
    case OperatorStatus::kWaitForNotify:
      ARROW_DCHECK(waiting_for_notify_ == nullptr);
      waiting_for_notify_ = io_op_;
      io_op_ = nullptr;
      return op_status;
    default:
      io_op_ = nullptr;
      return op_status;
  }
}

arrow::Result<OperatorStatus> PipelineExec::Await() {
  if (awaitable_ == nullptr) {
    return arrow::Status::Invalid("pipeline exec has no awaitable operator");
  }
  if (io_op_ != nullptr || waiting_for_notify_ != nullptr) {
    return arrow::Status::Invalid("pipeline exec is not in await state");
  }

  ARROW_ASSIGN_OR_RAISE(const auto op_status, awaitable_->Await());
  switch (op_status) {
    case OperatorStatus::kWaiting:
      return op_status;
    case OperatorStatus::kIOIn:
    case OperatorStatus::kIOOut:
      ARROW_DCHECK(io_op_ == nullptr);
      io_op_ = awaitable_;
      awaitable_ = nullptr;
      return op_status;
    case OperatorStatus::kWaitForNotify:
      ARROW_DCHECK(waiting_for_notify_ == nullptr);
      waiting_for_notify_ = awaitable_;
      awaitable_ = nullptr;
      return op_status;
    default:
      awaitable_ = nullptr;
      return op_status;
  }
}

arrow::Status PipelineExec::Notify() {
  if (waiting_for_notify_ == nullptr) {
    return arrow::Status::Invalid("pipeline exec has no operator waiting for notify");
  }
  ARROW_RETURN_NOT_OK(waiting_for_notify_->Notify());
  waiting_for_notify_ = nullptr;
  return arrow::Status::OK();
}

void PipelineExecBuilder::SetSourceOp(SourceOpPtr source) { source_op = std::move(source); }

void PipelineExecBuilder::AppendTransformOp(TransformOpPtr transform) {
  transform_ops.push_back(std::move(transform));
}

void PipelineExecBuilder::SetSinkOp(SinkOpPtr sink) { sink_op = std::move(sink); }

arrow::Result<std::unique_ptr<PipelineExec>> PipelineExecBuilder::Build() {
  if (source_op == nullptr) {
    return arrow::Status::Invalid("source must not be null");
  }
  if (sink_op == nullptr) {
    return arrow::Status::Invalid("sink must not be null");
  }
  for (const auto& transform : transform_ops) {
    if (transform == nullptr) {
      return arrow::Status::Invalid("transform must not be null");
    }
  }

  return std::make_unique<PipelineExec>(std::move(source_op), std::move(transform_ops),
                                        std::move(sink_op));
}

}  // namespace tiforth
