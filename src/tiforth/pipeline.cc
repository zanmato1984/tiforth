#include "tiforth/pipeline.h"

#include <utility>

#include <arrow/record_batch.h>
#include <arrow/status.h>

namespace tiforth {

namespace {

class TaskRecordBatchReader final : public arrow::RecordBatchReader {
 public:
  TaskRecordBatchReader(std::shared_ptr<arrow::Schema> schema, std::shared_ptr<Task> task,
                        std::shared_ptr<arrow::RecordBatchReader> input)
      : schema_(std::move(schema)), task_(std::move(task)), input_(std::move(input)) {}

  std::shared_ptr<arrow::Schema> schema() const override { return schema_; }

  arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* batch) override {
    if (batch == nullptr) {
      return arrow::Status::Invalid("batch output must not be null");
    }

    while (true) {
      ARROW_ASSIGN_OR_RAISE(auto state, task_->Step());
      switch (state) {
        case TaskState::kHasOutput: {
          ARROW_ASSIGN_OR_RAISE(*batch, task_->PullOutput());
          return arrow::Status::OK();
        }
        case TaskState::kFinished:
          *batch = nullptr;
          return arrow::Status::OK();
        case TaskState::kNeedInput:
          // With an input reader configured, the task generally pulls input itself. Still, some
          // blocked-state transitions (IO/await) may temporarily report kNeedInput, so keep driving.
          break;
        case TaskState::kCancelled:
          return arrow::Status::Cancelled("task is cancelled");
        case TaskState::kIOIn:
        case TaskState::kIOOut: {
          ARROW_ASSIGN_OR_RAISE(state, task_->ExecuteIO());
          if (state == TaskState::kHasOutput) {
            ARROW_ASSIGN_OR_RAISE(*batch, task_->PullOutput());
            return arrow::Status::OK();
          }
          if (state == TaskState::kFinished) {
            *batch = nullptr;
            return arrow::Status::OK();
          }
          if (state == TaskState::kCancelled) {
            return arrow::Status::Cancelled("task is cancelled");
          }
          if (state == TaskState::kWaitForNotify) {
            return arrow::Status::NotImplemented("task is waiting for notify");
          }
          // kNeedInput / kWaiting / kIO*: continue driving.
          break;
        }
        case TaskState::kWaiting: {
          ARROW_ASSIGN_OR_RAISE(state, task_->Await());
          if (state == TaskState::kHasOutput) {
            ARROW_ASSIGN_OR_RAISE(*batch, task_->PullOutput());
            return arrow::Status::OK();
          }
          if (state == TaskState::kFinished) {
            *batch = nullptr;
            return arrow::Status::OK();
          }
          if (state == TaskState::kCancelled) {
            return arrow::Status::Cancelled("task is cancelled");
          }
          if (state == TaskState::kWaitForNotify) {
            return arrow::Status::NotImplemented("task is waiting for notify");
          }
          // kNeedInput / kWaiting / kIO*: continue driving.
          break;
        }
        case TaskState::kWaitForNotify:
          return arrow::Status::NotImplemented("task is waiting for notify");
      }
    }
  }

  arrow::Status Close() override {
    if (input_ == nullptr) {
      return arrow::Status::OK();
    }
    return input_->Close();
  }

 private:
  std::shared_ptr<arrow::Schema> schema_;
  std::shared_ptr<Task> task_;
  std::shared_ptr<arrow::RecordBatchReader> input_;
};

}  // namespace

arrow::Result<std::unique_ptr<PipelineBuilder>> PipelineBuilder::Create(const Engine* engine) {
  if (engine == nullptr) {
    return arrow::Status::Invalid("engine must not be null");
  }
  return std::unique_ptr<PipelineBuilder>(new PipelineBuilder(engine));
}

PipelineBuilder::PipelineBuilder(const Engine* engine) : engine_(engine) {}

PipelineBuilder::~PipelineBuilder() = default;

arrow::Status PipelineBuilder::AppendPipe(PipeFactory factory) {
  if (!factory) {
    return arrow::Status::Invalid("pipe factory must not be empty");
  }
  pipe_factories_.push_back(std::move(factory));
  return arrow::Status::OK();
}

arrow::Result<std::unique_ptr<Pipeline>> PipelineBuilder::Finalize() {
  return std::unique_ptr<Pipeline>(new Pipeline(engine_, std::move(pipe_factories_)));
}

Pipeline::Pipeline(const Engine* engine, std::vector<PipelineBuilder::PipeFactory> pipe_factories)
    : engine_(engine), pipe_factories_(std::move(pipe_factories)) {}

arrow::Result<std::unique_ptr<Task>> Pipeline::CreateTask() const {
  if (engine_ == nullptr) {
    return arrow::Status::Invalid("engine must not be null");
  }

  std::vector<std::unique_ptr<pipeline::PipeOp>> pipe_ops;
  pipe_ops.reserve(pipe_factories_.size());
  for (const auto& factory : pipe_factories_) {
    if (!factory) {
      return arrow::Status::Invalid("pipe factory must not be empty");
    }
    ARROW_ASSIGN_OR_RAISE(auto op, factory());
    if (op == nullptr) {
      return arrow::Status::Invalid("pipe factory returned null");
    }
    pipe_ops.push_back(std::move(op));
  }

  return Task::Create(std::move(pipe_ops));
}

arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> Pipeline::MakeReader(
    std::shared_ptr<arrow::RecordBatchReader> input) const {
  if (input == nullptr) {
    return arrow::Status::Invalid("input must not be null");
  }
  if (input->schema() == nullptr) {
    return arrow::Status::Invalid("input schema must not be null");
  }

  ARROW_ASSIGN_OR_RAISE(auto task_unique, CreateTask());
  ARROW_RETURN_NOT_OK(task_unique->SetInputReader(input));
  std::shared_ptr<Task> task = std::move(task_unique);

  return std::make_shared<TaskRecordBatchReader>(input->schema(), std::move(task), input);
}

}  // namespace tiforth
