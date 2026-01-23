#pragma once

#include <deque>
#include <memory>
#include <optional>
#include <vector>

#include <arrow/result.h>
#include <arrow/record_batch.h>

#include "tiforth/pipeline/pipeline_context.h"
#include "tiforth/pipeline/op/op.h"
#include "tiforth/task/blocked_resumer.h"
#include "tiforth/task/task_context.h"

namespace tiforth {

class Plan;
class PlanTaskContext;

enum class TaskState {
  kNeedInput,
  kHasOutput,
  kFinished,
  kCancelled,
  kWaiting,
  kWaitForNotify,
  kIOIn,
  kIOOut,
};

class Task {
 public:
  static arrow::Result<std::unique_ptr<Task>> Create();
  static arrow::Result<std::unique_ptr<Task>> Create(
      std::vector<std::unique_ptr<pipeline::PipeOp>> pipe_ops);

  Task(const Task&) = delete;
  Task& operator=(const Task&) = delete;

  ~Task();

  arrow::Status PushInput(std::shared_ptr<arrow::RecordBatch> batch);
  arrow::Status CloseInput();
  arrow::Result<std::shared_ptr<arrow::RecordBatch>> PullOutput();

  arrow::Status SetInputReader(std::shared_ptr<arrow::RecordBatchReader> reader);

  arrow::Result<TaskState> Step();
  arrow::Result<TaskState> ExecuteIO();
  arrow::Result<TaskState> Await();
  arrow::Status Notify();

 private:
  Task();

  class InputSourceOp;
  class OutputSinkOp;
  struct Stage;

  arrow::Status Init(std::vector<std::unique_ptr<pipeline::PipeOp>> pipe_ops);
  arrow::Status InitPlan(const Plan& plan);
  arrow::Status ValidateOrSetSchema(const std::shared_ptr<arrow::Schema>& schema);

  std::shared_ptr<arrow::Schema> schema_;
  std::shared_ptr<arrow::RecordBatchReader> input_reader_;
  std::deque<std::shared_ptr<arrow::RecordBatch>> input_queue_;
  std::deque<std::shared_ptr<arrow::RecordBatch>> output_queue_;
  bool input_closed_ = false;

  std::unique_ptr<PlanTaskContext> plan_task_context_;
  std::vector<std::unique_ptr<Stage>> stages_;
  std::size_t current_stage_index_ = 0;

  pipeline::PipelineContext pipeline_context_;
  task::TaskContext task_context_;

  bool need_input_ = false;
  task::ResumerPtr input_resumer_;
  task::ResumerPtr blocked_resumer_;
  std::optional<task::BlockedKind> blocked_kind_;

  friend class Plan;
};

}  // namespace tiforth
