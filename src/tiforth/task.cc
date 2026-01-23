#include "tiforth/task.h"

#include <atomic>
#include <utility>

#include <arrow/status.h>
#include <arrow/type.h>

#include "tiforth/pipeline/physical_pipeline.h"
#include "tiforth/pipeline/pipeline_task.h"
#include "tiforth/plan.h"
#include "tiforth/task/awaiter.h"
#include "tiforth/task/blocked_resumer.h"
#include "tiforth/task/task.h"
#include "tiforth/task/task_group.h"
#include "tiforth/task/task_status.h"

namespace tiforth {

namespace {

class SimpleResumer final : public task::Resumer {
 public:
  void Resume() override { resumed_.store(true, std::memory_order_release); }
  bool IsResumed() const override { return resumed_.load(std::memory_order_acquire); }

 private:
  std::atomic_bool resumed_{false};
};

class ResumersAwaiter final : public task::Awaiter {
 public:
  explicit ResumersAwaiter(task::Resumers resumers) : resumers_(std::move(resumers)) {}

  const task::Resumers& resumers() const { return resumers_; }

 private:
  task::Resumers resumers_;
};

}  // namespace

arrow::Result<std::unique_ptr<Task>> Task::Create() {
  return Create(std::vector<std::unique_ptr<pipeline::PipeOp>>{});
}

arrow::Result<std::unique_ptr<Task>> Task::Create(std::vector<std::unique_ptr<pipeline::PipeOp>> pipe_ops) {
  auto task = std::unique_ptr<Task>(new Task());
  ARROW_RETURN_NOT_OK(task->Init(std::move(pipe_ops)));
  return task;
}

Task::Task() = default;

Task::~Task() = default;

class Task::InputSourceOp final : public pipeline::SourceOp {
 public:
  explicit InputSourceOp(Task* task) : task_(task) {}

  pipeline::PipelineSource Source(const pipeline::PipelineContext&) override {
    return [this](const pipeline::PipelineContext&, const task::TaskContext& task_ctx,
                  pipeline::ThreadId) -> pipeline::OpResult {
      if (task_ == nullptr) {
        return arrow::Status::Invalid("task must not be null");
      }

      task_->need_input_ = false;

      if (!task_->input_queue_.empty()) {
        auto batch = std::move(task_->input_queue_.front());
        task_->input_queue_.pop_front();
        if (batch == nullptr) {
          return arrow::Status::Invalid("input batch must not be null");
        }
        return pipeline::OpOutput::SourcePipeHasMore(std::move(batch));
      }

      if (!task_->input_closed_ && task_->input_reader_ != nullptr) {
        std::shared_ptr<arrow::RecordBatch> batch;
        ARROW_RETURN_NOT_OK(task_->input_reader_->ReadNext(&batch));
        if (batch == nullptr) {
          task_->input_closed_ = true;
          return pipeline::OpOutput::Finished();
        }
        ARROW_RETURN_NOT_OK(task_->ValidateOrSetSchema(batch->schema()));
        return pipeline::OpOutput::SourcePipeHasMore(std::move(batch));
      }

      if (task_->input_closed_) {
        return pipeline::OpOutput::Finished();
      }

      if (task_->input_reader_ != nullptr) {
        return arrow::Status::Invalid("task input reader must not be set when input is not closed");
      }
      if (!task_ctx.resumer_factory) {
        return arrow::Status::Invalid("resumer_factory must not be empty");
      }
      ARROW_ASSIGN_OR_RAISE(auto resumer, task_ctx.resumer_factory());
      if (resumer == nullptr) {
        return arrow::Status::Invalid("resumer_factory returned null");
      }
      task_->need_input_ = true;
      task_->input_resumer_ = resumer;
      return pipeline::OpOutput::Blocked(std::move(resumer));
    };
  }

 private:
  Task* task_;
};

class Task::OutputSinkOp final : public pipeline::SinkOp {
 public:
  explicit OutputSinkOp(Task* task) : task_(task) {}

  pipeline::PipelineSink Sink(const pipeline::PipelineContext&) override {
    return [this](const pipeline::PipelineContext&, const task::TaskContext&, pipeline::ThreadId,
                  std::optional<pipeline::Batch> input) -> pipeline::OpResult {
      if (task_ == nullptr) {
        return arrow::Status::Invalid("task must not be null");
      }
      if (!input.has_value()) {
        return pipeline::OpOutput::PipeSinkNeedsMore();
      }
      auto batch = std::move(*input);
      if (batch != nullptr) {
        task_->output_queue_.push_back(std::move(batch));
      }
      return pipeline::OpOutput::PipeSinkNeedsMore();
    };
  }

 private:
  Task* task_;
};

struct Task::Stage {
  std::unique_ptr<pipeline::SourceOp> source_op;
  std::vector<std::unique_ptr<pipeline::PipeOp>> pipe_ops;
  std::unique_ptr<pipeline::SinkOp> sink_op;

  std::unique_ptr<pipeline::LogicalPipeline> logical;
  pipeline::PhysicalPipelines physical;
  std::unique_ptr<pipeline::PipelineTask> pipeline_task;
  std::optional<task::TaskGroup> sink_backend;
  bool backend_done = false;

  arrow::Status Init(const pipeline::PipelineContext& pipeline_ctx) {
    if (source_op == nullptr) {
      return arrow::Status::Invalid("stage source must not be null");
    }
    if (sink_op == nullptr) {
      return arrow::Status::Invalid("stage sink must not be null");
    }

    pipeline::LogicalPipeline::Channel channel;
    channel.source_op = source_op.get();
    channel.pipe_ops.reserve(pipe_ops.size());
    for (auto& op : pipe_ops) {
      if (op == nullptr) {
        return arrow::Status::Invalid("stage pipe op must not be null");
      }
      channel.pipe_ops.push_back(op.get());
    }

    logical = std::make_unique<pipeline::LogicalPipeline>(
        "Stage", std::vector<pipeline::LogicalPipeline::Channel>{std::move(channel)},
        sink_op.get());
    physical = pipeline::CompilePipeline(pipeline_ctx, *logical);
    if (physical.size() != 1) {
      return arrow::Status::NotImplemented(
          "only single physical pipeline is supported in current Task wrapper");
    }
    pipeline_task = std::make_unique<pipeline::PipelineTask>(pipeline_ctx, physical[0], /*dop=*/1);
    sink_backend = sink_op->Backend(pipeline_ctx);
    return arrow::Status::OK();
  }

  arrow::Result<task::TaskStatus> StepOnce(const pipeline::PipelineContext& pipeline_ctx,
                                           const task::TaskContext& task_ctx) {
    if (pipeline_task == nullptr) {
      return arrow::Status::Invalid("stage pipeline task must not be null");
    }
    ARROW_ASSIGN_OR_RAISE(auto status, (*pipeline_task)(pipeline_ctx, task_ctx, /*thread_id=*/0));
    return status;
  }

  arrow::Status RunBackend(const task::TaskContext& task_ctx) {
    if (backend_done) {
      return arrow::Status::OK();
    }
    backend_done = true;

    if (!sink_backend.has_value()) {
      return arrow::Status::OK();
    }

    const auto& group = *sink_backend;
    if (!group.GetTask()) {
      return arrow::Status::Invalid("task group must have a task");
    }
    if (group.NumTasks() == 0) {
      return arrow::Status::Invalid("task group num_tasks must be positive");
    }

    for (task::TaskId task_id = 0; task_id < group.NumTasks(); ++task_id) {
      while (true) {
        ARROW_ASSIGN_OR_RAISE(auto st, group.GetTask()(task_ctx, task_id));
        if (st.IsFinished()) {
          break;
        }
        if (st.IsContinue()) {
          continue;
        }
        if (st.IsCancelled()) {
          return arrow::Status::Cancelled("backend task cancelled");
        }
        if (st.IsBlocked()) {
          return arrow::Status::Invalid("backend task returned Blocked");
        }
        if (st.IsYield()) {
          return arrow::Status::Invalid("backend task returned Yield");
        }
      }
    }

    if (group.GetContinuation().has_value()) {
      const auto& cont = *group.GetContinuation();
      while (true) {
        ARROW_ASSIGN_OR_RAISE(auto st, cont(task_ctx));
        if (st.IsFinished()) {
          break;
        }
        if (st.IsContinue()) {
          continue;
        }
        if (st.IsCancelled()) {
          return arrow::Status::Cancelled("backend continuation cancelled");
        }
        if (st.IsBlocked()) {
          return arrow::Status::Invalid("backend continuation returned Blocked");
        }
        if (st.IsYield()) {
          return arrow::Status::Invalid("backend continuation returned Yield");
        }
      }
    }

    return arrow::Status::OK();
  }
};

arrow::Status Task::Init(std::vector<std::unique_ptr<pipeline::PipeOp>> pipe_ops) {
  pipeline_context_ = pipeline::PipelineContext{};

  task_context_ = task::TaskContext{};
  task_context_.query_ctx = pipeline_context_.query_ctx;
  task_context_.resumer_factory = []() -> arrow::Result<task::ResumerPtr> {
    return std::make_shared<SimpleResumer>();
  };
  task_context_.single_awaiter_factory =
      [](task::ResumerPtr resumer) -> arrow::Result<task::AwaiterPtr> {
    task::Resumers resumers;
    resumers.push_back(std::move(resumer));
    return std::make_shared<ResumersAwaiter>(std::move(resumers));
  };
  task_context_.any_awaiter_factory =
      [](task::Resumers resumers) -> arrow::Result<task::AwaiterPtr> {
    return std::make_shared<ResumersAwaiter>(std::move(resumers));
  };
  task_context_.all_awaiter_factory =
      [](task::Resumers resumers) -> arrow::Result<task::AwaiterPtr> {
    return std::make_shared<ResumersAwaiter>(std::move(resumers));
  };

  need_input_ = false;
  input_resumer_.reset();
  blocked_resumer_.reset();
  blocked_kind_.reset();

  stages_.clear();
  current_stage_index_ = 0;

  auto stage = std::make_unique<Stage>();
  stage->source_op = std::make_unique<InputSourceOp>(this);
  stage->sink_op = std::make_unique<OutputSinkOp>(this);
  stage->pipe_ops = std::move(pipe_ops);
  ARROW_RETURN_NOT_OK(stage->Init(pipeline_context_));
  stages_.push_back(std::move(stage));

  return arrow::Status::OK();
}

arrow::Status Task::InitPlan(const Plan& plan) {
  if (plan.engine_ == nullptr) {
    return arrow::Status::Invalid("plan engine must not be null");
  }

  pipeline_context_ = pipeline::PipelineContext{};

  task_context_ = task::TaskContext{};
  task_context_.query_ctx = pipeline_context_.query_ctx;
  task_context_.resumer_factory = []() -> arrow::Result<task::ResumerPtr> {
    return std::make_shared<SimpleResumer>();
  };
  task_context_.single_awaiter_factory =
      [](task::ResumerPtr resumer) -> arrow::Result<task::AwaiterPtr> {
    task::Resumers resumers;
    resumers.push_back(std::move(resumer));
    return std::make_shared<ResumersAwaiter>(std::move(resumers));
  };
  task_context_.any_awaiter_factory =
      [](task::Resumers resumers) -> arrow::Result<task::AwaiterPtr> {
    return std::make_shared<ResumersAwaiter>(std::move(resumers));
  };
  task_context_.all_awaiter_factory =
      [](task::Resumers resumers) -> arrow::Result<task::AwaiterPtr> {
    return std::make_shared<ResumersAwaiter>(std::move(resumers));
  };

  need_input_ = false;
  input_resumer_.reset();
  blocked_resumer_.reset();
  blocked_kind_.reset();

  plan_task_context_ = std::make_unique<PlanTaskContext>();

  plan_task_context_->breaker_states_.clear();
  plan_task_context_->breaker_states_.reserve(plan.breaker_state_factories_.size());
  for (const auto& factory : plan.breaker_state_factories_) {
    if (!factory) {
      return arrow::Status::Invalid("breaker state factory must not be empty");
    }
    ARROW_ASSIGN_OR_RAISE(auto state, factory());
    if (state == nullptr) {
      return arrow::Status::Invalid("breaker state factory returned null");
    }
    plan_task_context_->breaker_states_.push_back(std::move(state));
  }

  stages_.clear();
  stages_.reserve(plan.stage_order_.size());
  current_stage_index_ = 0;

  for (const auto stage_id : plan.stage_order_) {
    if (stage_id >= plan.stages_.size()) {
      return arrow::Status::Invalid("plan stage id out of range");
    }
    const auto& stage = plan.stages_[stage_id];

    auto stage_exec = std::make_unique<Stage>();

    switch (stage.source_kind) {
      case PlanStageSourceKind::kTaskInput:
        stage_exec->source_op = std::make_unique<InputSourceOp>(this);
        break;
      case PlanStageSourceKind::kCustom: {
        if (!stage.pipeline_source_factory) {
          return arrow::Status::Invalid("custom stage source factory must not be empty");
        }
        ARROW_ASSIGN_OR_RAISE(auto source, stage.pipeline_source_factory(plan_task_context_.get()));
        if (source == nullptr) {
          return arrow::Status::Invalid("custom stage source factory returned null");
        }
        stage_exec->source_op = std::move(source);
        break;
      }
    }

    stage_exec->pipe_ops.reserve(stage.pipe_factories.size());
    for (const auto& pipe_factory : stage.pipe_factories) {
      if (!pipe_factory) {
        return arrow::Status::Invalid("pipe factory must not be empty");
      }
      ARROW_ASSIGN_OR_RAISE(auto pipe, pipe_factory(plan_task_context_.get()));
      if (pipe == nullptr) {
        return arrow::Status::Invalid("pipe factory returned null");
      }
      stage_exec->pipe_ops.push_back(std::move(pipe));
    }

    switch (stage.sink_kind) {
      case PlanStageSinkKind::kTaskOutput:
        stage_exec->sink_op = std::make_unique<OutputSinkOp>(this);
        break;
      case PlanStageSinkKind::kCustom: {
        if (!stage.pipeline_sink_factory) {
          return arrow::Status::Invalid("custom stage sink factory must not be empty");
        }
        ARROW_ASSIGN_OR_RAISE(auto sink, stage.pipeline_sink_factory(plan_task_context_.get()));
        if (sink == nullptr) {
          return arrow::Status::Invalid("custom stage sink factory returned null");
        }
        stage_exec->sink_op = std::move(sink);
        break;
      }
    }

    ARROW_RETURN_NOT_OK(stage_exec->Init(pipeline_context_));
    stages_.push_back(std::move(stage_exec));
  }

  return arrow::Status::OK();
}

arrow::Status Task::ValidateOrSetSchema(const std::shared_ptr<arrow::Schema>& schema) {
  if (schema == nullptr) {
    return arrow::Status::Invalid("schema must not be null");
  }
  if (schema_ == nullptr) {
    schema_ = schema;
    return arrow::Status::OK();
  }
  if (!schema_->Equals(*schema, /*check_metadata=*/true)) {
    return arrow::Status::Invalid("schema mismatch");
  }
  return arrow::Status::OK();
}

arrow::Status Task::PushInput(std::shared_ptr<arrow::RecordBatch> batch) {
  if (batch == nullptr) {
    return arrow::Status::Invalid("batch must not be null");
  }
  if (input_reader_ != nullptr) {
    return arrow::Status::Invalid("PushInput cannot be used when an input reader is configured");
  }
  if (input_closed_) {
    return arrow::Status::Invalid("PushInput cannot be used after CloseInput");
  }
  ARROW_RETURN_NOT_OK(ValidateOrSetSchema(batch->schema()));
  input_queue_.push_back(std::move(batch));
  need_input_ = false;
  if (input_resumer_ != nullptr) {
    input_resumer_->Resume();
    input_resumer_.reset();
  }
  return arrow::Status::OK();
}

arrow::Status Task::CloseInput() {
  if (input_reader_ != nullptr) {
    return arrow::Status::Invalid("CloseInput cannot be used when an input reader is configured");
  }
  input_closed_ = true;
  need_input_ = false;
  if (input_resumer_ != nullptr) {
    input_resumer_->Resume();
    input_resumer_.reset();
  }
  return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> Task::PullOutput() {
  if (!output_queue_.empty()) {
    auto batch = std::move(output_queue_.front());
    output_queue_.pop_front();
    return batch;
  }
  if (current_stage_index_ >= stages_.size()) {
    return std::shared_ptr<arrow::RecordBatch>();
  }
  return arrow::Status::Invalid("no output is available");
}

arrow::Status Task::SetInputReader(std::shared_ptr<arrow::RecordBatchReader> reader) {
  if (reader == nullptr) {
    return arrow::Status::Invalid("input reader must not be null");
  }
  if (!input_queue_.empty()) {
    return arrow::Status::Invalid("input reader cannot be set after PushInput");
  }
  if (input_closed_) {
    return arrow::Status::Invalid("input reader cannot be set after CloseInput");
  }
  if (input_reader_ != nullptr) {
    return arrow::Status::Invalid("input reader is already set");
  }
  ARROW_RETURN_NOT_OK(ValidateOrSetSchema(reader->schema()));
  input_reader_ = std::move(reader);
  return arrow::Status::OK();
}

arrow::Result<TaskState> Task::Step() {
  if (!output_queue_.empty()) {
    return TaskState::kHasOutput;
  }

  if (blocked_resumer_ != nullptr && blocked_resumer_->IsResumed()) {
    blocked_resumer_.reset();
    blocked_kind_.reset();
  }

  if (blocked_resumer_ != nullptr && blocked_kind_.has_value()) {
    switch (*blocked_kind_) {
      case task::BlockedKind::kIOIn:
        return TaskState::kIOIn;
      case task::BlockedKind::kIOOut:
        return TaskState::kIOOut;
      case task::BlockedKind::kWaitForNotify:
        return TaskState::kWaitForNotify;
      case task::BlockedKind::kWaiting:
        return TaskState::kWaiting;
    }
    return arrow::Status::Invalid("unknown blocked kind");
  }

  if (current_stage_index_ >= stages_.size()) {
    return TaskState::kFinished;
  }

  while (true) {
    if (current_stage_index_ >= stages_.size()) {
      return TaskState::kFinished;
    }
    if (stages_.empty()) {
      return arrow::Status::Invalid("task is not initialized");
    }
    auto& stage = *stages_[current_stage_index_];
    ARROW_ASSIGN_OR_RAISE(auto st, stage.StepOnce(pipeline_context_, task_context_));

    if (!output_queue_.empty()) {
      return TaskState::kHasOutput;
    }

    if (st.IsFinished()) {
      ARROW_RETURN_NOT_OK(stage.RunBackend(task_context_));
      ++current_stage_index_;
      continue;
    }
    if (st.IsCancelled()) {
      return TaskState::kCancelled;
    }
    if (st.IsBlocked()) {
      if (need_input_) {
        return TaskState::kNeedInput;
      }

      blocked_resumer_.reset();
      blocked_kind_.reset();

      const auto& awaiter = st.GetAwaiter();
      const auto* resumers_awaiter = dynamic_cast<const ResumersAwaiter*>(awaiter.get());
      if (resumers_awaiter == nullptr) {
        return TaskState::kWaiting;
      }

      task::ResumerPtr selected;
      for (const auto& resumer : resumers_awaiter->resumers()) {
        if (resumer == nullptr) {
          continue;
        }
        if (selected != nullptr) {
          return arrow::Status::NotImplemented(
              "Task wrapper supports at most one blocked resumer");
        }
        selected = resumer;
      }
      if (selected == nullptr) {
        return TaskState::kWaiting;
      }

      blocked_resumer_ = selected;
      auto blocked = std::dynamic_pointer_cast<task::BlockedResumer>(selected);
      if (blocked == nullptr) {
        return TaskState::kWaiting;
      }
      blocked_kind_ = blocked->kind();
      switch (*blocked_kind_) {
        case task::BlockedKind::kIOIn:
          return TaskState::kIOIn;
        case task::BlockedKind::kIOOut:
          return TaskState::kIOOut;
        case task::BlockedKind::kWaitForNotify:
          return TaskState::kWaitForNotify;
        case task::BlockedKind::kWaiting:
          return TaskState::kWaiting;
      }
      return arrow::Status::Invalid("unknown blocked kind");
    }
    if (st.IsYield()) {
      return TaskState::kWaiting;
    }
    ARROW_CHECK(st.IsContinue());
  }
}

arrow::Result<TaskState> Task::ExecuteIO() {
  if (blocked_resumer_ == nullptr) {
    return arrow::Status::Invalid("task is not blocked");
  }

  auto blocked = std::dynamic_pointer_cast<task::BlockedResumer>(blocked_resumer_);
  if (blocked == nullptr) {
    return arrow::Status::Invalid("blocked resumer does not support ExecuteIO");
  }
  if (blocked->kind() != task::BlockedKind::kIOIn && blocked->kind() != task::BlockedKind::kIOOut) {
    return arrow::Status::Invalid("task is not in IO state");
  }

  ARROW_ASSIGN_OR_RAISE(auto next_kind, blocked->ExecuteIO());
  if (next_kind.has_value()) {
    blocked_kind_ = *next_kind;
    switch (*next_kind) {
      case task::BlockedKind::kIOIn:
        return TaskState::kIOIn;
      case task::BlockedKind::kIOOut:
        return TaskState::kIOOut;
      case task::BlockedKind::kWaitForNotify:
        return TaskState::kWaitForNotify;
      case task::BlockedKind::kWaiting:
        return TaskState::kWaiting;
    }
    return arrow::Status::Invalid("unknown blocked kind");
  }

  blocked_resumer_->Resume();
  blocked_resumer_.reset();
  blocked_kind_.reset();
  return TaskState::kNeedInput;
}

arrow::Result<TaskState> Task::Await() {
  if (blocked_resumer_ == nullptr) {
    return arrow::Status::Invalid("task is not blocked");
  }

  if (blocked_resumer_->IsResumed()) {
    blocked_resumer_.reset();
    blocked_kind_.reset();
    return TaskState::kNeedInput;
  }

  auto blocked = std::dynamic_pointer_cast<task::BlockedResumer>(blocked_resumer_);
  if (blocked == nullptr) {
    return TaskState::kWaiting;
  }
  if (blocked->kind() != task::BlockedKind::kWaiting) {
    return arrow::Status::Invalid("task is not in await state");
  }

  ARROW_ASSIGN_OR_RAISE(auto next_kind, blocked->Await());
  if (next_kind.has_value()) {
    blocked_kind_ = *next_kind;
    switch (*next_kind) {
      case task::BlockedKind::kIOIn:
        return TaskState::kIOIn;
      case task::BlockedKind::kIOOut:
        return TaskState::kIOOut;
      case task::BlockedKind::kWaitForNotify:
        return TaskState::kWaitForNotify;
      case task::BlockedKind::kWaiting:
        return TaskState::kWaiting;
    }
    return arrow::Status::Invalid("unknown blocked kind");
  }

  blocked_resumer_->Resume();
  blocked_resumer_.reset();
  blocked_kind_.reset();
  return TaskState::kNeedInput;
}

arrow::Status Task::Notify() {
  if (blocked_resumer_ == nullptr) {
    return arrow::Status::Invalid("task is not blocked");
  }

  auto blocked = std::dynamic_pointer_cast<task::BlockedResumer>(blocked_resumer_);
  if (blocked == nullptr) {
    return arrow::Status::Invalid("blocked resumer does not support Notify");
  }
  if (blocked->kind() != task::BlockedKind::kWaitForNotify) {
    return arrow::Status::Invalid("task is not waiting for notify");
  }

  ARROW_RETURN_NOT_OK(blocked->Notify());
  blocked_resumer_->Resume();
  blocked_resumer_.reset();
  blocked_kind_.reset();
  return arrow::Status::OK();
}

}  // namespace tiforth
