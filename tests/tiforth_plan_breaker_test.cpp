#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/testing/gtest_util.h>

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>

#include "tiforth/engine.h"
#include "tiforth/plan.h"
#include "tiforth/task.h"

namespace tiforth {

namespace {

struct CounterState {
  int64_t num_rows = 0;
};

class CountingSinkOp final : public pipeline::SinkOp {
 public:
  explicit CountingSinkOp(std::shared_ptr<CounterState> state) : state_(std::move(state)) {}

  pipeline::PipelineSink Sink(const pipeline::PipelineContext&) override {
    return [this](const pipeline::PipelineContext&, const task::TaskContext&, pipeline::ThreadId,
                  std::optional<pipeline::Batch> input) -> pipeline::OpResult {
      if (state_ == nullptr) {
        return arrow::Status::Invalid("state must not be null");
      }
      if (!input.has_value()) {
        return pipeline::OpOutput::PipeSinkNeedsMore();
      }
      auto batch = std::move(*input);
      if (batch == nullptr) {
        return pipeline::OpOutput::PipeSinkNeedsMore();
      }
      state_->num_rows += batch->num_rows();
      return pipeline::OpOutput::PipeSinkNeedsMore();
    };
  }

 private:
  std::shared_ptr<CounterState> state_;
};

class EmitCountSourceOp final : public pipeline::SourceOp {
 public:
  explicit EmitCountSourceOp(std::shared_ptr<CounterState> state) : state_(std::move(state)) {}

  pipeline::PipelineSource Source(const pipeline::PipelineContext&) override {
    return [this](const pipeline::PipelineContext&, const task::TaskContext&,
                  pipeline::ThreadId) -> pipeline::OpResult {
      if (state_ == nullptr) {
        return arrow::Status::Invalid("state must not be null");
      }
      if (emitted_) {
        return pipeline::OpOutput::Finished();
      }

      auto schema = arrow::schema({arrow::field("rows", arrow::int64())});
      arrow::Int64Builder builder;
      ARROW_RETURN_NOT_OK(builder.Append(state_->num_rows));
      std::shared_ptr<arrow::Array> array;
      ARROW_RETURN_NOT_OK(builder.Finish(&array));
      auto batch = arrow::RecordBatch::Make(schema, /*num_rows=*/1, {array});
      emitted_ = true;
      return pipeline::OpOutput::Finished(std::move(batch));
    };
  }

 private:
  std::shared_ptr<CounterState> state_;
  bool emitted_ = false;
};

arrow::Status RunBreakerPlanSmoke() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, PlanBuilder::Create(engine.get()));

  ARROW_ASSIGN_OR_RAISE(const auto counter_id,
                        builder->AddBreakerState<CounterState>([]() -> arrow::Result<std::shared_ptr<CounterState>> {
                          return std::make_shared<CounterState>();
                        }));

  ARROW_ASSIGN_OR_RAISE(const auto build_stage, builder->AddStage());
  ARROW_RETURN_NOT_OK(builder->SetStageSink(
      build_stage, [counter_id](PlanTaskContext* ctx) -> arrow::Result<std::unique_ptr<pipeline::SinkOp>> {
        ARROW_ASSIGN_OR_RAISE(auto state, ctx->GetBreakerState<CounterState>(counter_id));
        return std::make_unique<CountingSinkOp>(std::move(state));
      }));

  ARROW_ASSIGN_OR_RAISE(const auto convergent_stage, builder->AddStage());
  ARROW_RETURN_NOT_OK(builder->SetStageSource(
      convergent_stage, [counter_id](PlanTaskContext* ctx) -> arrow::Result<std::unique_ptr<pipeline::SourceOp>> {
        ARROW_ASSIGN_OR_RAISE(auto state, ctx->GetBreakerState<CounterState>(counter_id));
        return std::make_unique<EmitCountSourceOp>(std::move(state));
      }));

  ARROW_RETURN_NOT_OK(builder->AddDependency(build_stage, convergent_stage));

  ARROW_ASSIGN_OR_RAISE(auto plan, builder->Finalize());
  ARROW_ASSIGN_OR_RAISE(auto task, plan->CreateTask());

  ARROW_ASSIGN_OR_RAISE(const auto initial_state, task->Step());
  if (initial_state != TaskState::kNeedInput) {
    return arrow::Status::Invalid("expected TaskState::kNeedInput");
  }

  auto schema = arrow::schema({arrow::field("x", arrow::int32())});
  arrow::Int32Builder values;
  ARROW_RETURN_NOT_OK(values.AppendValues({1, 2, 3}));
  std::shared_ptr<arrow::Array> array;
  ARROW_RETURN_NOT_OK(values.Finish(&array));
  auto batch = arrow::RecordBatch::Make(schema, /*num_rows=*/3, {array});

  ARROW_RETURN_NOT_OK(task->PushInput(batch));

  ARROW_ASSIGN_OR_RAISE(const auto after_push_state, task->Step());
  if (after_push_state != TaskState::kNeedInput) {
    return arrow::Status::Invalid("expected TaskState::kNeedInput after build stage consumes input");
  }

  ARROW_RETURN_NOT_OK(task->CloseInput());

  TaskState state = TaskState::kNeedInput;
  while (state == TaskState::kNeedInput) {
    ARROW_ASSIGN_OR_RAISE(state, task->Step());
  }
  if (state != TaskState::kHasOutput) {
    return arrow::Status::Invalid("expected TaskState::kHasOutput after build finishes");
  }

  ARROW_ASSIGN_OR_RAISE(auto out, task->PullOutput());
  if (out == nullptr) {
    return arrow::Status::Invalid("expected non-null output batch");
  }
  if (out->num_columns() != 1 || out->num_rows() != 1) {
    return arrow::Status::Invalid("unexpected output shape");
  }
  if (out->schema()->field(0)->name() != "rows") {
    return arrow::Status::Invalid("unexpected output schema");
  }
  auto rows_array = std::dynamic_pointer_cast<arrow::Int64Array>(out->column(0));
  if (rows_array == nullptr) {
    return arrow::Status::Invalid("unexpected output column type");
  }
  if (rows_array->IsNull(0) || rows_array->Value(0) != 3) {
    return arrow::Status::Invalid("unexpected output value");
  }

  ARROW_ASSIGN_OR_RAISE(const auto final_state, task->Step());
  if (final_state != TaskState::kFinished) {
    return arrow::Status::Invalid("expected TaskState::kFinished after draining output");
  }

  return arrow::Status::OK();
}

}  // namespace

TEST(TiForthPlanBreakerTest, BuildThenConvergent) {
  auto status = RunBreakerPlanSmoke();
  ASSERT_OK(status);
}

}  // namespace tiforth
