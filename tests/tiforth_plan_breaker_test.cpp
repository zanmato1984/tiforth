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
#include "tiforth/pipeline/logical_pipeline.h"
#include "tiforth/pipeline/task_groups.h"

#include "test_pipeline_ops.h"
#include "test_task_group_runner.h"

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
  (void)engine;

  auto counter_state = std::make_shared<CounterState>();

  auto schema = arrow::schema({arrow::field("x", arrow::int32())});
  arrow::Int32Builder values;
  ARROW_RETURN_NOT_OK(values.AppendValues({1, 2, 3}));
  std::shared_ptr<arrow::Array> array;
  ARROW_RETURN_NOT_OK(values.Finish(&array));
  auto batch = arrow::RecordBatch::Make(schema, /*num_rows=*/3, {array});

  // Build stage: count rows.
  {
    auto source_op = std::make_unique<test::VectorSourceOp>(
        std::vector<std::shared_ptr<arrow::RecordBatch>>{batch});
    auto sink_op = std::make_unique<CountingSinkOp>(counter_state);

    pipeline::LogicalPipeline::Channel channel;
    channel.source_op = source_op.get();
    channel.pipe_ops = {};

    pipeline::LogicalPipeline logical_pipeline{
        "BreakerBuild",
        std::vector<pipeline::LogicalPipeline::Channel>{std::move(channel)},
        sink_op.get()};

    ARROW_ASSIGN_OR_RAISE(auto task_groups,
                          pipeline::CompileToTaskGroups(pipeline::PipelineContext{}, logical_pipeline,
                                                        /*dop=*/1));
    auto task_ctx = test::MakeTestTaskContext();
    ARROW_RETURN_NOT_OK(test::RunTaskGroupsToCompletion(task_groups, task_ctx));
  }

  // Convergent stage: emit count as a batch.
  std::shared_ptr<arrow::RecordBatch> out;
  {
    auto source_op = std::make_unique<EmitCountSourceOp>(counter_state);

    test::CollectSinkOp::OutputsByThread outputs_by_thread(1);
    auto sink_op = std::make_unique<test::CollectSinkOp>(&outputs_by_thread);

    pipeline::LogicalPipeline::Channel channel;
    channel.source_op = source_op.get();
    channel.pipe_ops = {};

    pipeline::LogicalPipeline logical_pipeline{
        "BreakerConvergent",
        std::vector<pipeline::LogicalPipeline::Channel>{std::move(channel)},
        sink_op.get()};

    ARROW_ASSIGN_OR_RAISE(auto task_groups,
                          pipeline::CompileToTaskGroups(pipeline::PipelineContext{}, logical_pipeline,
                                                        /*dop=*/1));
    auto task_ctx = test::MakeTestTaskContext();
    ARROW_RETURN_NOT_OK(test::RunTaskGroupsToCompletion(task_groups, task_ctx));

    auto outputs = test::FlattenOutputs(std::move(outputs_by_thread));
    if (outputs.size() != 1) {
      return arrow::Status::Invalid("expected exactly 1 output batch");
    }
    out = std::move(outputs[0]);
    if (out == nullptr) {
      return arrow::Status::Invalid("expected non-null output batch");
    }
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

  return arrow::Status::OK();
}

}  // namespace

TEST(TiForthPlanBreakerTest, BuildThenConvergent) {
  auto status = RunBreakerPlanSmoke();
  ASSERT_OK(status);
}

}  // namespace tiforth
