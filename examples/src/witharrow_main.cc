#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/compute/api_scalar.h>
#include <arrow/compute/initialize.h>
#include <arrow/result.h>
#include <arrow/status.h>

#include <cstdint>
#include <iostream>
#include <optional>
#include <vector>

#include "tiforth/tiforth.h"
#include "task_group_runner.h"

namespace {

arrow::Result<std::shared_ptr<arrow::Array>> MakeInt32Array(
    const std::vector<int32_t>& values);
arrow::Result<std::shared_ptr<arrow::RecordBatch>> MakeXYBatch(const std::vector<int32_t>& xs,
                                                               const std::vector<int32_t>& ys);

class VectorSourceOp final : public tiforth::SourceOp {
 public:
  explicit VectorSourceOp(std::vector<std::shared_ptr<arrow::RecordBatch>> batches)
      : batches_(std::move(batches)) {}

  tiforth::PipelineSource Source() override {
    return [this](const tiforth::TaskContext&, tiforth::ThreadId thread_id) -> tiforth::OpResult {
      if (thread_id != 0) {
        return arrow::Status::Invalid("VectorSourceOp only supports thread_id=0");
      }
      if (next_ >= batches_.size()) {
        return tiforth::OpOutput::Finished();
      }
      auto batch = batches_[next_++];
      if (batch == nullptr) {
        return arrow::Status::Invalid("source batch must not be null");
      }
      return tiforth::OpOutput::SourcePipeHasMore(std::move(batch));
    };
  }

  tiforth::TaskGroups Frontend() override { return {}; }
  std::optional<tiforth::TaskGroup> Backend() override { return std::nullopt; }

 private:
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches_;
  std::size_t next_ = 0;
};

class CollectSinkOp final : public tiforth::SinkOp {
 public:
  explicit CollectSinkOp(std::vector<std::shared_ptr<arrow::RecordBatch>>* outputs)
      : outputs_(outputs) {}

  tiforth::PipelineSink Sink() override {
    return [this](const tiforth::TaskContext&, tiforth::ThreadId thread_id,
                  std::optional<tiforth::Batch> input) -> tiforth::OpResult {
      if (thread_id != 0) {
        return arrow::Status::Invalid("CollectSinkOp only supports thread_id=0");
      }
      if (outputs_ == nullptr) {
        return arrow::Status::Invalid("outputs must not be null");
      }
      if (!input.has_value()) {
        return tiforth::OpOutput::PipeSinkNeedsMore();
      }
      auto batch = std::move(*input);
      if (batch != nullptr) {
        outputs_->push_back(std::move(batch));
      }
      return tiforth::OpOutput::PipeSinkNeedsMore();
    };
  }

  tiforth::TaskGroups Frontend() override { return {}; }
  std::optional<tiforth::TaskGroup> Backend() override { return std::nullopt; }
  std::unique_ptr<tiforth::SourceOp> ImplicitSource() override { return nullptr; }

 private:
  std::vector<std::shared_ptr<arrow::RecordBatch>>* outputs_ = nullptr;
};

arrow::Status RunTiForthSmoke() {
  ARROW_ASSIGN_OR_RAISE(auto engine, tiforth::Engine::Create(tiforth::EngineOptions{}));

  auto schema = arrow::schema({arrow::field("x", arrow::int32())});
  ARROW_ASSIGN_OR_RAISE(auto array, MakeInt32Array({1, 2, 3}));
  auto batch = arrow::RecordBatch::Make(schema, /*num_rows=*/3, {array});

  auto source_op = std::make_unique<VectorSourceOp>(
      std::vector<std::shared_ptr<arrow::RecordBatch>>{batch});
  std::vector<std::shared_ptr<arrow::RecordBatch>> outputs;
  auto sink_op = std::make_unique<CollectSinkOp>(&outputs);

  tiforth::LogicalPipeline::Channel channel;
  channel.source_op = source_op.get();
  channel.pipe_ops = {};

  tiforth::LogicalPipeline logical_pipeline{
      "Smoke",
      std::vector<tiforth::LogicalPipeline::Channel>{std::move(channel)},
      sink_op.get()};

  ARROW_ASSIGN_OR_RAISE(auto task_groups,
                        tiforth_example::CompileToTaskGroups(logical_pipeline, /*dop=*/1));

  auto task_ctx = tiforth_example::MakeTaskContext();
  ARROW_RETURN_NOT_OK(tiforth_example::RunTaskGroupsToCompletion(task_groups, task_ctx));

  if (outputs.size() != 1) {
    return arrow::Status::Invalid("expected exactly 1 output batch");
  }
  if (outputs[0].get() != batch.get()) {
    return arrow::Status::Invalid("expected pass-through output batch");
  }
  return arrow::Status::OK();
}

arrow::Status RunTiForthProjectionSmoke() {
  ARROW_ASSIGN_OR_RAISE(auto engine, tiforth::Engine::Create(tiforth::EngineOptions{}));

  std::vector<tiforth::op::ProjectionExpr> exprs;
  exprs.push_back({"x", tiforth::MakeFieldRef("x")});
  exprs.push_back({"x_plus_y",
                   tiforth::MakeCall("add", {tiforth::MakeFieldRef("x"), tiforth::MakeFieldRef("y")})});
  exprs.push_back(
      {"x_plus_10",
       tiforth::MakeCall("add",
                         {tiforth::MakeFieldRef("x"),
                          tiforth::MakeLiteral(std::make_shared<arrow::Int32Scalar>(10))})});

  ARROW_ASSIGN_OR_RAISE(auto batch0, MakeXYBatch({1, 2, 3}, {10, 20, 30}));
  ARROW_ASSIGN_OR_RAISE(auto batch1, MakeXYBatch({4}, {100}));

  auto source_op = std::make_unique<VectorSourceOp>(
      std::vector<std::shared_ptr<arrow::RecordBatch>>{batch0, batch1});

  std::vector<std::unique_ptr<tiforth::PipeOp>> pipe_ops;
  pipe_ops.push_back(std::make_unique<tiforth::op::ProjectionPipeOp>(engine.get(), exprs));

  std::vector<std::shared_ptr<arrow::RecordBatch>> outputs;
  auto sink_op = std::make_unique<CollectSinkOp>(&outputs);

  tiforth::LogicalPipeline::Channel channel;
  channel.source_op = source_op.get();
  channel.pipe_ops.reserve(pipe_ops.size());
  for (auto& op : pipe_ops) {
    channel.pipe_ops.push_back(op.get());
  }

  tiforth::LogicalPipeline logical_pipeline{
      "Projection",
      std::vector<tiforth::LogicalPipeline::Channel>{std::move(channel)},
      sink_op.get()};

  ARROW_ASSIGN_OR_RAISE(auto task_groups,
                        tiforth_example::CompileToTaskGroups(logical_pipeline, /*dop=*/1));

  auto task_ctx = tiforth_example::MakeTaskContext();
  ARROW_RETURN_NOT_OK(tiforth_example::RunTaskGroupsToCompletion(task_groups, task_ctx));

  std::size_t seen_batches = 0;
  for (const auto& out : outputs) {
    if (out == nullptr) {
      return arrow::Status::Invalid("expected non-null output batch");
    }

    if (out->num_columns() != 3) {
      return arrow::Status::Invalid("unexpected projection output column count");
    }
    if (out->schema()->field(0)->name() != "x" || out->schema()->field(1)->name() != "x_plus_y" ||
        out->schema()->field(2)->name() != "x_plus_10") {
      return arrow::Status::Invalid("unexpected projection output schema");
    }

    if (seen_batches == 0) {
      ARROW_ASSIGN_OR_RAISE(auto expect_x, MakeInt32Array({1, 2, 3}));
      ARROW_ASSIGN_OR_RAISE(auto expect_x_plus_y, MakeInt32Array({11, 22, 33}));
      ARROW_ASSIGN_OR_RAISE(auto expect_x_plus_10, MakeInt32Array({11, 12, 13}));
      if (!expect_x->Equals(*out->column(0)) || !expect_x_plus_y->Equals(*out->column(1)) ||
          !expect_x_plus_10->Equals(*out->column(2))) {
        return arrow::Status::Invalid("unexpected projection output values (batch0)");
      }
    } else if (seen_batches == 1) {
      ARROW_ASSIGN_OR_RAISE(auto expect_x, MakeInt32Array({4}));
      ARROW_ASSIGN_OR_RAISE(auto expect_x_plus_y, MakeInt32Array({104}));
      ARROW_ASSIGN_OR_RAISE(auto expect_x_plus_10, MakeInt32Array({14}));
      if (!expect_x->Equals(*out->column(0)) || !expect_x_plus_y->Equals(*out->column(1)) ||
          !expect_x_plus_10->Equals(*out->column(2))) {
        return arrow::Status::Invalid("unexpected projection output values (batch1)");
      }
    } else {
      return arrow::Status::Invalid("unexpected extra output batch");
    }

    ++seen_batches;
  }

  if (seen_batches != 2) {
    return arrow::Status::Invalid("expected exactly 2 output batches");
  }

  return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::Array>> MakeInt32Array(
    const std::vector<int32_t>& values) {
  arrow::Int32Builder builder;
  for (const auto value : values) {
    ARROW_RETURN_NOT_OK(builder.Append(value));
  }

  std::shared_ptr<arrow::Array> array;
  ARROW_RETURN_NOT_OK(builder.Finish(&array));
  return array;
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> MakeXYBatch(const std::vector<int32_t>& xs,
                                                               const std::vector<int32_t>& ys) {
  if (xs.size() != ys.size()) {
    return arrow::Status::Invalid("xs and ys must have the same length");
  }

  auto schema =
      arrow::schema({arrow::field("x", arrow::int32()), arrow::field("y", arrow::int32())});
  ARROW_ASSIGN_OR_RAISE(auto x_array, MakeInt32Array(xs));
  ARROW_ASSIGN_OR_RAISE(auto y_array, MakeInt32Array(ys));
  return arrow::RecordBatch::Make(schema, static_cast<int64_t>(xs.size()), {x_array, y_array});
}

arrow::Status RunArrowComputeSmoke() {
  ARROW_RETURN_NOT_OK(arrow::compute::Initialize());

  ARROW_ASSIGN_OR_RAISE(auto lhs, MakeInt32Array({1, 2, 3}));
  ARROW_ASSIGN_OR_RAISE(auto rhs, MakeInt32Array({10, 20, 30}));

  ARROW_ASSIGN_OR_RAISE(auto sum,
                        arrow::compute::Add(arrow::Datum(lhs), arrow::Datum(rhs)));

  if (!sum.is_array()) {
    return arrow::Status::Invalid("expected array result");
  }

  auto sum_array = sum.make_array();
  if (sum_array == nullptr) {
    return arrow::Status::Invalid("expected non-null result array");
  }
  if (sum_array->type_id() != arrow::Type::INT32) {
    return arrow::Status::Invalid("expected int32 result array");
  }

  auto sum_i32 = std::static_pointer_cast<arrow::Int32Array>(sum_array);
  if (sum_i32->length() != 3) {
    return arrow::Status::Invalid("expected length=3 result array");
  }
  if (sum_i32->Value(0) != 11 || sum_i32->Value(1) != 22 || sum_i32->Value(2) != 33) {
    return arrow::Status::Invalid("unexpected compute result values");
  }

  return arrow::Status::OK();
}

arrow::Status RunAll() {
  ARROW_RETURN_NOT_OK(RunTiForthSmoke());
  ARROW_RETURN_NOT_OK(RunTiForthProjectionSmoke());
  ARROW_RETURN_NOT_OK(RunArrowComputeSmoke());
  return arrow::Status::OK();
}

}  // namespace

int main() {
  auto status = RunAll();
  if (!status.ok()) {
    std::cerr << status.ToString() << "\n";
    return 1;
  }
  return 0;
}
