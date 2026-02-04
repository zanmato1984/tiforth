#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/scalar.h>
#include <arrow/status.h>
#include <gtest/gtest.h>

#include "tiforth/engine.h"
#include "tiforth/expr.h"
#include "tiforth/operators/projection.h"
#include "tiforth/broken_pipeline_traits.h"

#include "test_pipeline_ops.h"
#include "test_task_group_runner.h"

namespace tiforth {

namespace {

arrow::Result<std::shared_ptr<arrow::Array>> MakeInt32Array(const std::vector<int32_t>& values) {
  arrow::Int32Builder builder;
  ARROW_RETURN_NOT_OK(builder.AppendValues(values));
  std::shared_ptr<arrow::Array> array;
  ARROW_RETURN_NOT_OK(builder.Finish(&array));
  return array;
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> MakeBatch(const std::vector<int32_t>& xs,
                                                            const std::vector<int32_t>& ys) {
  if (xs.size() != ys.size()) {
    return arrow::Status::Invalid("xs and ys must have the same length");
  }

  auto schema = arrow::schema({arrow::field("x", arrow::int32()), arrow::field("y", arrow::int32())});
  ARROW_ASSIGN_OR_RAISE(auto x_array, MakeInt32Array(xs));
  ARROW_ASSIGN_OR_RAISE(auto y_array, MakeInt32Array(ys));
  return arrow::RecordBatch::Make(schema, static_cast<int64_t>(xs.size()), {x_array, y_array});
}

arrow::Status RunProjectionSmoke() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));

  std::vector<op::ProjectionExpr> exprs;
  exprs.push_back({"x", MakeFieldRef("x")});
  exprs.push_back({"x_plus_y", MakeCall("add", {MakeFieldRef("x"), MakeFieldRef("y")})});
  exprs.push_back({"x_plus_10",
                   MakeCall("add",
                            {MakeFieldRef("x"), MakeLiteral(std::make_shared<arrow::Int32Scalar>(10))})});

  ARROW_ASSIGN_OR_RAISE(auto batch0, MakeBatch({1, 2, 3}, {10, 20, 30}));
  ARROW_ASSIGN_OR_RAISE(auto batch1, MakeBatch({4}, {100}));

  auto source_op = std::make_unique<test::VectorSourceOp>(
      std::vector<std::shared_ptr<arrow::RecordBatch>>{batch0, batch1});

  std::vector<std::unique_ptr<PipeOp>> pipe_ops;
  pipe_ops.push_back(std::make_unique<op::ProjectionPipeOp>(engine.get(), std::move(exprs)));

  test::CollectSinkOp::OutputsByThread outputs_by_thread(1);
  auto sink_op = std::make_unique<test::CollectSinkOp>(&outputs_by_thread);

  LogicalPipeline::Channel channel;
  channel.source_op = source_op.get();
  channel.pipe_ops.reserve(pipe_ops.size());
  for (auto& op : pipe_ops) {
    channel.pipe_ops.push_back(op.get());
  }

  LogicalPipeline logical_pipeline{"Projection", std::vector<LogicalPipeline::Channel>{std::move(channel)},
                                   sink_op.get()};

  ARROW_ASSIGN_OR_RAISE(auto task_groups,
                        test::CompileToTaskGroups(logical_pipeline, /*dop=*/1));

  auto task_ctx = test::MakeTestTaskContext();
  ARROW_RETURN_NOT_OK(test::RunTaskGroupsToCompletion(task_groups, task_ctx));

  auto outputs = test::FlattenOutputs(std::move(outputs_by_thread));

  std::shared_ptr<arrow::Schema> seen_schema;
  for (const auto& out : outputs) {
    if (out == nullptr) {
      return arrow::Status::Invalid("expected non-null output batch");
    }
    if (seen_schema == nullptr) {
      seen_schema = out->schema();
    } else if (out->schema().get() != seen_schema.get()) {
      return arrow::Status::Invalid("expected stable shared output schema");
    }
  }

  if (outputs.size() != 2) {
    return arrow::Status::Invalid("expected 2 output batches");
  }

  // Validate batch0 output.
  if (outputs[0]->num_columns() != 3 || outputs[0]->num_rows() != 3) {
    return arrow::Status::Invalid("unexpected output batch0 shape");
  }
  if (outputs[0]->schema()->field(0)->name() != "x" ||
      outputs[0]->schema()->field(1)->name() != "x_plus_y" ||
      outputs[0]->schema()->field(2)->name() != "x_plus_10") {
    return arrow::Status::Invalid("unexpected output batch0 schema");
  }

  ARROW_ASSIGN_OR_RAISE(auto expect_x, MakeInt32Array({1, 2, 3}));
  ARROW_ASSIGN_OR_RAISE(auto expect_x_plus_y, MakeInt32Array({11, 22, 33}));
  ARROW_ASSIGN_OR_RAISE(auto expect_x_plus_10, MakeInt32Array({11, 12, 13}));

  if (!expect_x->Equals(*outputs[0]->column(0)) ||
      !expect_x_plus_y->Equals(*outputs[0]->column(1)) ||
      !expect_x_plus_10->Equals(*outputs[0]->column(2))) {
    return arrow::Status::Invalid("unexpected output batch0 values");
  }

  // Validate batch1 output.
  if (outputs[1]->num_rows() != 1 || outputs[1]->num_columns() != 3) {
    return arrow::Status::Invalid("unexpected output batch1 shape");
  }
  ARROW_ASSIGN_OR_RAISE(auto expect_x1, MakeInt32Array({4}));
  ARROW_ASSIGN_OR_RAISE(auto expect_x1_plus_y, MakeInt32Array({104}));
  ARROW_ASSIGN_OR_RAISE(auto expect_x1_plus_10, MakeInt32Array({14}));
  if (!expect_x1->Equals(*outputs[1]->column(0)) ||
      !expect_x1_plus_y->Equals(*outputs[1]->column(1)) ||
      !expect_x1_plus_10->Equals(*outputs[1]->column(2))) {
    return arrow::Status::Invalid("unexpected output batch1 values");
  }

  return arrow::Status::OK();
}

}  // namespace

TEST(TiForthProjectionTest, ProjectionAdd) {
  auto status = RunProjectionSmoke();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

}  // namespace tiforth
