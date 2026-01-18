#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/testing/gtest_util.h>

#include <cstdint>
#include <optional>
#include <vector>

#include <gtest/gtest.h>

#include "tiforth/engine.h"
#include "tiforth/expr.h"
#include "tiforth/operators/hash_agg.h"
#include "tiforth/plan.h"
#include "tiforth/task.h"

namespace tiforth {

namespace {

arrow::Result<std::shared_ptr<arrow::Array>> MakeInt32Array(
    const std::vector<std::optional<int32_t>>& values) {
  arrow::Int32Builder builder;
  for (const auto& value : values) {
    if (value.has_value()) {
      ARROW_RETURN_NOT_OK(builder.Append(*value));
    } else {
      ARROW_RETURN_NOT_OK(builder.AppendNull());
    }
  }
  std::shared_ptr<arrow::Array> out;
  ARROW_RETURN_NOT_OK(builder.Finish(&out));
  return out;
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> MakeBatch(
    const std::vector<std::optional<int32_t>>& keys, const std::vector<std::optional<int32_t>>& values) {
  if (keys.size() != values.size()) {
    return arrow::Status::Invalid("keys and values must have the same length");
  }
  auto schema = arrow::schema({arrow::field("k", arrow::int32()), arrow::field("v", arrow::int32())});
  ARROW_ASSIGN_OR_RAISE(auto k_array, MakeInt32Array(keys));
  ARROW_ASSIGN_OR_RAISE(auto v_array, MakeInt32Array(values));
  return arrow::RecordBatch::Make(schema, static_cast<int64_t>(keys.size()), {k_array, v_array});
}

arrow::Status RunHashAggBreakerGlobalEmptyInputSmoke() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, PlanBuilder::Create(engine.get()));

  std::vector<AggKey> keys;
  std::vector<AggFunc> aggs;
  aggs.push_back({"cnt_all", "count_all", nullptr});
  aggs.push_back({"sum_v", "sum", MakeFieldRef("v")});
  aggs.push_back({"min_v", "min", MakeFieldRef("v")});
  aggs.push_back({"max_v", "max", MakeFieldRef("v")});

  const Engine* engine_ptr = engine.get();
  ARROW_ASSIGN_OR_RAISE(const auto ctx_id, builder->AddBreakerState<HashAggContext>(
                                               [engine_ptr, keys, aggs]() -> arrow::Result<std::shared_ptr<HashAggContext>> {
                                                 return std::make_shared<HashAggContext>(engine_ptr, keys, aggs);
                                               }));

  ARROW_ASSIGN_OR_RAISE(const auto build_stage, builder->AddStage());
  ARROW_RETURN_NOT_OK(builder->SetStageSink(
      build_stage, [ctx_id](PlanTaskContext* ctx) -> arrow::Result<SinkOpPtr> {
        ARROW_ASSIGN_OR_RAISE(auto agg_ctx, ctx->GetBreakerState<HashAggContext>(ctx_id));
        return std::make_unique<HashAggBuildSinkOp>(std::move(agg_ctx));
      }));

  ARROW_ASSIGN_OR_RAISE(const auto convergent_stage, builder->AddStage());
  ARROW_RETURN_NOT_OK(builder->SetStageSource(
      convergent_stage, [ctx_id](PlanTaskContext* ctx) -> arrow::Result<SourceOpPtr> {
        ARROW_ASSIGN_OR_RAISE(auto agg_ctx, ctx->GetBreakerState<HashAggContext>(ctx_id));
        return std::make_unique<HashAggConvergentSourceOp>(std::move(agg_ctx),
                                                           /*max_output_rows=*/1 << 30);
      }));
  ARROW_RETURN_NOT_OK(builder->AddDependency(build_stage, convergent_stage));

  ARROW_ASSIGN_OR_RAISE(auto plan, builder->Finalize());
  ARROW_ASSIGN_OR_RAISE(auto task, plan->CreateTask());

  ARROW_ASSIGN_OR_RAISE(const auto initial_state, task->Step());
  if (initial_state != TaskState::kNeedInput) {
    return arrow::Status::Invalid("expected TaskState::kNeedInput");
  }

  ARROW_ASSIGN_OR_RAISE(auto empty_input, MakeBatch({}, {}));
  ARROW_RETURN_NOT_OK(task->PushInput(empty_input));
  ARROW_RETURN_NOT_OK(task->CloseInput());

  std::vector<std::shared_ptr<arrow::RecordBatch>> outputs;
  while (true) {
    ARROW_ASSIGN_OR_RAISE(const auto state, task->Step());
    if (state == TaskState::kFinished) {
      break;
    }
    if (state == TaskState::kNeedInput) {
      continue;
    }
    if (state != TaskState::kHasOutput) {
      return arrow::Status::Invalid("unexpected task state");
    }

    ARROW_ASSIGN_OR_RAISE(auto out, task->PullOutput());
    if (out == nullptr) {
      return arrow::Status::Invalid("expected non-null output batch");
    }
    outputs.push_back(std::move(out));
  }

  if (outputs.size() != 1) {
    return arrow::Status::Invalid("expected exactly 1 output batch");
  }

  const auto& out = outputs[0];
  if (out->num_columns() != 4 || out->num_rows() != 1) {
    return arrow::Status::Invalid("unexpected output shape");
  }
  if (out->schema()->field(0)->name() != "cnt_all" || out->schema()->field(1)->name() != "sum_v" ||
      out->schema()->field(2)->name() != "min_v" || out->schema()->field(3)->name() != "max_v") {
    return arrow::Status::Invalid("unexpected output schema");
  }

  arrow::UInt64Builder cnt_all_builder;
  ARROW_RETURN_NOT_OK(cnt_all_builder.Append(0));
  std::shared_ptr<arrow::Array> expect_cnt_all;
  ARROW_RETURN_NOT_OK(cnt_all_builder.Finish(&expect_cnt_all));

  arrow::Int64Builder sum_builder;
  ARROW_RETURN_NOT_OK(sum_builder.AppendNull());
  std::shared_ptr<arrow::Array> expect_sum;
  ARROW_RETURN_NOT_OK(sum_builder.Finish(&expect_sum));

  arrow::Int32Builder min_builder;
  ARROW_RETURN_NOT_OK(min_builder.AppendNull());
  std::shared_ptr<arrow::Array> expect_min;
  ARROW_RETURN_NOT_OK(min_builder.Finish(&expect_min));

  arrow::Int32Builder max_builder;
  ARROW_RETURN_NOT_OK(max_builder.AppendNull());
  std::shared_ptr<arrow::Array> expect_max;
  ARROW_RETURN_NOT_OK(max_builder.Finish(&expect_max));

  if (!expect_cnt_all->Equals(*out->column(0)) || !expect_sum->Equals(*out->column(1)) ||
      !expect_min->Equals(*out->column(2)) || !expect_max->Equals(*out->column(3))) {
    return arrow::Status::Invalid("unexpected output values");
  }

  return arrow::Status::OK();
}

}  // namespace

TEST(TiForthHashAggBreakerTest, GlobalAggEmptyInput) {
  auto status = RunHashAggBreakerGlobalEmptyInputSmoke();
  ASSERT_OK(status);
}

}  // namespace tiforth

