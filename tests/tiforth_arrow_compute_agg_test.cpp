#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/testing/gtest_util.h>
#include <arrow/type.h>

#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <vector>

#include <gtest/gtest.h>

#include "tiforth/engine.h"
#include "tiforth/expr.h"
#include "tiforth/operators/arrow_compute_agg.h"
#include "tiforth/pipeline.h"
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
  std::shared_ptr<arrow::Array> array;
  ARROW_RETURN_NOT_OK(builder.Finish(&array));
  return array;
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> MakeBatch(
    const std::vector<std::optional<int32_t>>& keys,
    const std::vector<std::optional<int32_t>>& values) {
  if (keys.size() != values.size()) {
    return arrow::Status::Invalid("keys and values must have the same length");
  }

  auto schema =
      arrow::schema({arrow::field("k", arrow::int32()), arrow::field("v", arrow::int32())});
  ARROW_ASSIGN_OR_RAISE(auto k_array, MakeInt32Array(keys));
  ARROW_ASSIGN_OR_RAISE(auto v_array, MakeInt32Array(values));
  return arrow::RecordBatch::Make(schema, static_cast<int64_t>(keys.size()), {k_array, v_array});
}

arrow::Status RunArrowComputeAggSmoke() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, PipelineBuilder::Create(engine.get()));

  std::vector<AggKey> keys = {{"k", MakeFieldRef("k")}};
  std::vector<AggFunc> aggs;
  aggs.push_back({"cnt_all", "count_all", nullptr});
  aggs.push_back({"cnt_v", "count", MakeFieldRef("v")});
  aggs.push_back({"sum_v", "sum", MakeFieldRef("v")});
  aggs.push_back({"min_v", "min", MakeFieldRef("v")});
  aggs.push_back({"max_v", "max", MakeFieldRef("v")});
  aggs.push_back({"mean_v", "mean", MakeFieldRef("v")});

  ARROW_RETURN_NOT_OK(builder->AppendTransform(
      [engine_ptr = engine.get(), keys, aggs]() -> arrow::Result<TransformOpPtr> {
        return std::make_unique<ArrowComputeAggTransformOp>(engine_ptr, keys, aggs);
      }));

  ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());
  ARROW_ASSIGN_OR_RAISE(auto task, pipeline->CreateTask());

  ARROW_ASSIGN_OR_RAISE(auto state, task->Step());
  if (state != TaskState::kNeedInput) {
    return arrow::Status::Invalid("expected TaskState::kNeedInput");
  }

  ARROW_ASSIGN_OR_RAISE(auto batch0, MakeBatch({1, 2, 1, std::nullopt}, {10, 20, std::nullopt, 7}));
  ARROW_RETURN_NOT_OK(task->PushInput(batch0));
  ARROW_ASSIGN_OR_RAISE(
      auto batch1,
      MakeBatch({2, 3, std::nullopt, 4}, {1, 5, std::nullopt, std::nullopt}));
  ARROW_RETURN_NOT_OK(task->PushInput(batch1));
  ARROW_RETURN_NOT_OK(task->CloseInput());

  std::vector<std::shared_ptr<arrow::RecordBatch>> outputs;
  while (true) {
    ARROW_ASSIGN_OR_RAISE(state, task->Step());
    if (state == TaskState::kFinished) {
      break;
    }
    if (state == TaskState::kNeedInput) {
      continue;
    }
    if (state != TaskState::kHasOutput) {
      return arrow::Status::Invalid("expected TaskState::kHasOutput/kNeedInput/kFinished");
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
  if (out->num_columns() != 7 || out->num_rows() != 5) {
    return arrow::Status::Invalid("unexpected output shape");
  }

  ARROW_ASSIGN_OR_RAISE(auto expected_k, MakeInt32Array({1, 2, std::nullopt, 3, 4}));

  arrow::Int64Builder cnt_all_builder;
  ARROW_RETURN_NOT_OK(cnt_all_builder.AppendValues({2, 2, 2, 1, 1}));
  std::shared_ptr<arrow::Array> expected_cnt_all;
  ARROW_RETURN_NOT_OK(cnt_all_builder.Finish(&expected_cnt_all));

  arrow::Int64Builder cnt_v_builder;
  ARROW_RETURN_NOT_OK(cnt_v_builder.AppendValues({1, 2, 1, 1, 0}));
  std::shared_ptr<arrow::Array> expected_cnt_v;
  ARROW_RETURN_NOT_OK(cnt_v_builder.Finish(&expected_cnt_v));

  arrow::Int64Builder sum_builder;
  ARROW_RETURN_NOT_OK(sum_builder.AppendValues({10, 21, 7, 5}));
  ARROW_RETURN_NOT_OK(sum_builder.AppendNull());
  std::shared_ptr<arrow::Array> expected_sum;
  ARROW_RETURN_NOT_OK(sum_builder.Finish(&expected_sum));

  ARROW_ASSIGN_OR_RAISE(auto expected_min, MakeInt32Array({10, 1, 7, 5, std::nullopt}));
  ARROW_ASSIGN_OR_RAISE(auto expected_max, MakeInt32Array({10, 20, 7, 5, std::nullopt}));

  arrow::DoubleBuilder mean_builder;
  ARROW_RETURN_NOT_OK(mean_builder.Append(10.0));
  ARROW_RETURN_NOT_OK(mean_builder.Append(10.5));
  ARROW_RETURN_NOT_OK(mean_builder.Append(7.0));
  ARROW_RETURN_NOT_OK(mean_builder.Append(5.0));
  ARROW_RETURN_NOT_OK(mean_builder.AppendNull());
  std::shared_ptr<arrow::Array> expected_mean;
  ARROW_RETURN_NOT_OK(mean_builder.Finish(&expected_mean));

  auto actual_k = out->GetColumnByName("k");
  auto actual_cnt_all = out->GetColumnByName("cnt_all");
  auto actual_cnt_v = out->GetColumnByName("cnt_v");
  auto actual_sum = out->GetColumnByName("sum_v");
  auto actual_min = out->GetColumnByName("min_v");
  auto actual_max = out->GetColumnByName("max_v");
  auto actual_mean = out->GetColumnByName("mean_v");

  if (actual_k == nullptr || actual_cnt_all == nullptr || actual_cnt_v == nullptr || actual_sum == nullptr ||
      actual_min == nullptr || actual_max == nullptr || actual_mean == nullptr) {
    return arrow::Status::Invalid("missing expected output columns");
  }

  if (!actual_k->Equals(expected_k)) {
    return arrow::Status::Invalid("k output mismatch");
  }
  if (!actual_cnt_all->Equals(expected_cnt_all)) {
    return arrow::Status::Invalid("cnt_all output mismatch");
  }
  if (!actual_cnt_v->Equals(expected_cnt_v)) {
    return arrow::Status::Invalid("cnt_v output mismatch");
  }
  if (!actual_sum->Equals(expected_sum)) {
    return arrow::Status::Invalid("sum_v output mismatch");
  }
  if (!actual_min->Equals(expected_min)) {
    return arrow::Status::Invalid("min_v output mismatch");
  }
  if (!actual_max->Equals(expected_max)) {
    return arrow::Status::Invalid("max_v output mismatch");
  }
  if (!actual_mean->Equals(expected_mean)) {
    return arrow::Status::Invalid("mean_v output mismatch");
  }

  return arrow::Status::OK();
}

}  // namespace

TEST(TiForthArrowComputeAggTest, GroupByAndAggregates) { ASSERT_OK(RunArrowComputeAggSmoke()); }

}  // namespace tiforth
