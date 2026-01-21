#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/scalar.h>
#include <arrow/status.h>
#include <arrow/testing/gtest_util.h>
#include <arrow/type.h>

#include <gtest/gtest.h>

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "test_pipeline_runner.h"
#include "tiforth/engine.h"
#include "tiforth/expr.h"
#include "tiforth/operators/hash_agg.h"
#include "tiforth/operators/projection.h"
#include "tiforth/pipeline_exec.h"

namespace tiforth {

namespace {

class VectorSourceOp final : public SourceOp {
 public:
  explicit VectorSourceOp(std::vector<std::shared_ptr<arrow::RecordBatch>> batches)
      : batches_(std::move(batches)) {}

 protected:
  arrow::Result<OperatorStatus> ReadImpl(std::shared_ptr<arrow::RecordBatch>* batch) override {
    if (batch == nullptr) {
      return arrow::Status::Invalid("batch output must not be null");
    }
    if (offset_ >= batches_.size()) {
      *batch = nullptr;
      return OperatorStatus::kFinished;
    }
    *batch = batches_[offset_++];
    if (*batch == nullptr) {
      return arrow::Status::Invalid("source batch must not be null");
    }
    return OperatorStatus::kHasOutput;
  }

 private:
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches_;
  std::size_t offset_ = 0;
};

struct CollectState {
  std::mutex mu;
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
};

class CollectSinkOp final : public SinkOp {
 public:
  explicit CollectSinkOp(std::shared_ptr<CollectState> state) : state_(std::move(state)) {}

 protected:
  arrow::Result<OperatorStatus> WriteImpl(std::shared_ptr<arrow::RecordBatch> batch) override {
    if (state_ == nullptr) {
      return arrow::Status::Invalid("collect state must not be null");
    }
    if (batch == nullptr) {
      return OperatorStatus::kFinished;
    }
    std::lock_guard<std::mutex> lock(state_->mu);
    state_->batches.push_back(std::move(batch));
    return OperatorStatus::kNeedInput;
  }

 private:
  std::shared_ptr<CollectState> state_;
};

arrow::Result<std::shared_ptr<arrow::Array>> MakeInt32Array(
    const std::vector<std::optional<int32_t>>& values) {
  arrow::Int32Builder builder;
  for (const auto& v : values) {
    if (v.has_value()) {
      ARROW_RETURN_NOT_OK(builder.Append(*v));
    } else {
      ARROW_RETURN_NOT_OK(builder.AppendNull());
    }
  }
  std::shared_ptr<arrow::Array> array;
  ARROW_RETURN_NOT_OK(builder.Finish(&array));
  return array;
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> MakeInt32Batch(
    const std::vector<std::optional<int32_t>>& keys,
    const std::vector<std::optional<int32_t>>& values) {
  if (keys.size() != values.size()) {
    return arrow::Status::Invalid("keys and values must have same length");
  }
  auto schema =
      arrow::schema({arrow::field("k", arrow::int32()), arrow::field("v", arrow::int32())});
  ARROW_ASSIGN_OR_RAISE(auto k_array, MakeInt32Array(keys));
  ARROW_ASSIGN_OR_RAISE(auto v_array, MakeInt32Array(values));
  return arrow::RecordBatch::Make(schema, static_cast<int64_t>(keys.size()), {k_array, v_array});
}

arrow::Status RunParallelHashAggTwoStagePipeline() {
  constexpr int kBuildParallelism = 4;
  constexpr int kResultParallelism = 3;

  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));

  std::vector<AggKey> keys = {{"k", MakeFieldRef("k")}};
  std::vector<AggFunc> aggs = {{"sum_v", "sum", MakeFieldRef("v")}};

  auto agg_ctx = std::make_shared<HashAggContext>(engine.get(), keys, aggs);
  ARROW_RETURN_NOT_OK(agg_ctx->SetExpectedMergeSinkCount(kBuildParallelism));

  ARROW_ASSIGN_OR_RAISE(auto batch0, MakeInt32Batch({1, 2}, {10, 20}));
  ARROW_ASSIGN_OR_RAISE(auto batch1, MakeInt32Batch({1, 3}, {5, 7}));
  ARROW_ASSIGN_OR_RAISE(auto batch2, MakeInt32Batch({2, 3}, {1, 2}));
  ARROW_ASSIGN_OR_RAISE(auto batch3, MakeInt32Batch({4, std::nullopt}, {4, 9}));
  ARROW_ASSIGN_OR_RAISE(auto batch4, MakeInt32Batch({1, 4}, {3, 8}));
  ARROW_ASSIGN_OR_RAISE(auto batch5, MakeInt32Batch({2, std::nullopt}, {6, 1}));

  std::vector<std::shared_ptr<arrow::RecordBatch>> inputs = {batch0, batch1, batch2,
                                                             batch3, batch4, batch5};
  auto partitions = std::make_shared<std::vector<std::vector<std::shared_ptr<arrow::RecordBatch>>>>(
      static_cast<std::size_t>(kBuildParallelism));
  for (std::size_t i = 0; i < inputs.size(); ++i) {
    (*partitions)[i % kBuildParallelism].push_back(inputs[i]);
  }

  auto collect_state = std::make_shared<CollectState>();

  test::PipelineRunner runner;
  test::PipelineStageSpec build_spec;
  build_spec.name = "hash_agg_build";
  build_spec.num_tasks = kBuildParallelism;
  build_spec.task_factory =
      [agg_ctx, partitions](int task_id) -> arrow::Result<std::unique_ptr<PipelineExec>> {
    if (task_id < 0 || static_cast<std::size_t>(task_id) >= partitions->size()) {
      return arrow::Status::Invalid("task_id out of range");
    }

    PipelineExecBuilder builder;
    builder.SetSourceOp(
        std::make_unique<VectorSourceOp>((*partitions)[static_cast<std::size_t>(task_id)]));
    builder.AppendTransformOp(std::make_unique<HashAggTransformOp>(agg_ctx));
    builder.SetSinkOp(std::make_unique<HashAggMergeSinkOp>(agg_ctx));
    return builder.Build();
  };
  ARROW_ASSIGN_OR_RAISE(const auto build_stage, runner.AddStage(std::move(build_spec)));

  const Engine* engine_ptr = engine.get();
  test::PipelineStageSpec result_spec;
  result_spec.name = "hash_agg_result";
  result_spec.num_tasks = kResultParallelism;
  result_spec.task_factory =
      [agg_ctx, engine_ptr,
       collect_state](int /*task_id*/) -> arrow::Result<std::unique_ptr<PipelineExec>> {
    PipelineExecBuilder builder;
    builder.SetSourceOp(std::make_unique<HashAggResultSourceOp>(agg_ctx, /*max_output_rows=*/1));

    std::vector<ProjectionExpr> exprs;
    exprs.push_back({"k", MakeFieldRef("k")});
    exprs.push_back({"sum_v_plus_1",
                     MakeCall("add", {MakeFieldRef("sum_v"),
                                      MakeLiteral(std::make_shared<arrow::Int64Scalar>(1))})});
    builder.AppendTransformOp(
        std::make_unique<ProjectionTransformOp>(engine_ptr, std::move(exprs)));

    builder.SetSinkOp(std::make_unique<CollectSinkOp>(collect_state));
    return builder.Build();
  };
  ARROW_ASSIGN_OR_RAISE(const auto result_stage, runner.AddStage(std::move(result_spec)));

  ARROW_RETURN_NOT_OK(runner.AddDependency(build_stage, result_stage));
  ARROW_RETURN_NOT_OK(runner.Run());

  std::vector<std::shared_ptr<arrow::RecordBatch>> outputs;
  {
    std::lock_guard<std::mutex> lock(collect_state->mu);
    outputs = collect_state->batches;
  }
  if (outputs.empty()) {
    return arrow::Status::Invalid("expected non-empty output");
  }

  std::map<std::optional<int32_t>, int64_t> expected_sum_v_plus_1;
  expected_sum_v_plus_1.emplace(std::optional<int32_t>(1), 18 + 1);
  expected_sum_v_plus_1.emplace(std::optional<int32_t>(2), 27 + 1);
  expected_sum_v_plus_1.emplace(std::optional<int32_t>(3), 9 + 1);
  expected_sum_v_plus_1.emplace(std::optional<int32_t>(4), 12 + 1);
  expected_sum_v_plus_1.emplace(std::optional<int32_t>(), 10 + 1);

  for (const auto& batch : outputs) {
    if (batch == nullptr) {
      return arrow::Status::Invalid("output batch must not be null");
    }
    auto k_array = std::dynamic_pointer_cast<arrow::Int32Array>(batch->GetColumnByName("k"));
    auto sum_array =
        std::dynamic_pointer_cast<arrow::Int64Array>(batch->GetColumnByName("sum_v_plus_1"));
    if (k_array == nullptr || sum_array == nullptr) {
      return arrow::Status::Invalid("unexpected output types");
    }
    if (batch->num_rows() != k_array->length() || batch->num_rows() != sum_array->length()) {
      return arrow::Status::Invalid("output length mismatch");
    }

    for (int64_t i = 0; i < batch->num_rows(); ++i) {
      std::optional<int32_t> key;
      if (!k_array->IsNull(i)) {
        key = k_array->Value(i);
      }
      const auto it = expected_sum_v_plus_1.find(key);
      if (it == expected_sum_v_plus_1.end()) {
        return arrow::Status::Invalid("unexpected group key");
      }
      if (sum_array->IsNull(i) || sum_array->Value(i) != it->second) {
        return arrow::Status::Invalid("sum_v_plus_1 mismatch");
      }
      expected_sum_v_plus_1.erase(it);
    }
  }

  if (!expected_sum_v_plus_1.empty()) {
    return arrow::Status::Invalid("missing expected group keys");
  }
  return arrow::Status::OK();
}

}  // namespace

TEST(TiForthParallelHashAggPipelineTest, TwoStageParallel) {
  ASSERT_OK(RunParallelHashAggTwoStagePipeline());
}

}  // namespace tiforth
