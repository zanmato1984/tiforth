#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/testing/gtest_util.h>
#include <arrow/type.h>

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "tiforth/engine.h"
#include "tiforth/expr.h"
#include "tiforth/operators/hash_agg.h"
#include "tiforth/traits.h"

#include "test_pipeline_ops.h"
#include "test_task_group_runner.h"

namespace tiforth {

namespace {

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
  constexpr std::size_t kBuildParallelism = 4;
  constexpr std::size_t kResultParallelism = 3;

  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));

  std::vector<op::AggKey> keys = {{"k", MakeFieldRef("k")}};
  std::vector<op::AggFunc> aggs = {{"sum_v", "sum", MakeFieldRef("v")}};

  auto agg_state =
      std::make_shared<op::HashAggState>(engine.get(), keys, aggs,
                                     /*grouper_factory=*/op::HashAggState::GrouperFactory{},
                                     /*memory_pool=*/nullptr,
                                     /*dop=*/kBuildParallelism);

  ARROW_ASSIGN_OR_RAISE(auto batch0, MakeInt32Batch({1, 2}, {10, 20}));
  ARROW_ASSIGN_OR_RAISE(auto batch1, MakeInt32Batch({1, 3}, {5, 7}));
  ARROW_ASSIGN_OR_RAISE(auto batch2, MakeInt32Batch({2, 3}, {1, 2}));
  ARROW_ASSIGN_OR_RAISE(auto batch3, MakeInt32Batch({4, std::nullopt}, {4, 9}));
  ARROW_ASSIGN_OR_RAISE(auto batch4, MakeInt32Batch({1, 4}, {3, 8}));
  ARROW_ASSIGN_OR_RAISE(auto batch5, MakeInt32Batch({2, std::nullopt}, {6, 1}));

  const std::vector<std::shared_ptr<arrow::RecordBatch>> inputs = {batch0, batch1, batch2,
                                                                   batch3, batch4, batch5};
  auto partitions = test::RoundRobinPartition(inputs, kBuildParallelism);

  auto task_ctx = test::MakeTestTaskContext();

  // Warm up the hash agg state (compile exprs, init kernels/groupers) without affecting results.
  ARROW_RETURN_NOT_OK(
      agg_state->Consume(/*thread_id=*/0, *batch0->Slice(/*offset=*/0, /*length=*/0)));

  // Build stage: consume all input into HashAggState in parallel.
  {
    auto source_op = std::make_unique<test::PartitionedVectorSourceOp>(std::move(partitions));
    auto sink_op = std::make_unique<op::HashAggSinkOp>(agg_state);

    Pipeline::Channel channel;
    channel.source_op = source_op.get();
    channel.pipe_ops = {};

    Pipeline logical_pipeline{
        "HashAggBuild",
        std::vector<Pipeline::Channel>{std::move(channel)},
        sink_op.get()};

    ARROW_ASSIGN_OR_RAISE(auto task_groups,
                          test::CompileToTaskGroups(logical_pipeline, /*dop=*/kBuildParallelism));
    ARROW_RETURN_NOT_OK(test::RunTaskGroupsToCompletion(task_groups, task_ctx));
  }

  // Materialize finalized output once (provides a happens-before barrier for threads below).
  ARROW_ASSIGN_OR_RAISE(auto out, agg_state->OutputBatch());
  if (out == nullptr) {
    return arrow::Status::Invalid("expected non-null hash agg output");
  }
  const int64_t total_rows = out->num_rows();
  if (total_rows < 0) {
    return arrow::Status::Invalid("negative output row count");
  }

  // Result stage: scan output in parallel by range partitioning.
  std::vector<std::vector<std::shared_ptr<arrow::RecordBatch>>> outputs_by_task(kResultParallelism);
  {
    std::mutex mu;
    std::atomic_bool has_error{false};
    arrow::Status first_error;

    std::vector<std::thread> threads;
    threads.reserve(kResultParallelism);
    for (std::size_t task_id = 0; task_id < kResultParallelism; ++task_id) {
      threads.emplace_back([&, task_id]() {
        const int64_t start = (static_cast<int64_t>(task_id) * total_rows) /
                              static_cast<int64_t>(kResultParallelism);
        const int64_t end = (static_cast<int64_t>(task_id + 1) * total_rows) /
                            static_cast<int64_t>(kResultParallelism);

        auto source_op = std::make_unique<op::HashAggResultSourceOp>(agg_state, start, end,
                                                                 /*max_output_rows=*/1);

        test::CollectSinkOp::OutputsByThread outputs_by_thread(1);
        auto sink_op = std::make_unique<test::CollectSinkOp>(&outputs_by_thread);

        Pipeline::Channel channel;
        channel.source_op = source_op.get();
        channel.pipe_ops = {};

        Pipeline logical_pipeline{
            "HashAggResult",
            std::vector<Pipeline::Channel>{std::move(channel)},
            sink_op.get()};

        auto groups_r = test::CompileToTaskGroups(logical_pipeline, /*dop=*/1);
        if (!groups_r.ok()) {
          if (!has_error.exchange(true)) {
            std::lock_guard<std::mutex> lock(mu);
            first_error = groups_r.status();
          }
          return;
        }

        auto st = test::RunTaskGroupsToCompletion(groups_r.ValueUnsafe(), task_ctx);
        if (!st.ok() && !has_error.exchange(true)) {
          std::lock_guard<std::mutex> lock(mu);
          first_error = std::move(st);
          return;
        }

        outputs_by_task[task_id] = test::FlattenOutputs(std::move(outputs_by_thread));
      });
    }

    for (auto& th : threads) {
      th.join();
    }

    if (has_error.load()) {
      return first_error.ok() ? arrow::Status::Invalid("result stage failed") : first_error;
    }
  }

  std::vector<std::shared_ptr<arrow::RecordBatch>> outputs;
  outputs.reserve(32);
  for (auto& task_outputs : outputs_by_task) {
    for (auto& batch : task_outputs) {
      outputs.push_back(std::move(batch));
    }
  }
  if (outputs.empty()) {
    return arrow::Status::Invalid("expected non-empty output");
  }

  std::map<std::optional<int32_t>, int64_t> expected_sum_v;
  expected_sum_v.emplace(std::optional<int32_t>(1), 18);
  expected_sum_v.emplace(std::optional<int32_t>(2), 27);
  expected_sum_v.emplace(std::optional<int32_t>(3), 9);
  expected_sum_v.emplace(std::optional<int32_t>(4), 12);
  expected_sum_v.emplace(std::optional<int32_t>(), 10);

  for (const auto& batch : outputs) {
    if (batch == nullptr) {
      return arrow::Status::Invalid("output batch must not be null");
    }
    auto k_array = std::dynamic_pointer_cast<arrow::Int32Array>(batch->GetColumnByName("k"));
    auto sum_array = std::dynamic_pointer_cast<arrow::Int64Array>(batch->GetColumnByName("sum_v"));
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
      const auto it = expected_sum_v.find(key);
      if (it == expected_sum_v.end()) {
        return arrow::Status::Invalid("unexpected group key");
      }
      if (sum_array->IsNull(i) || sum_array->Value(i) != it->second) {
        return arrow::Status::Invalid("sum_v mismatch");
      }
      expected_sum_v.erase(it);
    }
  }

  if (!expected_sum_v.empty()) {
    return arrow::Status::Invalid("missing expected group keys");
  }
  return arrow::Status::OK();
}

}  // namespace

TEST(TiForthParallelHashAggPipelineTest, TwoStageParallel) {
  ASSERT_OK(RunParallelHashAggTwoStagePipeline());
}

}  // namespace tiforth
