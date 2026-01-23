#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/scalar.h>
#include <arrow/status.h>
#include <arrow/testing/gtest_util.h>
#include <arrow/type.h>

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "tiforth/engine.h"
#include "tiforth/expr.h"
#include "tiforth/operators/hash_agg.h"
#include "tiforth/pipeline/logical_pipeline.h"
#include "tiforth/pipeline/physical_pipeline.h"
#include "tiforth/pipeline/pipeline_context.h"
#include "tiforth/pipeline/pipeline_task.h"
#include "tiforth/task/awaiter.h"
#include "tiforth/task/resumer.h"
#include "tiforth/task/task_context.h"
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

class SimpleAwaiter final : public task::Awaiter {
 public:
  explicit SimpleAwaiter(task::Resumers resumers) : resumers_(std::move(resumers)) {}

  const task::Resumers& resumers() const { return resumers_; }

 private:
  task::Resumers resumers_;
};

class PartitionedVectorSourceOp final : public pipeline::SourceOp {
 public:
  using Partitions = std::vector<std::vector<std::shared_ptr<arrow::RecordBatch>>>;

  explicit PartitionedVectorSourceOp(std::shared_ptr<const Partitions> partitions)
      : partitions_(std::move(partitions)),
        offsets_(partitions_ != nullptr ? partitions_->size() : 0, 0) {}

  pipeline::PipelineSource Source(const pipeline::PipelineContext&) override {
    return [this](const pipeline::PipelineContext&, const task::TaskContext&,
                  pipeline::ThreadId thread_id) -> pipeline::OpResult {
      if (partitions_ == nullptr) {
        return arrow::Status::Invalid("partitions must not be null");
      }
      if (thread_id >= partitions_->size()) {
        return arrow::Status::Invalid("thread id out of range");
      }
      if (thread_id >= offsets_.size()) {
        return arrow::Status::Invalid("thread offset out of range");
      }
      auto& offset = offsets_[thread_id];
      const auto& batches = (*partitions_)[thread_id];
      if (offset >= batches.size()) {
        return pipeline::OpOutput::Finished();
      }
      auto batch = batches[offset++];
      if (batch == nullptr) {
        return arrow::Status::Invalid("source batch must not be null");
      }
      return pipeline::OpOutput::SourcePipeHasMore(std::move(batch));
    };
  }

 private:
  std::shared_ptr<const Partitions> partitions_;
  std::vector<std::size_t> offsets_;
};

class CollectSinkOp final : public pipeline::SinkOp {
 public:
  explicit CollectSinkOp(std::vector<std::shared_ptr<arrow::RecordBatch>>* out) : out_(out) {}

  pipeline::PipelineSink Sink(const pipeline::PipelineContext&) override {
    return [this](const pipeline::PipelineContext&, const task::TaskContext&, pipeline::ThreadId,
                  std::optional<pipeline::Batch> input) -> pipeline::OpResult {
      if (out_ == nullptr) {
        return arrow::Status::Invalid("output slot must not be null");
      }
      if (!input.has_value()) {
        return pipeline::OpOutput::PipeSinkNeedsMore();
      }
      auto batch = std::move(*input);
      if (batch != nullptr) {
        out_->push_back(std::move(batch));
      }
      return pipeline::OpOutput::PipeSinkNeedsMore();
    };
  }

 private:
  std::vector<std::shared_ptr<arrow::RecordBatch>>* out_ = nullptr;
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

task::TaskContext MakeTestTaskContext() {
  task::TaskContext task_ctx;
  task_ctx.resumer_factory = []() -> arrow::Result<task::ResumerPtr> {
    return std::make_shared<SimpleResumer>();
  };
  task_ctx.single_awaiter_factory =
      [](task::ResumerPtr resumer) -> arrow::Result<task::AwaiterPtr> {
    task::Resumers resumers;
    resumers.push_back(std::move(resumer));
    return std::make_shared<SimpleAwaiter>(std::move(resumers));
  };
  task_ctx.any_awaiter_factory =
      [](task::Resumers resumers) -> arrow::Result<task::AwaiterPtr> {
    return std::make_shared<SimpleAwaiter>(std::move(resumers));
  };
  task_ctx.all_awaiter_factory =
      [](task::Resumers resumers) -> arrow::Result<task::AwaiterPtr> {
    return std::make_shared<SimpleAwaiter>(std::move(resumers));
  };
  return task_ctx;
}

arrow::Status RunPipelineTaskToCompletion(pipeline::PipelineTask* pipeline_task,
                                         const pipeline::PipelineContext& pipeline_ctx,
                                         const task::TaskContext& task_ctx) {
  if (pipeline_task == nullptr) {
    return arrow::Status::Invalid("pipeline task must not be null");
  }

  while (true) {
    ARROW_ASSIGN_OR_RAISE(auto status, (*pipeline_task)(pipeline_ctx, task_ctx, /*thread_id=*/0));
    if (status.IsFinished()) {
      return arrow::Status::OK();
    }
    if (status.IsCancelled()) {
      return arrow::Status::Cancelled("pipeline task cancelled");
    }
    if (status.IsBlocked()) {
      return arrow::Status::Invalid("unexpected Blocked in this test");
    }
    if (status.IsYield()) {
      std::this_thread::yield();
      continue;
    }
    ARROW_CHECK(status.IsContinue());
  }
}

arrow::Status RunParallelHashAggTwoStagePipeline() {
  constexpr int kBuildParallelism = 4;
  constexpr int kResultParallelism = 3;

  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));

  std::vector<AggKey> keys = {{"k", MakeFieldRef("k")}};
  std::vector<AggFunc> aggs = {{"sum_v", "sum", MakeFieldRef("v")}};

  auto agg_state = std::make_shared<HashAggState>(engine.get(), keys, aggs,
                                                  /*grouper_factory=*/HashAggState::GrouperFactory{},
                                                  /*memory_pool=*/nullptr,
                                                  /*dop=*/static_cast<std::size_t>(kBuildParallelism));

  ARROW_ASSIGN_OR_RAISE(auto batch0, MakeInt32Batch({1, 2}, {10, 20}));
  ARROW_ASSIGN_OR_RAISE(auto batch1, MakeInt32Batch({1, 3}, {5, 7}));
  ARROW_ASSIGN_OR_RAISE(auto batch2, MakeInt32Batch({2, 3}, {1, 2}));
  ARROW_ASSIGN_OR_RAISE(auto batch3, MakeInt32Batch({4, std::nullopt}, {4, 9}));
  ARROW_ASSIGN_OR_RAISE(auto batch4, MakeInt32Batch({1, 4}, {3, 8}));
  ARROW_ASSIGN_OR_RAISE(auto batch5, MakeInt32Batch({2, std::nullopt}, {6, 1}));

  std::vector<std::shared_ptr<arrow::RecordBatch>> inputs = {batch0, batch1, batch2,
                                                             batch3, batch4, batch5};
  auto partitions = std::make_shared<PartitionedVectorSourceOp::Partitions>(
      static_cast<std::size_t>(kBuildParallelism));
  for (std::size_t i = 0; i < inputs.size(); ++i) {
    (*partitions)[i % kBuildParallelism].push_back(inputs[i]);
  }

  auto outputs_by_task =
      std::make_shared<std::vector<std::vector<std::shared_ptr<arrow::RecordBatch>>>>(
          kResultParallelism);

  const auto pipeline_ctx = pipeline::PipelineContext{};
  auto task_ctx = MakeTestTaskContext();

  // Warm up the hash agg state (compile exprs, init kernels/groupers) without affecting results.
  ARROW_RETURN_NOT_OK(agg_state->Consume(/*thread_id=*/0, *batch0->Slice(/*offset=*/0, /*length=*/0)));

  auto source_op = std::make_unique<PartitionedVectorSourceOp>(partitions);
  auto sink_op = std::make_unique<HashAggSinkOp>(agg_state);

  pipeline::LogicalPipeline::Channel channel;
  channel.source_op = source_op.get();
  channel.pipe_ops = {};
  pipeline::LogicalPipeline logical{
      "HashAggBuild",
      std::vector<pipeline::LogicalPipeline::Channel>{std::move(channel)},
      sink_op.get()};
  auto physical = pipeline::CompilePipeline(pipeline_ctx, logical);
  if (physical.size() != 1) {
    return arrow::Status::Invalid("expected a single physical pipeline");
  }

  pipeline::PipelineTask pipeline_task{pipeline_ctx, physical[0],
                                       /*dop=*/static_cast<std::size_t>(kBuildParallelism)};
  {
    std::atomic_bool has_error{false};
    arrow::Status first_error;
    std::vector<std::thread> threads;
    threads.reserve(kBuildParallelism);
    for (int task_id = 0; task_id < kBuildParallelism; ++task_id) {
      threads.emplace_back([&, task_id]() {
        const auto thread_id = static_cast<pipeline::ThreadId>(task_id);
        while (true) {
          auto st_res = pipeline_task(pipeline_ctx, task_ctx, thread_id);
          if (!st_res.ok()) {
            if (!has_error.exchange(true)) {
              first_error = st_res.status();
            }
            return;
          }
          const auto st = st_res.ValueUnsafe();
          if (st.IsFinished()) {
            return;
          }
          if (st.IsCancelled()) {
            if (!has_error.exchange(true)) {
              first_error = arrow::Status::Cancelled("pipeline task cancelled");
            }
            return;
          }
          if (st.IsBlocked()) {
            if (!has_error.exchange(true)) {
              first_error = arrow::Status::Invalid("unexpected Blocked in this test");
            }
            return;
          }
          if (st.IsYield()) {
            std::this_thread::yield();
            continue;
          }
          ARROW_CHECK(st.IsContinue());
        }
      });
    }
    for (auto& th : threads) {
      th.join();
    }
    if (has_error.load()) {
      return first_error.ok() ? arrow::Status::Invalid("build stage failed") : first_error;
    }
  }

  ARROW_ASSIGN_OR_RAISE(auto out, agg_state->OutputBatch());
  if (out == nullptr) {
    return arrow::Status::Invalid("expected non-null hash agg output");
  }
  const int64_t total_rows = out->num_rows();
  if (total_rows < 0) {
    return arrow::Status::Invalid("negative output row count");
  }

  {
    std::atomic_bool has_error{false};
    arrow::Status first_error;
    std::vector<std::thread> threads;
    threads.reserve(kResultParallelism);
    for (int task_id = 0; task_id < kResultParallelism; ++task_id) {
      threads.emplace_back([&, task_id]() {
        if (task_id < 0 || static_cast<std::size_t>(task_id) >= outputs_by_task->size()) {
          has_error.store(true);
          first_error = arrow::Status::Invalid("task_id out of range");
          return;
        }
        const int64_t start = (static_cast<int64_t>(task_id) * total_rows) / kResultParallelism;
        const int64_t end =
            (static_cast<int64_t>(task_id + 1) * total_rows) / kResultParallelism;

        auto source_op = std::make_unique<HashAggResultSourceOp>(agg_state, start, end,
                                                                 /*max_output_rows=*/1);
        auto* slot = &(*outputs_by_task)[static_cast<std::size_t>(task_id)];
        auto sink_op = std::make_unique<CollectSinkOp>(slot);

        pipeline::LogicalPipeline::Channel channel;
        channel.source_op = source_op.get();
        channel.pipe_ops = {};
        pipeline::LogicalPipeline logical{"HashAggResult",
                                          std::vector<pipeline::LogicalPipeline::Channel>{
                                              std::move(channel)},
                                          sink_op.get()};
        auto physical = pipeline::CompilePipeline(pipeline_ctx, logical);
        if (physical.size() != 1) {
          has_error.store(true);
          first_error = arrow::Status::Invalid("expected a single physical pipeline");
          return;
        }

        pipeline::PipelineTask pipeline_task{pipeline_ctx, physical[0], /*dop=*/1};
        auto st = RunPipelineTaskToCompletion(&pipeline_task, pipeline_ctx, task_ctx);
        if (!st.ok() && !has_error.exchange(true)) {
          first_error = std::move(st);
        }
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
  for (auto& task_outputs : *outputs_by_task) {
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
