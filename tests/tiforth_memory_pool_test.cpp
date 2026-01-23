#include <arrow/memory_pool.h>
#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "tiforth/engine.h"
#include "tiforth/expr.h"
#include "tiforth/operators/hash_agg.h"
#include "tiforth/operators/hash_join.h"
#include "tiforth/operators/sort.h"
#include "tiforth/pipeline/logical_pipeline.h"
#include "tiforth/pipeline/task_groups.h"
#include "tiforth/type_metadata.h"

#include "test_pipeline_ops.h"
#include "test_task_group_runner.h"

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
    const std::vector<std::optional<int32_t>>& xs) {
  auto schema = arrow::schema({arrow::field("x", arrow::int32())});
  ARROW_ASSIGN_OR_RAISE(auto x_array, MakeInt32Array(xs));
  return arrow::RecordBatch::Make(schema, static_cast<int64_t>(xs.size()), {x_array});
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> MakeBatch2(std::string a_name,
                                                              std::string b_name,
                                                              const std::vector<std::optional<int32_t>>& a,
                                                              const std::vector<std::optional<int32_t>>& b) {
  if (a.size() != b.size()) {
    return arrow::Status::Invalid("batch column size mismatch");
  }
  auto schema = arrow::schema({arrow::field(std::move(a_name), arrow::int32()),
                               arrow::field(std::move(b_name), arrow::int32())});
  ARROW_ASSIGN_OR_RAISE(auto a_array, MakeInt32Array(a));
  ARROW_ASSIGN_OR_RAISE(auto b_array, MakeInt32Array(b));
  return arrow::RecordBatch::Make(schema, static_cast<int64_t>(a.size()), {a_array, b_array});
}

arrow::Result<std::shared_ptr<arrow::Array>> MakeBinaryArray(
    const std::vector<std::optional<std::string>>& values) {
  arrow::BinaryBuilder builder;
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

arrow::Result<std::shared_ptr<arrow::RecordBatch>> MakeBinaryBatch(
    int32_t collation_id, const std::vector<std::optional<std::string>>& xs) {
  auto field = arrow::field("x", arrow::binary());
  LogicalType logical;
  logical.id = LogicalTypeId::kString;
  logical.collation_id = collation_id;
  ARROW_ASSIGN_OR_RAISE(field, WithLogicalTypeMetadata(field, logical));
  auto schema = arrow::schema({field});

  ARROW_ASSIGN_OR_RAISE(auto x_array, MakeBinaryArray(xs));
  return arrow::RecordBatch::Make(schema, static_cast<int64_t>(xs.size()), {x_array});
}

arrow::Status RunMemoryPoolSmoke() {
  arrow::ProxyMemoryPool pool(arrow::default_memory_pool());

  EngineOptions options;
  options.memory_pool = &pool;
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(options));
  const auto* eng = engine.get();

  std::vector<SortKey> keys = {SortKey{.name = "x", .ascending = true, .nulls_first = false}};

  ARROW_ASSIGN_OR_RAISE(auto batch0, MakeBatch({3, 1}));
  ARROW_ASSIGN_OR_RAISE(auto batch1, MakeBatch({std::nullopt, 2}));

  auto source_op = std::make_unique<test::VectorSourceOp>(
      std::vector<std::shared_ptr<arrow::RecordBatch>>{batch0, batch1});

  std::vector<std::unique_ptr<pipeline::PipeOp>> pipe_ops;
  pipe_ops.push_back(std::make_unique<SortPipeOp>(eng, std::move(keys)));

  test::CollectSinkOp::OutputsByThread outputs_by_thread(1);
  auto sink_op = std::make_unique<test::CollectSinkOp>(&outputs_by_thread);

  pipeline::LogicalPipeline::Channel channel;
  channel.source_op = source_op.get();
  channel.pipe_ops.reserve(pipe_ops.size());
  for (auto& op : pipe_ops) {
    channel.pipe_ops.push_back(op.get());
  }

  pipeline::LogicalPipeline logical_pipeline{
      "MemoryPoolSort",
      std::vector<pipeline::LogicalPipeline::Channel>{std::move(channel)},
      sink_op.get()};

  ARROW_ASSIGN_OR_RAISE(
      auto task_groups,
      pipeline::CompileToTaskGroups(pipeline::PipelineContext{}, logical_pipeline, /*dop=*/1));

  auto task_ctx = test::MakeTestTaskContext();
  ARROW_RETURN_NOT_OK(test::RunTaskGroupsToCompletion(task_groups, task_ctx));

  if (pool.num_allocations() <= 0 || pool.total_bytes_allocated() <= 0) {
    return arrow::Status::Invalid("expected allocations to go through the provided memory pool");
  }
  return arrow::Status::OK();
}

arrow::Result<int64_t> RunCollatedSortBytesAllocated(int32_t collation_id) {
  arrow::ProxyMemoryPool pool(arrow::default_memory_pool());

  EngineOptions options;
  options.memory_pool = &pool;
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(options));
  const auto* eng = engine.get();

  std::vector<SortKey> keys = {SortKey{.name = "x", .ascending = true, .nulls_first = false}};

  std::vector<std::optional<std::string>> values;
  values.reserve(128);
  for (int i = 0; i < 128; ++i) {
    values.push_back(std::string(64, static_cast<char>('a' + (i % 26))));
  }
  ARROW_ASSIGN_OR_RAISE(auto batch, MakeBinaryBatch(collation_id, values));

  auto source_op = std::make_unique<test::VectorSourceOp>(
      std::vector<std::shared_ptr<arrow::RecordBatch>>{batch});

  std::vector<std::unique_ptr<pipeline::PipeOp>> pipe_ops;
  pipe_ops.push_back(std::make_unique<SortPipeOp>(eng, std::move(keys)));

  test::CollectSinkOp::OutputsByThread outputs_by_thread(1);
  auto sink_op = std::make_unique<test::CollectSinkOp>(&outputs_by_thread);

  pipeline::LogicalPipeline::Channel channel;
  channel.source_op = source_op.get();
  channel.pipe_ops.reserve(pipe_ops.size());
  for (auto& op : pipe_ops) {
    channel.pipe_ops.push_back(op.get());
  }

  pipeline::LogicalPipeline logical_pipeline{
      "CollatedSortBytesAllocated",
      std::vector<pipeline::LogicalPipeline::Channel>{std::move(channel)},
      sink_op.get()};

  ARROW_ASSIGN_OR_RAISE(
      auto task_groups,
      pipeline::CompileToTaskGroups(pipeline::PipelineContext{}, logical_pipeline, /*dop=*/1));

  auto task_ctx = test::MakeTestTaskContext();
  ARROW_RETURN_NOT_OK(test::RunTaskGroupsToCompletion(task_groups, task_ctx));

  return pool.total_bytes_allocated();
}

arrow::Status RunHashJoinMemoryPoolSmoke() {
  arrow::ProxyMemoryPool pool(arrow::default_memory_pool());

  EngineOptions options;
  options.memory_pool = &pool;
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(options));
  const auto* eng = engine.get();

  ARROW_ASSIGN_OR_RAISE(auto build,
                        MakeBatch2("k", "bv", {1, 2, 2, std::nullopt}, {100, 200, 201, 999}));
  std::vector<std::shared_ptr<arrow::RecordBatch>> build_batches = {std::move(build)};
  JoinKey key{.left = {"k"}, .right = {"k"}};

  ARROW_ASSIGN_OR_RAISE(auto probe, MakeBatch2("k", "pv", {2, 1, 3, std::nullopt}, {20, 10, 30, 0}));

  auto source_op = std::make_unique<test::VectorSourceOp>(
      std::vector<std::shared_ptr<arrow::RecordBatch>>{probe});

  std::vector<std::unique_ptr<pipeline::PipeOp>> pipe_ops;
  pipe_ops.push_back(std::make_unique<HashJoinPipeOp>(eng, std::move(build_batches), key));

  test::CollectSinkOp::OutputsByThread outputs_by_thread(1);
  auto sink_op = std::make_unique<test::CollectSinkOp>(&outputs_by_thread);

  pipeline::LogicalPipeline::Channel channel;
  channel.source_op = source_op.get();
  channel.pipe_ops.reserve(pipe_ops.size());
  for (auto& op : pipe_ops) {
    channel.pipe_ops.push_back(op.get());
  }

  pipeline::LogicalPipeline logical_pipeline{
      "MemoryPoolHashJoin",
      std::vector<pipeline::LogicalPipeline::Channel>{std::move(channel)},
      sink_op.get()};

  ARROW_ASSIGN_OR_RAISE(
      auto task_groups,
      pipeline::CompileToTaskGroups(pipeline::PipelineContext{}, logical_pipeline, /*dop=*/1));

  auto task_ctx = test::MakeTestTaskContext();
  ARROW_RETURN_NOT_OK(test::RunTaskGroupsToCompletion(task_groups, task_ctx));

  if (pool.num_allocations() <= 0 || pool.total_bytes_allocated() <= 0) {
    return arrow::Status::Invalid("expected allocations to go through the provided memory pool");
  }
  return arrow::Status::OK();
}

arrow::Result<int64_t> RunHashAggBytesAllocated(int32_t collation_id) {
  arrow::ProxyMemoryPool pool(arrow::default_memory_pool());

  EngineOptions options;
  options.memory_pool = &pool;
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(options));

  std::vector<AggKey> keys = {AggKey{.name = "x", .expr = MakeFieldRef("x")}};
  std::vector<AggFunc> aggs;
  aggs.push_back(AggFunc{.name = "cnt", .func = "count_all", .arg = nullptr});
  auto agg_state = std::make_shared<HashAggState>(engine.get(), keys, aggs);

  std::vector<std::optional<std::string>> values;
  values.reserve(4096);
  for (int i = 0; i < 4096; ++i) {
    std::string s(64, static_cast<char>('a' + (i % 26)));
    s[0] = static_cast<char>('a' + (i % 26));
    s[1] = static_cast<char>('0' + ((i / 26) % 10));
    s[2] = static_cast<char>('0' + ((i / 260) % 10));
    values.push_back(std::move(s));
  }
  ARROW_ASSIGN_OR_RAISE(auto batch, MakeBinaryBatch(collation_id, values));

  // Build stage.
  {
    auto source_op = std::make_unique<test::VectorSourceOp>(
        std::vector<std::shared_ptr<arrow::RecordBatch>>{batch});
    auto sink_op = std::make_unique<HashAggSinkOp>(agg_state);

    pipeline::LogicalPipeline::Channel channel;
    channel.source_op = source_op.get();
    channel.pipe_ops = {};

    pipeline::LogicalPipeline logical_pipeline{
        "HashAggBuildBytes",
        std::vector<pipeline::LogicalPipeline::Channel>{std::move(channel)},
        sink_op.get()};

    ARROW_ASSIGN_OR_RAISE(auto task_groups,
                          pipeline::CompileToTaskGroups(pipeline::PipelineContext{}, logical_pipeline,
                                                        /*dop=*/1));
    auto task_ctx = test::MakeTestTaskContext();
    ARROW_RETURN_NOT_OK(test::RunTaskGroupsToCompletion(task_groups, task_ctx));
  }

  // Result stage (force output materialization).
  {
    auto source_op = std::make_unique<HashAggResultSourceOp>(agg_state, /*max_output_rows=*/1 << 30);

    test::CollectSinkOp::OutputsByThread outputs_by_thread(1);
    auto sink_op = std::make_unique<test::CollectSinkOp>(&outputs_by_thread);

    pipeline::LogicalPipeline::Channel channel;
    channel.source_op = source_op.get();
    channel.pipe_ops = {};

    pipeline::LogicalPipeline logical_pipeline{
        "HashAggResultBytes",
        std::vector<pipeline::LogicalPipeline::Channel>{std::move(channel)},
        sink_op.get()};

    ARROW_ASSIGN_OR_RAISE(auto task_groups,
                          pipeline::CompileToTaskGroups(pipeline::PipelineContext{}, logical_pipeline,
                                                        /*dop=*/1));
    auto task_ctx = test::MakeTestTaskContext();
    ARROW_RETURN_NOT_OK(test::RunTaskGroupsToCompletion(task_groups, task_ctx));
    (void)test::FlattenOutputs(std::move(outputs_by_thread));
  }

  return pool.total_bytes_allocated();
}

}  // namespace

TEST(TiForthMemoryPoolTest, SortUsesEnginePool) {
  auto status = RunMemoryPoolSmoke();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthMemoryPoolTest, CollatedSortUsesEnginePoolForSortKeys) {
  auto bytes_binary = RunCollatedSortBytesAllocated(/*collation_id=*/63);
  ASSERT_TRUE(bytes_binary.ok()) << bytes_binary.status().ToString();

  auto bytes_general_ci = RunCollatedSortBytesAllocated(/*collation_id=*/33);
  ASSERT_TRUE(bytes_general_ci.ok()) << bytes_general_ci.status().ToString();

  ASSERT_GT(*bytes_general_ci, *bytes_binary + 8192)
      << "expected collated sort to allocate additional sort-key memory through the engine pool";
}

TEST(TiForthMemoryPoolTest, HashJoinUsesEnginePool) {
  auto status = RunHashJoinMemoryPoolSmoke();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthMemoryPoolTest, HashAggUsesEnginePool) {
  auto bytes_binary = RunHashAggBytesAllocated(/*collation_id=*/63);
  ASSERT_TRUE(bytes_binary.ok()) << bytes_binary.status().ToString();

  auto bytes_general_ci = RunHashAggBytesAllocated(/*collation_id=*/33);
  ASSERT_TRUE(bytes_general_ci.ok()) << bytes_general_ci.status().ToString();

  ASSERT_GT(*bytes_binary, 0);
  ASSERT_GT(*bytes_general_ci, 0);
}

}  // namespace tiforth
