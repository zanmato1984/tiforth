#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/testing/gtest_util.h>
#include <arrow/type.h>

#include <cmath>
#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "tiforth/engine.h"
#include "tiforth/expr.h"
#include "tiforth/operators/hash_agg.h"
#include "tiforth/plan.h"
#include "tiforth/task.h"
#include "tiforth/type_metadata.h"

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

arrow::Result<std::shared_ptr<arrow::RecordBatch>> MakeInt32Batch(
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

arrow::Result<std::shared_ptr<arrow::Array>> MakeStringArray(
    const std::vector<std::optional<std::string>>& values) {
  arrow::StringBuilder builder;
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

arrow::Result<std::shared_ptr<arrow::RecordBatch>> MakeStringKeyBatch(
    const std::vector<std::optional<std::string>>& keys,
    const std::vector<std::optional<int32_t>>& values) {
  if (keys.size() != values.size()) {
    return arrow::Status::Invalid("keys and values must have the same length");
  }

  auto schema =
      arrow::schema({arrow::field("s", arrow::utf8()), arrow::field("v", arrow::int32())});
  ARROW_ASSIGN_OR_RAISE(auto s_array, MakeStringArray(keys));
  ARROW_ASSIGN_OR_RAISE(auto v_array, MakeInt32Array(values));
  return arrow::RecordBatch::Make(schema, static_cast<int64_t>(keys.size()), {s_array, v_array});
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> MakeStringKeyBatchWithSchema(
    const std::shared_ptr<arrow::Schema>& schema,
    const std::vector<std::optional<std::string>>& keys,
    const std::vector<std::optional<int32_t>>& values) {
  if (schema == nullptr) {
    return arrow::Status::Invalid("schema must not be null");
  }
  if (schema->num_fields() != 2) {
    return arrow::Status::Invalid("expected 2 fields in schema");
  }
  if (keys.size() != values.size()) {
    return arrow::Status::Invalid("keys and values must have the same length");
  }

  ARROW_ASSIGN_OR_RAISE(auto s_array, MakeStringArray(keys));
  ARROW_ASSIGN_OR_RAISE(auto v_array, MakeInt32Array(values));
  return arrow::RecordBatch::Make(schema, static_cast<int64_t>(keys.size()), {s_array, v_array});
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> MakeMultiKeyBatch(
    const std::vector<std::optional<int32_t>>& k_values,
    const std::vector<std::optional<std::string>>& s_values,
    const std::vector<std::optional<int32_t>>& v_values) {
  if (k_values.size() != s_values.size() || k_values.size() != v_values.size()) {
    return arrow::Status::Invalid("batch columns must have the same length");
  }

  auto schema = arrow::schema({arrow::field("k", arrow::int32()), arrow::field("s", arrow::utf8()),
                               arrow::field("v", arrow::int32())});
  ARROW_ASSIGN_OR_RAISE(auto k_array, MakeInt32Array(k_values));
  ARROW_ASSIGN_OR_RAISE(auto s_array, MakeStringArray(s_values));
  ARROW_ASSIGN_OR_RAISE(auto v_array, MakeInt32Array(v_values));
  return arrow::RecordBatch::Make(schema, static_cast<int64_t>(k_values.size()),
                                  {k_array, s_array, v_array});
}

arrow::Result<std::vector<std::shared_ptr<arrow::RecordBatch>>> RunAggPlan(
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& inputs, const std::vector<AggKey>& keys,
    const std::vector<AggFunc>& aggs) {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, PlanBuilder::Create(engine.get()));

  const Engine* engine_ptr = engine.get();
  ARROW_ASSIGN_OR_RAISE(const auto ctx_id,
                        builder->AddBreakerState<HashAggContext>(
                            [engine_ptr, keys, aggs]()
                                -> arrow::Result<std::shared_ptr<HashAggContext>> {
                              return std::make_shared<HashAggContext>(engine_ptr, keys, aggs);
                            }));

  ARROW_ASSIGN_OR_RAISE(const auto build_stage, builder->AddStage());
  ARROW_RETURN_NOT_OK(builder->AppendTransform(
      build_stage,
      [ctx_id](PlanTaskContext* ctx) -> arrow::Result<TransformOpPtr> {
        ARROW_ASSIGN_OR_RAISE(auto agg_ctx, ctx->GetBreakerState<HashAggContext>(ctx_id));
        return std::make_unique<HashAggTransformOp>(std::move(agg_ctx));
      }));
  ARROW_RETURN_NOT_OK(builder->SetStageSink(
      build_stage, [ctx_id](PlanTaskContext* ctx) -> arrow::Result<SinkOpPtr> {
        ARROW_ASSIGN_OR_RAISE(auto agg_ctx, ctx->GetBreakerState<HashAggContext>(ctx_id));
        return std::make_unique<HashAggMergeSinkOp>(std::move(agg_ctx));
      }));

  ARROW_ASSIGN_OR_RAISE(const auto result_stage, builder->AddStage());
  ARROW_RETURN_NOT_OK(builder->SetStageSource(
      result_stage, [ctx_id](PlanTaskContext* ctx) -> arrow::Result<SourceOpPtr> {
        ARROW_ASSIGN_OR_RAISE(auto agg_ctx, ctx->GetBreakerState<HashAggContext>(ctx_id));
        return std::make_unique<HashAggResultSourceOp>(std::move(agg_ctx));
      }));
  ARROW_RETURN_NOT_OK(builder->AddDependency(build_stage, result_stage));

  ARROW_ASSIGN_OR_RAISE(auto plan, builder->Finalize());
  ARROW_ASSIGN_OR_RAISE(auto task, plan->CreateTask());

  ARROW_ASSIGN_OR_RAISE(auto state, task->Step());
  if (state != TaskState::kNeedInput) {
    return arrow::Status::Invalid("expected TaskState::kNeedInput");
  }

  for (const auto& batch : inputs) {
    if (batch == nullptr) {
      return arrow::Status::Invalid("input batch must not be null");
    }
    ARROW_RETURN_NOT_OK(task->PushInput(batch));
  }
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
  return outputs;
}

arrow::Status RunHashAggSmoke() {
  std::vector<AggKey> keys = {{"k", MakeFieldRef("k")}};
  std::vector<AggFunc> aggs;
  aggs.push_back({"cnt_all", "count_all", nullptr});
  aggs.push_back({"cnt_v", "count", MakeFieldRef("v")});
  aggs.push_back({"sum_v", "sum", MakeFieldRef("v")});
  aggs.push_back({"min_v", "min", MakeFieldRef("v")});
  aggs.push_back({"max_v", "max", MakeFieldRef("v")});
  aggs.push_back({"mean_v", "mean", MakeFieldRef("v")});

  ARROW_ASSIGN_OR_RAISE(auto batch0,
                        MakeInt32Batch({1, 2, 1, std::nullopt}, {10, 20, std::nullopt, 7}));
  ARROW_ASSIGN_OR_RAISE(
      auto batch1, MakeInt32Batch({2, 3, std::nullopt, 4}, {1, 5, std::nullopt, std::nullopt}));

  ARROW_ASSIGN_OR_RAISE(auto outputs, RunAggPlan({batch0, batch1}, keys, aggs));
  if (outputs.empty()) {
    return arrow::Status::Invalid("expected at least 1 output batch");
  }

  struct ExpectedAgg {
    uint64_t cnt_all;
    uint64_t cnt_v;
    std::optional<int64_t> sum_v;
    std::optional<int32_t> min_v;
    std::optional<int32_t> max_v;
    std::optional<double> mean_v;
  };

  std::map<std::optional<int32_t>, ExpectedAgg> expected;
  expected.emplace(std::optional<int32_t>(1),
                   ExpectedAgg{2, 1, 10, static_cast<int32_t>(10), static_cast<int32_t>(10), 10.0});
  expected.emplace(std::optional<int32_t>(2),
                   ExpectedAgg{2, 2, 21, static_cast<int32_t>(1), static_cast<int32_t>(20), 10.5});
  expected.emplace(std::optional<int32_t>(),
                   ExpectedAgg{2, 1, 7, static_cast<int32_t>(7), static_cast<int32_t>(7), 7.0});
  expected.emplace(std::optional<int32_t>(3),
                   ExpectedAgg{1, 1, 5, static_cast<int32_t>(5), static_cast<int32_t>(5), 5.0});
  expected.emplace(std::optional<int32_t>(4),
                   ExpectedAgg{1, 0, std::nullopt, std::nullopt, std::nullopt, std::nullopt});

  for (const auto& out : outputs) {
    if (out == nullptr) {
      return arrow::Status::Invalid("expected non-null output batch");
    }

    auto k_array = std::dynamic_pointer_cast<arrow::Int32Array>(out->GetColumnByName("k"));
    auto cnt_all_array = std::dynamic_pointer_cast<arrow::UInt64Array>(out->GetColumnByName("cnt_all"));
    auto cnt_v_array = std::dynamic_pointer_cast<arrow::UInt64Array>(out->GetColumnByName("cnt_v"));
    auto sum_array = std::dynamic_pointer_cast<arrow::Int64Array>(out->GetColumnByName("sum_v"));
    auto min_array = std::dynamic_pointer_cast<arrow::Int32Array>(out->GetColumnByName("min_v"));
    auto max_array = std::dynamic_pointer_cast<arrow::Int32Array>(out->GetColumnByName("max_v"));
    auto mean_array = std::dynamic_pointer_cast<arrow::DoubleArray>(out->GetColumnByName("mean_v"));

    if (k_array == nullptr || cnt_all_array == nullptr || cnt_v_array == nullptr ||
        sum_array == nullptr || min_array == nullptr || max_array == nullptr ||
        mean_array == nullptr) {
      return arrow::Status::Invalid("unexpected output types for smoke test");
    }

    for (int64_t i = 0; i < out->num_rows(); ++i) {
      std::optional<int32_t> key;
      if (!k_array->IsNull(i)) {
        key = k_array->Value(i);
      }
      const auto it = expected.find(key);
      if (it == expected.end()) {
        return arrow::Status::Invalid("unexpected group key");
      }
      const auto exp = it->second;

      if (cnt_all_array->IsNull(i) || cnt_all_array->Value(i) != exp.cnt_all) {
        return arrow::Status::Invalid("cnt_all output mismatch");
      }
      if (cnt_v_array->IsNull(i) || cnt_v_array->Value(i) != exp.cnt_v) {
        return arrow::Status::Invalid("cnt_v output mismatch");
      }
      if (exp.sum_v.has_value()) {
        if (sum_array->IsNull(i) || sum_array->Value(i) != *exp.sum_v) {
          return arrow::Status::Invalid("sum_v output mismatch");
        }
      } else if (!sum_array->IsNull(i)) {
        return arrow::Status::Invalid("sum_v output mismatch");
      }
      if (exp.min_v.has_value()) {
        if (min_array->IsNull(i) || min_array->Value(i) != *exp.min_v) {
          return arrow::Status::Invalid("min_v output mismatch");
        }
      } else if (!min_array->IsNull(i)) {
        return arrow::Status::Invalid("min_v output mismatch");
      }
      if (exp.max_v.has_value()) {
        if (max_array->IsNull(i) || max_array->Value(i) != *exp.max_v) {
          return arrow::Status::Invalid("max_v output mismatch");
        }
      } else if (!max_array->IsNull(i)) {
        return arrow::Status::Invalid("max_v output mismatch");
      }
      if (exp.mean_v.has_value()) {
        if (mean_array->IsNull(i) ||
            std::abs(mean_array->Value(i) - *exp.mean_v) > 1e-12) {
          return arrow::Status::Invalid("mean_v output mismatch");
        }
      } else if (!mean_array->IsNull(i)) {
        return arrow::Status::Invalid("mean_v output mismatch");
      }

      expected.erase(it);
    }
  }

  if (!expected.empty()) {
    return arrow::Status::Invalid("missing expected group keys");
  }
  return arrow::Status::OK();
}

arrow::Status RunHashAggComputedExpr() {
  auto plus_one = [](ExprPtr arg) {
    return MakeCall("add", {std::move(arg), MakeLiteral(std::make_shared<arrow::Int32Scalar>(1))});
  };

  std::vector<AggKey> keys = {{"k_plus_1", plus_one(MakeFieldRef("k"))}};
  std::vector<AggFunc> aggs;
  aggs.push_back({"cnt_all", "count_all", nullptr});
  aggs.push_back({"sum_v_plus_1", "sum", plus_one(MakeFieldRef("v"))});

  ARROW_ASSIGN_OR_RAISE(auto batch0,
                        MakeInt32Batch({1, 2, 1, std::nullopt}, {10, 20, std::nullopt, 7}));
  ARROW_ASSIGN_OR_RAISE(
      auto batch1, MakeInt32Batch({2, 3, std::nullopt, 4}, {1, 5, std::nullopt, std::nullopt}));
  ARROW_ASSIGN_OR_RAISE(auto outputs, RunAggPlan({batch0, batch1}, keys, aggs));

  struct Expected {
    uint64_t cnt_all;
    std::optional<int64_t> sum_v;
  };
  std::map<std::optional<int32_t>, Expected> expected;
  expected.emplace(std::optional<int32_t>(2), Expected{2, 11});
  expected.emplace(std::optional<int32_t>(3), Expected{2, 23});
  expected.emplace(std::optional<int32_t>(), Expected{2, 8});
  expected.emplace(std::optional<int32_t>(4), Expected{1, 6});
  expected.emplace(std::optional<int32_t>(5), Expected{1, std::nullopt});

  for (const auto& out : outputs) {
    if (out == nullptr) {
      return arrow::Status::Invalid("expected non-null output batch");
    }
    auto k_array = std::dynamic_pointer_cast<arrow::Int32Array>(out->GetColumnByName("k_plus_1"));
    auto cnt_all_array = std::dynamic_pointer_cast<arrow::UInt64Array>(out->GetColumnByName("cnt_all"));
    auto sum_array = std::dynamic_pointer_cast<arrow::Int64Array>(out->GetColumnByName("sum_v_plus_1"));
    if (k_array == nullptr || cnt_all_array == nullptr || sum_array == nullptr) {
      return arrow::Status::Invalid("unexpected output types for computed expr test");
    }

    for (int64_t i = 0; i < out->num_rows(); ++i) {
      std::optional<int32_t> key;
      if (!k_array->IsNull(i)) {
        key = k_array->Value(i);
      }

      const auto it = expected.find(key);
      if (it == expected.end()) {
        return arrow::Status::Invalid("unexpected group key in computed expr test");
      }
      const auto exp = it->second;

      if (cnt_all_array->IsNull(i) || cnt_all_array->Value(i) != exp.cnt_all) {
        return arrow::Status::Invalid("cnt_all output mismatch");
      }
      if (exp.sum_v.has_value()) {
        if (sum_array->IsNull(i) || sum_array->Value(i) != *exp.sum_v) {
          return arrow::Status::Invalid("sum_v output mismatch");
        }
      } else if (!sum_array->IsNull(i)) {
        return arrow::Status::Invalid("sum_v output mismatch");
      }

      expected.erase(it);
    }
  }

  if (!expected.empty()) {
    return arrow::Status::Invalid("missing expected group keys in computed expr test");
  }
  return arrow::Status::OK();
}

arrow::Status RunHashAggStringKey() {
  std::vector<AggKey> keys = {{"s", MakeFieldRef("s")}};
  std::vector<AggFunc> aggs;
  aggs.push_back({"cnt_all", "count_all", nullptr});
  aggs.push_back({"sum_v", "sum", MakeFieldRef("v")});

  ARROW_ASSIGN_OR_RAISE(
      auto batch0, MakeStringKeyBatch({"a", "b", "a", std::nullopt}, {10, 20, std::nullopt, 7}));
  ARROW_ASSIGN_OR_RAISE(
      auto batch1, MakeStringKeyBatch({"b", "c", std::nullopt, "a"}, {1, 5, std::nullopt, std::nullopt}));
  ARROW_ASSIGN_OR_RAISE(auto outputs, RunAggPlan({batch0, batch1}, keys, aggs));

  struct Expected {
    uint64_t cnt_all;
    std::optional<int64_t> sum_v;
  };
  std::map<std::optional<std::string>, Expected> expected;
  expected.emplace(std::optional<std::string>("a"), Expected{3, 10});
  expected.emplace(std::optional<std::string>("b"), Expected{2, 21});
  expected.emplace(std::optional<std::string>("c"), Expected{1, 5});
  expected.emplace(std::optional<std::string>(), Expected{2, 7});

  for (const auto& out : outputs) {
    if (out == nullptr) {
      return arrow::Status::Invalid("expected non-null output batch");
    }
    auto s_array = std::dynamic_pointer_cast<arrow::StringArray>(out->GetColumnByName("s"));
    auto cnt_all_array = std::dynamic_pointer_cast<arrow::UInt64Array>(out->GetColumnByName("cnt_all"));
    auto sum_array = std::dynamic_pointer_cast<arrow::Int64Array>(out->GetColumnByName("sum_v"));
    if (s_array == nullptr || cnt_all_array == nullptr || sum_array == nullptr) {
      return arrow::Status::Invalid("unexpected output types for string key test");
    }

    for (int64_t i = 0; i < out->num_rows(); ++i) {
      std::optional<std::string> key;
      if (!s_array->IsNull(i)) {
        key = s_array->GetString(i);
      }

      const auto it = expected.find(key);
      if (it == expected.end()) {
        return arrow::Status::Invalid("unexpected group key in string key test");
      }
      const auto exp = it->second;

      if (cnt_all_array->IsNull(i) || cnt_all_array->Value(i) != exp.cnt_all) {
        return arrow::Status::Invalid("cnt_all output mismatch");
      }
      if (exp.sum_v.has_value()) {
        if (sum_array->IsNull(i) || sum_array->Value(i) != *exp.sum_v) {
          return arrow::Status::Invalid("sum_v output mismatch");
        }
      } else if (!sum_array->IsNull(i)) {
        return arrow::Status::Invalid("sum_v output mismatch");
      }
      expected.erase(it);
    }
  }

  if (!expected.empty()) {
    return arrow::Status::Invalid("missing expected group keys in string key test");
  }
  return arrow::Status::OK();
}

arrow::Status RunHashAggCollatedStringKey() {
  auto s_field = arrow::field("s", arrow::utf8());
  LogicalType logical_type;
  logical_type.id = LogicalTypeId::kString;
  logical_type.collation_id = 255;  // UTF8MB4_0900_AI_CI (NO PAD)
  ARROW_ASSIGN_OR_RAISE(s_field, WithLogicalTypeMetadata(s_field, logical_type));

  auto schema = arrow::schema({s_field, arrow::field("v", arrow::int32())});

  std::vector<AggKey> keys = {{"s", MakeFieldRef("s")}};
  std::vector<AggFunc> aggs;
  aggs.push_back({"cnt_all", "count_all", nullptr});
  aggs.push_back({"sum_v", "sum", MakeFieldRef("v")});

  ARROW_ASSIGN_OR_RAISE(auto batch0, MakeStringKeyBatchWithSchema(schema, {"a", "A", "b", std::nullopt},
                                                                  {10, 20, 5, 7}));
  ARROW_ASSIGN_OR_RAISE(auto outputs, RunAggPlan({batch0}, keys, aggs));

  struct Expected {
    uint64_t cnt_all;
    std::optional<int64_t> sum_v;
  };
  std::map<std::optional<std::string>, Expected> expected;
  expected.emplace(std::optional<std::string>("a"), Expected{2, 30});
  expected.emplace(std::optional<std::string>("b"), Expected{1, 5});
  expected.emplace(std::optional<std::string>(), Expected{1, 7});

  for (const auto& out : outputs) {
    if (out == nullptr) {
      return arrow::Status::Invalid("expected non-null output batch");
    }
    auto s_array = std::dynamic_pointer_cast<arrow::StringArray>(out->GetColumnByName("s"));
    auto cnt_all_array =
        std::dynamic_pointer_cast<arrow::UInt64Array>(out->GetColumnByName("cnt_all"));
    auto sum_array = std::dynamic_pointer_cast<arrow::Int64Array>(out->GetColumnByName("sum_v"));
    if (s_array == nullptr || cnt_all_array == nullptr || sum_array == nullptr) {
      return arrow::Status::Invalid("unexpected output types for collated string key test");
    }

    for (int64_t i = 0; i < out->num_rows(); ++i) {
      std::optional<std::string> key;
      if (!s_array->IsNull(i)) {
        key = s_array->GetString(i);
      }

      const auto it = expected.find(key);
      if (it == expected.end()) {
        return arrow::Status::Invalid("unexpected group key in collated string key test");
      }
      const auto exp = it->second;
      if (cnt_all_array->IsNull(i) || cnt_all_array->Value(i) != exp.cnt_all) {
        return arrow::Status::Invalid("cnt_all output mismatch");
      }
      if (exp.sum_v.has_value()) {
        if (sum_array->IsNull(i) || sum_array->Value(i) != *exp.sum_v) {
          return arrow::Status::Invalid("sum_v output mismatch");
        }
      } else if (!sum_array->IsNull(i)) {
        return arrow::Status::Invalid("sum_v output mismatch");
      }

      expected.erase(it);
    }
  }

  if (!expected.empty()) {
    return arrow::Status::Invalid("missing expected group keys in collated string key test");
  }
  return arrow::Status::OK();
}

arrow::Status RunHashAggMultiKey() {
  std::vector<AggKey> keys = {{"k", MakeFieldRef("k")}, {"s", MakeFieldRef("s")}};
  std::vector<AggFunc> aggs;
  aggs.push_back({"cnt_all", "count_all", nullptr});
  aggs.push_back({"sum_v", "sum", MakeFieldRef("v")});

  ARROW_ASSIGN_OR_RAISE(
      auto batch0, MakeMultiKeyBatch({1, 1, 2, std::nullopt}, {"a", "b", "a", "a"},
                                     {10, 20, 30, 40}));
  ARROW_ASSIGN_OR_RAISE(
      auto batch1, MakeMultiKeyBatch({1, 2, 2, std::nullopt}, {"a", "a", std::nullopt, "a"},
                                     {1, 2, 3, 4}));
  ARROW_ASSIGN_OR_RAISE(auto outputs, RunAggPlan({batch0, batch1}, keys, aggs));

  struct Expected {
    uint64_t cnt_all;
    std::optional<int64_t> sum_v;
  };
  using GroupKey = std::tuple<std::optional<int32_t>, std::optional<std::string>>;
  std::map<GroupKey, Expected> expected;
  expected.emplace(GroupKey{std::optional<int32_t>(1), std::optional<std::string>("a")}, Expected{2, 11});
  expected.emplace(GroupKey{std::optional<int32_t>(1), std::optional<std::string>("b")}, Expected{1, 20});
  expected.emplace(GroupKey{std::optional<int32_t>(2), std::optional<std::string>("a")}, Expected{2, 32});
  expected.emplace(GroupKey{std::optional<int32_t>(2), std::optional<std::string>()}, Expected{1, 3});
  expected.emplace(GroupKey{std::optional<int32_t>(), std::optional<std::string>("a")}, Expected{2, 44});

  for (const auto& out : outputs) {
    if (out == nullptr) {
      return arrow::Status::Invalid("expected non-null output batch");
    }
    auto k_array = std::dynamic_pointer_cast<arrow::Int32Array>(out->GetColumnByName("k"));
    auto s_array = std::dynamic_pointer_cast<arrow::StringArray>(out->GetColumnByName("s"));
    auto cnt_all_array = std::dynamic_pointer_cast<arrow::UInt64Array>(out->GetColumnByName("cnt_all"));
    auto sum_array = std::dynamic_pointer_cast<arrow::Int64Array>(out->GetColumnByName("sum_v"));
    if (k_array == nullptr || s_array == nullptr || cnt_all_array == nullptr || sum_array == nullptr) {
      return arrow::Status::Invalid("unexpected output types for multi-key test");
    }

    for (int64_t i = 0; i < out->num_rows(); ++i) {
      std::optional<int32_t> k;
      if (!k_array->IsNull(i)) {
        k = k_array->Value(i);
      }
      std::optional<std::string> s;
      if (!s_array->IsNull(i)) {
        s = s_array->GetString(i);
      }
      GroupKey key{k, s};

      const auto it = expected.find(key);
      if (it == expected.end()) {
        return arrow::Status::Invalid("unexpected group key in multi-key test");
      }
      const auto exp = it->second;

      if (cnt_all_array->IsNull(i) || cnt_all_array->Value(i) != exp.cnt_all) {
        return arrow::Status::Invalid("cnt_all output mismatch");
      }
      if (exp.sum_v.has_value()) {
        if (sum_array->IsNull(i) || sum_array->Value(i) != *exp.sum_v) {
          return arrow::Status::Invalid("sum_v output mismatch");
        }
      } else if (!sum_array->IsNull(i)) {
        return arrow::Status::Invalid("sum_v output mismatch");
      }

      expected.erase(it);
    }
  }

  if (!expected.empty()) {
    return arrow::Status::Invalid("missing expected group keys in multi-key test");
  }
  return arrow::Status::OK();
}

}  // namespace

TEST(TiForthHashAggTest, GroupByAndAggregates) { ASSERT_OK(RunHashAggSmoke()); }
TEST(TiForthHashAggTest, ComputedKeyAndArg) { ASSERT_OK(RunHashAggComputedExpr()); }
TEST(TiForthHashAggTest, StringKey) { ASSERT_OK(RunHashAggStringKey()); }
TEST(TiForthHashAggTest, CollatedStringKey) { ASSERT_OK(RunHashAggCollatedStringKey()); }
TEST(TiForthHashAggTest, MultiKey) { ASSERT_OK(RunHashAggMultiKey()); }

}  // namespace tiforth
