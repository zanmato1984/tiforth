#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/util/decimal.h>

#include <cmath>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <gtest/gtest.h>

#include "tiforth/engine.h"
#include "tiforth/expr.h"
#include "tiforth/operators/hash_agg.h"
#include "tiforth/pipeline.h"
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

arrow::Result<std::shared_ptr<arrow::Array>> MakeBinaryArray(
    const std::vector<std::optional<std::string>>& values) {
  arrow::BinaryBuilder builder;
  for (const auto& value : values) {
    if (value.has_value()) {
      ARROW_RETURN_NOT_OK(builder.Append(reinterpret_cast<const uint8_t*>(value->data()),
                                         static_cast<int32_t>(value->size())));
    } else {
      ARROW_RETURN_NOT_OK(builder.AppendNull());
    }
  }
  std::shared_ptr<arrow::Array> array;
  ARROW_RETURN_NOT_OK(builder.Finish(&array));
  return array;
}

arrow::Result<std::shared_ptr<arrow::Array>> MakeDoubleArray(
    const std::vector<std::optional<double>>& values) {
  arrow::DoubleBuilder builder;
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

arrow::Result<std::shared_ptr<arrow::Array>> MakeFloatArray(
    const std::vector<std::optional<float>>& values) {
  arrow::FloatBuilder builder;
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

arrow::Result<std::shared_ptr<arrow::Array>> MakeDecimal128Array(
    int32_t precision, int32_t scale, const std::vector<std::optional<int64_t>>& unscaled_values) {
  auto type = arrow::decimal128(precision, scale);
  arrow::Decimal128Builder builder(type);
  for (const auto& value : unscaled_values) {
    if (value.has_value()) {
      ARROW_RETURN_NOT_OK(builder.Append(arrow::Decimal128(*value)));
    } else {
      ARROW_RETURN_NOT_OK(builder.AppendNull());
    }
  }
  std::shared_ptr<arrow::Array> array;
  ARROW_RETURN_NOT_OK(builder.Finish(&array));
  return array;
}

arrow::Result<std::shared_ptr<arrow::Array>> MakeDecimal256Array(
    int32_t precision, int32_t scale, const std::vector<std::optional<int64_t>>& unscaled_values) {
  auto type = arrow::decimal256(precision, scale);
  arrow::Decimal256Builder builder(type);
  for (const auto& value : unscaled_values) {
    if (value.has_value()) {
      ARROW_RETURN_NOT_OK(builder.Append(arrow::Decimal256(*value)));
    } else {
      ARROW_RETURN_NOT_OK(builder.AppendNull());
    }
  }
  std::shared_ptr<arrow::Array> array;
  ARROW_RETURN_NOT_OK(builder.Finish(&array));
  return array;
}

arrow::Result<std::shared_ptr<arrow::Field>> MakeBinaryFieldWithCollation(std::string name,
                                                                          int32_t collation_id) {
  auto field = arrow::field(std::move(name), arrow::binary(), /*nullable=*/true);
  LogicalType type;
  type.id = LogicalTypeId::kString;
  type.collation_id = collation_id;
  return WithLogicalTypeMetadata(field, type);
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> MakeBatch(
    const std::vector<std::optional<int32_t>>& keys, const std::vector<std::optional<int32_t>>& values) {
  if (keys.size() != values.size()) {
    return arrow::Status::Invalid("keys and values must have the same length");
  }

  auto schema =
      arrow::schema({arrow::field("k", arrow::int32()), arrow::field("v", arrow::int32())});
  ARROW_ASSIGN_OR_RAISE(auto k_array, MakeInt32Array(keys));
  ARROW_ASSIGN_OR_RAISE(auto v_array, MakeInt32Array(values));
  return arrow::RecordBatch::Make(schema, static_cast<int64_t>(keys.size()), {k_array, v_array});
}

arrow::Status RunHashAggSmoke() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, PipelineBuilder::Create(engine.get()));

  std::vector<AggKey> keys = {{"k", MakeFieldRef("k")}};
  std::vector<AggFunc> aggs;
  aggs.push_back({"cnt", "count_all", nullptr});
  aggs.push_back({"sum_v", "sum", MakeFieldRef("v")});

  ARROW_RETURN_NOT_OK(builder->AppendTransform(
      [engine_ptr = engine.get(), keys, aggs]() -> arrow::Result<TransformOpPtr> {
        return std::make_unique<HashAggTransformOp>(engine_ptr, keys, aggs);
      }));

  ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());
  ARROW_ASSIGN_OR_RAISE(auto task, pipeline->CreateTask());

  ARROW_ASSIGN_OR_RAISE(auto initial_state, task->Step());
  if (initial_state != TaskState::kNeedInput) {
    return arrow::Status::Invalid("expected TaskState::kNeedInput");
  }

  ARROW_ASSIGN_OR_RAISE(auto batch0, MakeBatch({1, 2, 1, std::nullopt}, {10, 20, std::nullopt, 7}));
  ARROW_RETURN_NOT_OK(task->PushInput(batch0));
  ARROW_ASSIGN_OR_RAISE(auto batch1, MakeBatch({2, 3, std::nullopt, 4}, {1, 5, std::nullopt, std::nullopt}));
  ARROW_RETURN_NOT_OK(task->PushInput(batch1));
  ARROW_RETURN_NOT_OK(task->CloseInput());

  std::vector<std::shared_ptr<arrow::RecordBatch>> outputs;
  while (true) {
    ARROW_ASSIGN_OR_RAISE(auto state, task->Step());
    if (state == TaskState::kFinished) {
      break;
    }
    if (state == TaskState::kNeedInput) {
      // Hash agg is blocking; it consumes input without producing output.
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
  if (out->num_columns() != 3 || out->num_rows() != 5) {
    return arrow::Status::Invalid("unexpected output shape");
  }
  if (out->schema()->field(0)->name() != "cnt" || out->schema()->field(1)->name() != "sum_v" ||
      out->schema()->field(2)->name() != "k") {
    return arrow::Status::Invalid("unexpected output schema");
  }

  arrow::Int32Builder key_builder;
  ARROW_RETURN_NOT_OK(key_builder.Append(1));
  ARROW_RETURN_NOT_OK(key_builder.Append(2));
  ARROW_RETURN_NOT_OK(key_builder.AppendNull());
  ARROW_RETURN_NOT_OK(key_builder.Append(3));
  ARROW_RETURN_NOT_OK(key_builder.Append(4));
  std::shared_ptr<arrow::Array> expect_k;
  ARROW_RETURN_NOT_OK(key_builder.Finish(&expect_k));

  arrow::UInt64Builder cnt_builder;
  ARROW_RETURN_NOT_OK(cnt_builder.AppendValues({2, 2, 2, 1, 1}));
  std::shared_ptr<arrow::Array> expect_cnt;
  ARROW_RETURN_NOT_OK(cnt_builder.Finish(&expect_cnt));

  arrow::Int64Builder sum_builder;
  ARROW_RETURN_NOT_OK(sum_builder.AppendValues({10, 21, 7, 5}));
  ARROW_RETURN_NOT_OK(sum_builder.AppendNull());
  std::shared_ptr<arrow::Array> expect_sum;
  ARROW_RETURN_NOT_OK(sum_builder.Finish(&expect_sum));

  if (!expect_cnt->Equals(*out->column(0)) || !expect_sum->Equals(*out->column(1)) ||
      !expect_k->Equals(*out->column(2))) {
    return arrow::Status::Invalid("unexpected output values");
  }

  return arrow::Status::OK();
}

arrow::Status RunHashAggTwoKeySmoke() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, PipelineBuilder::Create(engine.get()));

  std::vector<AggKey> keys = {{"s", MakeFieldRef("s")}, {"k2", MakeFieldRef("k2")}};
  std::vector<AggFunc> aggs;
  aggs.push_back({"cnt", "count_all", nullptr});
  aggs.push_back({"sum_v", "sum", MakeFieldRef("v")});

  ARROW_RETURN_NOT_OK(builder->AppendTransform(
      [engine_ptr = engine.get(), keys, aggs]() -> arrow::Result<TransformOpPtr> {
        return std::make_unique<HashAggTransformOp>(engine_ptr, keys, aggs);
      }));

  ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());
  ARROW_ASSIGN_OR_RAISE(auto task, pipeline->CreateTask());

  ARROW_ASSIGN_OR_RAISE(auto initial_state, task->Step());
  if (initial_state != TaskState::kNeedInput) {
    return arrow::Status::Invalid("expected TaskState::kNeedInput");
  }

  // s: binary with padding BIN collation; k2: int32; v: int32.
  ARROW_ASSIGN_OR_RAISE(auto s_field, MakeBinaryFieldWithCollation("s", /*collation_id=*/46));
  auto schema = arrow::schema({s_field, arrow::field("k2", arrow::int32()),
                               arrow::field("v", arrow::int32())});

  std::vector<std::optional<std::string>> s_values = {std::string("a"), std::string("a "),
                                                      std::string("a"), std::nullopt,
                                                      std::nullopt, std::string("a "),
                                                      std::string("a")};
  ARROW_ASSIGN_OR_RAISE(auto s_array, MakeBinaryArray(s_values));
  ARROW_ASSIGN_OR_RAISE(auto k2_array, MakeInt32Array({1, 1, 2, 1, 2, std::nullopt, std::nullopt}));
  ARROW_ASSIGN_OR_RAISE(auto v_array, MakeInt32Array({10, 20, 1, 7, 8, 5, std::nullopt}));
  auto batch = arrow::RecordBatch::Make(schema, static_cast<int64_t>(s_values.size()),
                                        {s_array, k2_array, v_array});

  ARROW_RETURN_NOT_OK(task->PushInput(batch));
  ARROW_RETURN_NOT_OK(task->CloseInput());

  std::vector<std::shared_ptr<arrow::RecordBatch>> outputs;
  while (true) {
    ARROW_ASSIGN_OR_RAISE(auto state, task->Step());
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
  if (out->num_columns() != 4 || out->num_rows() != 5) {
    return arrow::Status::Invalid("unexpected output shape");
  }
  if (out->schema()->field(0)->name() != "cnt" || out->schema()->field(1)->name() != "sum_v" ||
      out->schema()->field(2)->name() != "s" || out->schema()->field(3)->name() != "k2") {
    return arrow::Status::Invalid("unexpected output schema");
  }

  // Validate metadata preservation for the string key.
  ARROW_ASSIGN_OR_RAISE(const auto logical_type, GetLogicalType(*out->schema()->field(2)));
  if (logical_type.id != LogicalTypeId::kString || logical_type.collation_id != 46) {
    return arrow::Status::Invalid("unexpected output string key metadata");
  }

  arrow::BinaryBuilder s_expect_builder;
  ARROW_RETURN_NOT_OK(s_expect_builder.Append(reinterpret_cast<const uint8_t*>("a"), 1));
  ARROW_RETURN_NOT_OK(s_expect_builder.Append(reinterpret_cast<const uint8_t*>("a"), 1));
  ARROW_RETURN_NOT_OK(s_expect_builder.AppendNull());
  ARROW_RETURN_NOT_OK(s_expect_builder.AppendNull());
  ARROW_RETURN_NOT_OK(s_expect_builder.Append(reinterpret_cast<const uint8_t*>("a "), 2));
  std::shared_ptr<arrow::Array> s_expect;
  ARROW_RETURN_NOT_OK(s_expect_builder.Finish(&s_expect));

  ARROW_ASSIGN_OR_RAISE(auto k2_expect, MakeInt32Array({1, 2, 1, 2, std::nullopt}));

  arrow::UInt64Builder cnt_builder;
  ARROW_RETURN_NOT_OK(cnt_builder.AppendValues({2, 1, 1, 1, 2}));
  std::shared_ptr<arrow::Array> cnt_expect;
  ARROW_RETURN_NOT_OK(cnt_builder.Finish(&cnt_expect));

  arrow::Int64Builder sum_builder;
  ARROW_RETURN_NOT_OK(sum_builder.AppendValues({30, 1, 7, 8, 5}));
  std::shared_ptr<arrow::Array> sum_expect;
  ARROW_RETURN_NOT_OK(sum_builder.Finish(&sum_expect));

  if (!cnt_expect->Equals(*out->column(0)) || !sum_expect->Equals(*out->column(1)) ||
      !s_expect->Equals(*out->column(2)) || !k2_expect->Equals(*out->column(3))) {
    return arrow::Status::Invalid("unexpected output values");
  }

  return arrow::Status::OK();
}

arrow::Status RunHashAggGeneralCiStringKeySmoke() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, PipelineBuilder::Create(engine.get()));

  std::vector<AggKey> keys = {{"s", MakeFieldRef("s")}};
  std::vector<AggFunc> aggs;
  aggs.push_back({"cnt", "count_all", nullptr});
  aggs.push_back({"sum_v", "sum", MakeFieldRef("v")});

  ARROW_RETURN_NOT_OK(builder->AppendTransform(
      [engine_ptr = engine.get(), keys, aggs]() -> arrow::Result<TransformOpPtr> {
        return std::make_unique<HashAggTransformOp>(engine_ptr, keys, aggs);
      }));

  ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());
  ARROW_ASSIGN_OR_RAISE(auto task, pipeline->CreateTask());

  ARROW_ASSIGN_OR_RAISE(auto initial_state, task->Step());
  if (initial_state != TaskState::kNeedInput) {
    return arrow::Status::Invalid("expected TaskState::kNeedInput");
  }

  ARROW_ASSIGN_OR_RAISE(auto s_field, MakeBinaryFieldWithCollation("s", /*collation_id=*/45));
  auto schema = arrow::schema({s_field, arrow::field("v", arrow::int32())});

  std::vector<std::optional<std::string>> s_values = {std::string("a"), std::string("A"),
                                                      std::string("a "), std::string("b")};
  ARROW_ASSIGN_OR_RAISE(auto s_array, MakeBinaryArray(s_values));
  ARROW_ASSIGN_OR_RAISE(auto v_array, MakeInt32Array({10, 20, 1, 5}));
  auto batch = arrow::RecordBatch::Make(schema, static_cast<int64_t>(s_values.size()),
                                        {s_array, v_array});

  ARROW_RETURN_NOT_OK(task->PushInput(batch));
  ARROW_RETURN_NOT_OK(task->CloseInput());

  std::vector<std::shared_ptr<arrow::RecordBatch>> outputs;
  while (true) {
    ARROW_ASSIGN_OR_RAISE(auto state, task->Step());
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
  if (out->num_columns() != 3 || out->num_rows() != 2) {
    return arrow::Status::Invalid("unexpected output shape");
  }
  if (out->schema()->field(0)->name() != "cnt" || out->schema()->field(1)->name() != "sum_v" ||
      out->schema()->field(2)->name() != "s") {
    return arrow::Status::Invalid("unexpected output schema");
  }

  ARROW_ASSIGN_OR_RAISE(const auto logical_type, GetLogicalType(*out->schema()->field(2)));
  if (logical_type.id != LogicalTypeId::kString || logical_type.collation_id != 45) {
    return arrow::Status::Invalid("unexpected output string key metadata");
  }

  arrow::BinaryBuilder s_expect_builder;
  ARROW_RETURN_NOT_OK(s_expect_builder.Append(reinterpret_cast<const uint8_t*>("a"), 1));
  ARROW_RETURN_NOT_OK(s_expect_builder.Append(reinterpret_cast<const uint8_t*>("b"), 1));
  std::shared_ptr<arrow::Array> s_expect;
  ARROW_RETURN_NOT_OK(s_expect_builder.Finish(&s_expect));

  arrow::UInt64Builder cnt_builder;
  ARROW_RETURN_NOT_OK(cnt_builder.AppendValues({3, 1}));
  std::shared_ptr<arrow::Array> cnt_expect;
  ARROW_RETURN_NOT_OK(cnt_builder.Finish(&cnt_expect));

  arrow::Int64Builder sum_builder;
  ARROW_RETURN_NOT_OK(sum_builder.AppendValues({31, 5}));
  std::shared_ptr<arrow::Array> sum_expect;
  ARROW_RETURN_NOT_OK(sum_builder.Finish(&sum_expect));

  if (!cnt_expect->Equals(*out->column(0)) || !sum_expect->Equals(*out->column(1)) ||
      !s_expect->Equals(*out->column(2))) {
    return arrow::Status::Invalid("unexpected output values");
  }
  return arrow::Status::OK();
}

arrow::Status RunHashAggUnicode0900StringKeySmoke() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, PipelineBuilder::Create(engine.get()));

  std::vector<AggKey> keys = {{"s", MakeFieldRef("s")}};
  std::vector<AggFunc> aggs;
  aggs.push_back({"cnt", "count_all", nullptr});
  aggs.push_back({"sum_v", "sum", MakeFieldRef("v")});

  ARROW_RETURN_NOT_OK(builder->AppendTransform(
      [engine_ptr = engine.get(), keys, aggs]() -> arrow::Result<TransformOpPtr> {
        return std::make_unique<HashAggTransformOp>(engine_ptr, keys, aggs);
      }));

  ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());
  ARROW_ASSIGN_OR_RAISE(auto task, pipeline->CreateTask());

  ARROW_ASSIGN_OR_RAISE(auto initial_state, task->Step());
  if (initial_state != TaskState::kNeedInput) {
    return arrow::Status::Invalid("expected TaskState::kNeedInput");
  }

  ARROW_ASSIGN_OR_RAISE(auto s_field, MakeBinaryFieldWithCollation("s", /*collation_id=*/255));
  auto schema = arrow::schema({s_field, arrow::field("v", arrow::int32())});

  std::vector<std::optional<std::string>> s_values = {std::string("a"), std::string("A"),
                                                      std::string("a "), std::string("A ")};
  ARROW_ASSIGN_OR_RAISE(auto s_array, MakeBinaryArray(s_values));
  ARROW_ASSIGN_OR_RAISE(auto v_array, MakeInt32Array({10, 20, 1, 2}));
  auto batch = arrow::RecordBatch::Make(schema, static_cast<int64_t>(s_values.size()),
                                        {s_array, v_array});

  ARROW_RETURN_NOT_OK(task->PushInput(batch));
  ARROW_RETURN_NOT_OK(task->CloseInput());

  std::vector<std::shared_ptr<arrow::RecordBatch>> outputs;
  while (true) {
    ARROW_ASSIGN_OR_RAISE(auto state, task->Step());
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
  if (out->num_columns() != 3 || out->num_rows() != 2) {
    return arrow::Status::Invalid("unexpected output shape");
  }

  if (out->schema()->field(0)->name() != "cnt" || out->schema()->field(1)->name() != "sum_v" ||
      out->schema()->field(2)->name() != "s") {
    return arrow::Status::Invalid("unexpected output schema");
  }

  ARROW_ASSIGN_OR_RAISE(const auto logical_type, GetLogicalType(*out->schema()->field(2)));
  if (logical_type.id != LogicalTypeId::kString || logical_type.collation_id != 255) {
    return arrow::Status::Invalid("unexpected output string key metadata");
  }

  arrow::BinaryBuilder s_expect_builder;
  ARROW_RETURN_NOT_OK(s_expect_builder.Append(reinterpret_cast<const uint8_t*>("a"), 1));
  ARROW_RETURN_NOT_OK(s_expect_builder.Append(reinterpret_cast<const uint8_t*>("a "), 2));
  std::shared_ptr<arrow::Array> s_expect;
  ARROW_RETURN_NOT_OK(s_expect_builder.Finish(&s_expect));

  arrow::UInt64Builder cnt_builder;
  ARROW_RETURN_NOT_OK(cnt_builder.AppendValues({2, 2}));
  std::shared_ptr<arrow::Array> cnt_expect;
  ARROW_RETURN_NOT_OK(cnt_builder.Finish(&cnt_expect));

  arrow::Int64Builder sum_builder;
  ARROW_RETURN_NOT_OK(sum_builder.AppendValues({30, 3}));
  std::shared_ptr<arrow::Array> sum_expect;
  ARROW_RETURN_NOT_OK(sum_builder.Finish(&sum_expect));

  if (!cnt_expect->Equals(*out->column(0)) || !sum_expect->Equals(*out->column(1)) ||
      !s_expect->Equals(*out->column(2))) {
    return arrow::Status::Invalid("unexpected output values");
  }
  return arrow::Status::OK();
}

arrow::Status RunHashAggCountMinMaxSmoke() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, PipelineBuilder::Create(engine.get()));

  std::vector<AggKey> keys = {{"k", MakeFieldRef("k")}};
  std::vector<AggFunc> aggs;
  aggs.push_back({"cnt_all", "count_all", nullptr});
  aggs.push_back({"cnt_v", "count", MakeFieldRef("v")});
  aggs.push_back({"sum_v", "sum", MakeFieldRef("v")});
  aggs.push_back({"min_v", "min", MakeFieldRef("v")});
  aggs.push_back({"max_v", "max", MakeFieldRef("v")});

  ARROW_RETURN_NOT_OK(builder->AppendTransform(
      [engine_ptr = engine.get(), keys, aggs]() -> arrow::Result<TransformOpPtr> {
        return std::make_unique<HashAggTransformOp>(engine_ptr, keys, aggs);
      }));

  ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());
  ARROW_ASSIGN_OR_RAISE(auto task, pipeline->CreateTask());

  ARROW_ASSIGN_OR_RAISE(auto initial_state, task->Step());
  if (initial_state != TaskState::kNeedInput) {
    return arrow::Status::Invalid("expected TaskState::kNeedInput");
  }

  ARROW_ASSIGN_OR_RAISE(auto batch0, MakeBatch({1, 2, 1, std::nullopt}, {10, 20, std::nullopt, 7}));
  ARROW_RETURN_NOT_OK(task->PushInput(batch0));
  ARROW_ASSIGN_OR_RAISE(auto batch1, MakeBatch({2, 3, std::nullopt, 4}, {1, 5, std::nullopt, std::nullopt}));
  ARROW_RETURN_NOT_OK(task->PushInput(batch1));
  ARROW_RETURN_NOT_OK(task->CloseInput());

  std::vector<std::shared_ptr<arrow::RecordBatch>> outputs;
  while (true) {
    ARROW_ASSIGN_OR_RAISE(auto state, task->Step());
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
  if (out->num_columns() != 6 || out->num_rows() != 5) {
    return arrow::Status::Invalid("unexpected output shape");
  }

  if (out->schema()->field(0)->name() != "cnt_all" || out->schema()->field(1)->name() != "cnt_v" ||
      out->schema()->field(2)->name() != "sum_v" || out->schema()->field(3)->name() != "min_v" ||
      out->schema()->field(4)->name() != "max_v" || out->schema()->field(5)->name() != "k") {
    return arrow::Status::Invalid("unexpected output schema");
  }

  ARROW_ASSIGN_OR_RAISE(auto expect_k, MakeInt32Array({1, 2, std::nullopt, 3, 4}));

  arrow::UInt64Builder cnt_all_builder;
  ARROW_RETURN_NOT_OK(cnt_all_builder.AppendValues({2, 2, 2, 1, 1}));
  std::shared_ptr<arrow::Array> expect_cnt_all;
  ARROW_RETURN_NOT_OK(cnt_all_builder.Finish(&expect_cnt_all));

  arrow::UInt64Builder cnt_v_builder;
  ARROW_RETURN_NOT_OK(cnt_v_builder.AppendValues({1, 2, 1, 1, 0}));
  std::shared_ptr<arrow::Array> expect_cnt_v;
  ARROW_RETURN_NOT_OK(cnt_v_builder.Finish(&expect_cnt_v));

  arrow::Int64Builder sum_builder;
  ARROW_RETURN_NOT_OK(sum_builder.AppendValues({10, 21, 7, 5}));
  ARROW_RETURN_NOT_OK(sum_builder.AppendNull());
  std::shared_ptr<arrow::Array> expect_sum;
  ARROW_RETURN_NOT_OK(sum_builder.Finish(&expect_sum));

  arrow::Int32Builder min_builder;
  ARROW_RETURN_NOT_OK(min_builder.AppendValues({10, 1, 7, 5}));
  ARROW_RETURN_NOT_OK(min_builder.AppendNull());
  std::shared_ptr<arrow::Array> expect_min;
  ARROW_RETURN_NOT_OK(min_builder.Finish(&expect_min));

  arrow::Int32Builder max_builder;
  ARROW_RETURN_NOT_OK(max_builder.AppendValues({10, 20, 7, 5}));
  ARROW_RETURN_NOT_OK(max_builder.AppendNull());
  std::shared_ptr<arrow::Array> expect_max;
  ARROW_RETURN_NOT_OK(max_builder.Finish(&expect_max));

  if (!expect_cnt_all->Equals(*out->column(0)) || !expect_cnt_v->Equals(*out->column(1)) ||
      !expect_sum->Equals(*out->column(2)) || !expect_min->Equals(*out->column(3)) ||
      !expect_max->Equals(*out->column(4)) || !expect_k->Equals(*out->column(5))) {
    return arrow::Status::Invalid("unexpected output values");
  }

  return arrow::Status::OK();
}

arrow::Status RunHashAggGlobalEmptyInputSmoke() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, PipelineBuilder::Create(engine.get()));

  std::vector<AggKey> keys;
  std::vector<AggFunc> aggs;
  aggs.push_back({"cnt_all", "count_all", nullptr});
  aggs.push_back({"sum_v", "sum", MakeFieldRef("v")});
  aggs.push_back({"min_v", "min", MakeFieldRef("v")});
  aggs.push_back({"max_v", "max", MakeFieldRef("v")});

  ARROW_RETURN_NOT_OK(builder->AppendTransform(
      [engine_ptr = engine.get(), keys, aggs]() -> arrow::Result<TransformOpPtr> {
        return std::make_unique<HashAggTransformOp>(engine_ptr, keys, aggs);
      }));

  ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());
  ARROW_ASSIGN_OR_RAISE(auto task, pipeline->CreateTask());

  ARROW_ASSIGN_OR_RAISE(auto initial_state, task->Step());
  if (initial_state != TaskState::kNeedInput) {
    return arrow::Status::Invalid("expected TaskState::kNeedInput");
  }

  ARROW_ASSIGN_OR_RAISE(auto empty_input, MakeBatch({}, {}));
  ARROW_RETURN_NOT_OK(task->PushInput(empty_input));
  ARROW_RETURN_NOT_OK(task->CloseInput());

  std::vector<std::shared_ptr<arrow::RecordBatch>> outputs;
  while (true) {
    ARROW_ASSIGN_OR_RAISE(auto state, task->Step());
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

arrow::Status RunHashAggAvgSmoke() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, PipelineBuilder::Create(engine.get()));

  std::vector<AggKey> keys = {{"k", MakeFieldRef("k")}};
  std::vector<AggFunc> aggs;
  aggs.push_back({"avg_v", "avg", MakeFieldRef("v")});

  ARROW_RETURN_NOT_OK(builder->AppendTransform(
      [engine_ptr = engine.get(), keys, aggs]() -> arrow::Result<TransformOpPtr> {
        return std::make_unique<HashAggTransformOp>(engine_ptr, keys, aggs);
      }));

  ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());
  ARROW_ASSIGN_OR_RAISE(auto task, pipeline->CreateTask());

  ARROW_ASSIGN_OR_RAISE(auto initial_state, task->Step());
  if (initial_state != TaskState::kNeedInput) {
    return arrow::Status::Invalid("expected TaskState::kNeedInput");
  }

  ARROW_ASSIGN_OR_RAISE(auto batch,
                        MakeBatch({1, 1, 2, 2, std::nullopt, 3}, {10, std::nullopt, 20, 0, 7, std::nullopt}));
  ARROW_RETURN_NOT_OK(task->PushInput(batch));
  ARROW_RETURN_NOT_OK(task->CloseInput());

  std::vector<std::shared_ptr<arrow::RecordBatch>> outputs;
  while (true) {
    ARROW_ASSIGN_OR_RAISE(auto state, task->Step());
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
  if (out->num_columns() != 2 || out->num_rows() != 4) {
    return arrow::Status::Invalid("unexpected output shape");
  }
  if (out->schema()->field(0)->name() != "avg_v" || out->schema()->field(1)->name() != "k") {
    return arrow::Status::Invalid("unexpected output schema");
  }

  arrow::DoubleBuilder avg_builder;
  ARROW_RETURN_NOT_OK(avg_builder.AppendValues({10.0, 10.0, 7.0}));
  ARROW_RETURN_NOT_OK(avg_builder.AppendNull());
  std::shared_ptr<arrow::Array> expect_avg;
  ARROW_RETURN_NOT_OK(avg_builder.Finish(&expect_avg));

  ARROW_ASSIGN_OR_RAISE(auto expect_k, MakeInt32Array({1, 2, std::nullopt, 3}));

  if (!expect_avg->Equals(*out->column(0)) || !expect_k->Equals(*out->column(1))) {
    return arrow::Status::Invalid("unexpected output values");
  }
  return arrow::Status::OK();
}

arrow::Status RunHashAggSumDoubleSmoke() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, PipelineBuilder::Create(engine.get()));

  std::vector<AggKey> keys = {{"k", MakeFieldRef("k")}};
  std::vector<AggFunc> aggs;
  aggs.push_back({"sum_v", "sum", MakeFieldRef("v")});

  ARROW_RETURN_NOT_OK(builder->AppendTransform(
      [engine_ptr = engine.get(), keys, aggs]() -> arrow::Result<TransformOpPtr> {
        return std::make_unique<HashAggTransformOp>(engine_ptr, keys, aggs);
      }));

  ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());
  ARROW_ASSIGN_OR_RAISE(auto task, pipeline->CreateTask());

  ARROW_ASSIGN_OR_RAISE(auto initial_state, task->Step());
  if (initial_state != TaskState::kNeedInput) {
    return arrow::Status::Invalid("expected TaskState::kNeedInput");
  }

  auto schema = arrow::schema({arrow::field("k", arrow::int32()), arrow::field("v", arrow::float64())});
  ARROW_ASSIGN_OR_RAISE(auto k_array, MakeInt32Array({1, 1, 2, 3, 3}));
  ARROW_ASSIGN_OR_RAISE(auto v_array, MakeDoubleArray({1.5, std::nullopt, 2.25, std::nullopt, std::nullopt}));
  auto batch = arrow::RecordBatch::Make(schema, k_array->length(), {k_array, v_array});

  ARROW_RETURN_NOT_OK(task->PushInput(batch));
  ARROW_RETURN_NOT_OK(task->CloseInput());

  std::vector<std::shared_ptr<arrow::RecordBatch>> outputs;
  while (true) {
    ARROW_ASSIGN_OR_RAISE(auto state, task->Step());
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
  if (out->num_columns() != 2 || out->num_rows() != 3) {
    return arrow::Status::Invalid("unexpected output shape");
  }
  if (out->schema()->field(0)->name() != "sum_v" || out->schema()->field(1)->name() != "k") {
    return arrow::Status::Invalid("unexpected output schema");
  }

  ARROW_ASSIGN_OR_RAISE(auto expect_k, MakeInt32Array({1, 2, 3}));

  arrow::DoubleBuilder sum_builder;
  ARROW_RETURN_NOT_OK(sum_builder.Append(1.5));
  ARROW_RETURN_NOT_OK(sum_builder.Append(2.25));
  ARROW_RETURN_NOT_OK(sum_builder.AppendNull());
  std::shared_ptr<arrow::Array> expect_sum;
  ARROW_RETURN_NOT_OK(sum_builder.Finish(&expect_sum));

  if (!expect_sum->Equals(*out->column(0)) || !expect_k->Equals(*out->column(1))) {
    return arrow::Status::Invalid("unexpected output values");
  }
  return arrow::Status::OK();
}

arrow::Status RunHashAggSumFloatSmoke() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, PipelineBuilder::Create(engine.get()));

  std::vector<AggKey> keys = {{"k", MakeFieldRef("k")}};
  std::vector<AggFunc> aggs;
  aggs.push_back({"sum_v", "sum", MakeFieldRef("v")});

  ARROW_RETURN_NOT_OK(builder->AppendTransform(
      [engine_ptr = engine.get(), keys, aggs]() -> arrow::Result<TransformOpPtr> {
        return std::make_unique<HashAggTransformOp>(engine_ptr, keys, aggs);
      }));

  ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());
  ARROW_ASSIGN_OR_RAISE(auto task, pipeline->CreateTask());

  ARROW_ASSIGN_OR_RAISE(auto initial_state, task->Step());
  if (initial_state != TaskState::kNeedInput) {
    return arrow::Status::Invalid("expected TaskState::kNeedInput");
  }

  auto schema = arrow::schema({arrow::field("k", arrow::int32()), arrow::field("v", arrow::float32())});
  ARROW_ASSIGN_OR_RAISE(auto k_array, MakeInt32Array({1, 1, 2, 3, 3}));
  ARROW_ASSIGN_OR_RAISE(auto v_array, MakeFloatArray({1.5F, std::nullopt, 2.25F, std::nullopt, std::nullopt}));
  auto batch = arrow::RecordBatch::Make(schema, k_array->length(), {k_array, v_array});

  ARROW_RETURN_NOT_OK(task->PushInput(batch));
  ARROW_RETURN_NOT_OK(task->CloseInput());

  std::vector<std::shared_ptr<arrow::RecordBatch>> outputs;
  while (true) {
    ARROW_ASSIGN_OR_RAISE(auto state, task->Step());
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
  if (out->num_columns() != 2 || out->num_rows() != 3) {
    return arrow::Status::Invalid("unexpected output shape");
  }
  if (out->schema()->field(0)->name() != "sum_v" || out->schema()->field(1)->name() != "k") {
    return arrow::Status::Invalid("unexpected output schema");
  }

  ARROW_ASSIGN_OR_RAISE(auto expect_k, MakeInt32Array({1, 2, 3}));

  arrow::DoubleBuilder sum_builder;
  ARROW_RETURN_NOT_OK(sum_builder.Append(1.5));
  ARROW_RETURN_NOT_OK(sum_builder.Append(2.25));
  ARROW_RETURN_NOT_OK(sum_builder.AppendNull());
  std::shared_ptr<arrow::Array> expect_sum;
  ARROW_RETURN_NOT_OK(sum_builder.Finish(&expect_sum));

  if (!expect_sum->Equals(*out->column(0)) || !expect_k->Equals(*out->column(1))) {
    return arrow::Status::Invalid("unexpected output values");
  }
  return arrow::Status::OK();
}

arrow::Status RunHashAggSumDecimal128Smoke() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, PipelineBuilder::Create(engine.get()));

  std::vector<AggKey> keys = {{"k", MakeFieldRef("k")}};
  std::vector<AggFunc> aggs;
  aggs.push_back({"sum_v", "sum", MakeFieldRef("v")});

  ARROW_RETURN_NOT_OK(builder->AppendTransform(
      [engine_ptr = engine.get(), keys, aggs]() -> arrow::Result<TransformOpPtr> {
        return std::make_unique<HashAggTransformOp>(engine_ptr, keys, aggs);
      }));

  ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());
  ARROW_ASSIGN_OR_RAISE(auto task, pipeline->CreateTask());

  ARROW_ASSIGN_OR_RAISE(auto initial_state, task->Step());
  if (initial_state != TaskState::kNeedInput) {
    return arrow::Status::Invalid("expected TaskState::kNeedInput");
  }

  constexpr int32_t kPrecision = 10;
  constexpr int32_t kScale = 2;
  auto schema = arrow::schema({arrow::field("k", arrow::int32()),
                               arrow::field("v", arrow::decimal128(kPrecision, kScale))});
  ARROW_ASSIGN_OR_RAISE(auto k_array, MakeInt32Array({1, 1, 2, std::nullopt}));
  ARROW_ASSIGN_OR_RAISE(auto v_array, MakeDecimal128Array(kPrecision, kScale, {123, std::nullopt, 200, -50}));
  auto batch = arrow::RecordBatch::Make(schema, k_array->length(), {k_array, v_array});

  ARROW_RETURN_NOT_OK(task->PushInput(batch));
  ARROW_RETURN_NOT_OK(task->CloseInput());

  std::vector<std::shared_ptr<arrow::RecordBatch>> outputs;
  while (true) {
    ARROW_ASSIGN_OR_RAISE(auto state, task->Step());
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
  if (out->num_columns() != 2 || out->num_rows() != 3) {
    return arrow::Status::Invalid("unexpected output shape");
  }
  if (out->schema()->field(0)->name() != "sum_v" || out->schema()->field(1)->name() != "k") {
    return arrow::Status::Invalid("unexpected output schema");
  }

  constexpr int32_t kOutPrecision = 32;  // min(kPrecision + 22, 65)
  ARROW_ASSIGN_OR_RAISE(auto expect_k, MakeInt32Array({1, 2, std::nullopt}));
  ARROW_ASSIGN_OR_RAISE(auto expect_sum,
                        MakeDecimal128Array(kOutPrecision, kScale, {123, 200, -50}));

  if (!expect_sum->Equals(*out->column(0)) || !expect_k->Equals(*out->column(1))) {
    return arrow::Status::Invalid("unexpected output values");
  }
  return arrow::Status::OK();
}

arrow::Status RunHashAggSumDecimal256Smoke() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, PipelineBuilder::Create(engine.get()));

  std::vector<AggKey> keys = {{"k", MakeFieldRef("k")}};
  std::vector<AggFunc> aggs;
  aggs.push_back({"sum_v", "sum", MakeFieldRef("v")});

  ARROW_RETURN_NOT_OK(builder->AppendTransform(
      [engine_ptr = engine.get(), keys, aggs]() -> arrow::Result<TransformOpPtr> {
        return std::make_unique<HashAggTransformOp>(engine_ptr, keys, aggs);
      }));

  ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());
  ARROW_ASSIGN_OR_RAISE(auto task, pipeline->CreateTask());

  ARROW_ASSIGN_OR_RAISE(auto initial_state, task->Step());
  if (initial_state != TaskState::kNeedInput) {
    return arrow::Status::Invalid("expected TaskState::kNeedInput");
  }

  constexpr int32_t kPrecision = 20;
  constexpr int32_t kScale = 2;
  auto schema = arrow::schema({arrow::field("k", arrow::int32()),
                               arrow::field("v", arrow::decimal128(kPrecision, kScale))});
  ARROW_ASSIGN_OR_RAISE(auto k_array, MakeInt32Array({1, 1, 2, std::nullopt}));
  ARROW_ASSIGN_OR_RAISE(auto v_array,
                        MakeDecimal128Array(kPrecision, kScale, {123, std::nullopt, 200, -50}));
  auto batch = arrow::RecordBatch::Make(schema, k_array->length(), {k_array, v_array});

  ARROW_RETURN_NOT_OK(task->PushInput(batch));
  ARROW_RETURN_NOT_OK(task->CloseInput());

  std::vector<std::shared_ptr<arrow::RecordBatch>> outputs;
  while (true) {
    ARROW_ASSIGN_OR_RAISE(auto state, task->Step());
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
  if (out->num_columns() != 2 || out->num_rows() != 3) {
    return arrow::Status::Invalid("unexpected output shape");
  }
  if (out->schema()->field(0)->name() != "sum_v" || out->schema()->field(1)->name() != "k") {
    return arrow::Status::Invalid("unexpected output schema");
  }

  constexpr int32_t kOutPrecision = 42;  // min(kPrecision + 22, 65)
  ARROW_ASSIGN_OR_RAISE(auto expect_k, MakeInt32Array({1, 2, std::nullopt}));
  ARROW_ASSIGN_OR_RAISE(auto expect_sum,
                        MakeDecimal256Array(kOutPrecision, kScale, {123, 200, -50}));

  if (!expect_sum->Equals(*out->column(0)) || !expect_k->Equals(*out->column(1))) {
    return arrow::Status::Invalid("unexpected output values");
  }
  return arrow::Status::OK();
}

arrow::Status RunHashAggAvgDecimalSmoke() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, PipelineBuilder::Create(engine.get()));

  std::vector<AggKey> keys = {{"k", MakeFieldRef("k")}};
  std::vector<AggFunc> aggs;
  aggs.push_back({"avg_v", "avg", MakeFieldRef("v")});

  ARROW_RETURN_NOT_OK(builder->AppendTransform(
      [engine_ptr = engine.get(), keys, aggs]() -> arrow::Result<TransformOpPtr> {
        return std::make_unique<HashAggTransformOp>(engine_ptr, keys, aggs);
      }));

  ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());
  ARROW_ASSIGN_OR_RAISE(auto task, pipeline->CreateTask());

  ARROW_ASSIGN_OR_RAISE(auto initial_state, task->Step());
  if (initial_state != TaskState::kNeedInput) {
    return arrow::Status::Invalid("expected TaskState::kNeedInput");
  }

  constexpr int32_t kPrecision = 10;
  constexpr int32_t kScale = 2;
  auto schema = arrow::schema({arrow::field("k", arrow::int32()),
                               arrow::field("v", arrow::decimal128(kPrecision, kScale))});
  ARROW_ASSIGN_OR_RAISE(auto k_array, MakeInt32Array({1, 1, 2, 2, 2, 3}));
  ARROW_ASSIGN_OR_RAISE(
      auto v_array,
      MakeDecimal128Array(kPrecision, kScale, {100, 200, 100, 200, 200, std::nullopt}));
  auto batch = arrow::RecordBatch::Make(schema, k_array->length(), {k_array, v_array});

  ARROW_RETURN_NOT_OK(task->PushInput(batch));
  ARROW_RETURN_NOT_OK(task->CloseInput());

  std::vector<std::shared_ptr<arrow::RecordBatch>> outputs;
  while (true) {
    ARROW_ASSIGN_OR_RAISE(auto state, task->Step());
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
  if (out->num_columns() != 2 || out->num_rows() != 3) {
    return arrow::Status::Invalid("unexpected output shape");
  }
  if (out->schema()->field(0)->name() != "avg_v" || out->schema()->field(1)->name() != "k") {
    return arrow::Status::Invalid("unexpected output schema");
  }

  constexpr int32_t kOutPrecision = 14;  // min(kPrecision + 4, 65)
  constexpr int32_t kOutScale = 6;       // min(kScale + 4, 30)
  ARROW_ASSIGN_OR_RAISE(auto expect_k, MakeInt32Array({1, 2, 3}));
  ARROW_ASSIGN_OR_RAISE(
      auto expect_avg,
      MakeDecimal128Array(kOutPrecision, kOutScale, {1500000, 1666666, std::nullopt}));

  if (!expect_avg->Equals(*out->column(0)) || !expect_k->Equals(*out->column(1))) {
    return arrow::Status::Invalid("unexpected output values");
  }
  return arrow::Status::OK();
}

arrow::Status RunHashAggMinMaxFloatSmoke() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, PipelineBuilder::Create(engine.get()));

  std::vector<AggKey> keys = {{"k", MakeFieldRef("k")}};
  std::vector<AggFunc> aggs;
  aggs.push_back({"min_v", "min", MakeFieldRef("v")});
  aggs.push_back({"max_v", "max", MakeFieldRef("v")});

  ARROW_RETURN_NOT_OK(builder->AppendTransform(
      [engine_ptr = engine.get(), keys, aggs]() -> arrow::Result<TransformOpPtr> {
        return std::make_unique<HashAggTransformOp>(engine_ptr, keys, aggs);
      }));

  ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());
  ARROW_ASSIGN_OR_RAISE(auto task, pipeline->CreateTask());

  ARROW_ASSIGN_OR_RAISE(auto initial_state, task->Step());
  if (initial_state != TaskState::kNeedInput) {
    return arrow::Status::Invalid("expected TaskState::kNeedInput");
  }

  const float nan = std::numeric_limits<float>::quiet_NaN();
  auto schema = arrow::schema({arrow::field("k", arrow::int32()), arrow::field("v", arrow::float32())});
  ARROW_ASSIGN_OR_RAISE(auto k_array, MakeInt32Array({1, 1, 1, 2, 2}));
  ARROW_ASSIGN_OR_RAISE(auto v_array, MakeFloatArray({nan, 3.0F, 1.0F, std::nullopt, std::nullopt}));
  auto batch = arrow::RecordBatch::Make(schema, k_array->length(), {k_array, v_array});

  ARROW_RETURN_NOT_OK(task->PushInput(batch));
  ARROW_RETURN_NOT_OK(task->CloseInput());

  std::vector<std::shared_ptr<arrow::RecordBatch>> outputs;
  while (true) {
    ARROW_ASSIGN_OR_RAISE(auto state, task->Step());
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
  if (out->num_columns() != 3 || out->num_rows() != 2) {
    return arrow::Status::Invalid("unexpected output shape");
  }
  if (out->schema()->field(0)->name() != "min_v" || out->schema()->field(1)->name() != "max_v" ||
      out->schema()->field(2)->name() != "k") {
    return arrow::Status::Invalid("unexpected output schema");
  }

  ARROW_ASSIGN_OR_RAISE(auto expect_k, MakeInt32Array({1, 2}));
  if (!expect_k->Equals(*out->column(2))) {
    return arrow::Status::Invalid("unexpected group key output");
  }

  const auto& min_arr = static_cast<const arrow::FloatArray&>(*out->column(0));
  const auto& max_arr = static_cast<const arrow::FloatArray&>(*out->column(1));
  if (min_arr.IsNull(0) || max_arr.IsNull(0)) {
    return arrow::Status::Invalid("expected non-null min/max for group 1");
  }
  if (!std::isnan(min_arr.Value(0)) || !std::isnan(max_arr.Value(0))) {
    return arrow::Status::Invalid("expected NaN min/max for group 1 (TiFlash semantics)");
  }
  if (!min_arr.IsNull(1) || !max_arr.IsNull(1)) {
    return arrow::Status::Invalid("expected null min/max for group 2");
  }
  return arrow::Status::OK();
}

arrow::Status RunHashAggMinMaxDecimal128Smoke() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, PipelineBuilder::Create(engine.get()));

  std::vector<AggKey> keys = {{"k", MakeFieldRef("k")}};
  std::vector<AggFunc> aggs;
  aggs.push_back({"min_v", "min", MakeFieldRef("v")});
  aggs.push_back({"max_v", "max", MakeFieldRef("v")});

  ARROW_RETURN_NOT_OK(builder->AppendTransform(
      [engine_ptr = engine.get(), keys, aggs]() -> arrow::Result<TransformOpPtr> {
        return std::make_unique<HashAggTransformOp>(engine_ptr, keys, aggs);
      }));

  ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());
  ARROW_ASSIGN_OR_RAISE(auto task, pipeline->CreateTask());

  ARROW_ASSIGN_OR_RAISE(auto initial_state, task->Step());
  if (initial_state != TaskState::kNeedInput) {
    return arrow::Status::Invalid("expected TaskState::kNeedInput");
  }

  constexpr int32_t kPrecision = 10;
  constexpr int32_t kScale = 2;
  auto schema = arrow::schema({arrow::field("k", arrow::int32()),
                               arrow::field("v", arrow::decimal128(kPrecision, kScale))});
  ARROW_ASSIGN_OR_RAISE(auto k_array, MakeInt32Array({1, 1, 2, std::nullopt}));
  ARROW_ASSIGN_OR_RAISE(auto v_array, MakeDecimal128Array(kPrecision, kScale, {123, -50, std::nullopt, 200}));
  auto batch = arrow::RecordBatch::Make(schema, k_array->length(), {k_array, v_array});

  ARROW_RETURN_NOT_OK(task->PushInput(batch));
  ARROW_RETURN_NOT_OK(task->CloseInput());

  std::vector<std::shared_ptr<arrow::RecordBatch>> outputs;
  while (true) {
    ARROW_ASSIGN_OR_RAISE(auto state, task->Step());
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
  if (out->num_columns() != 3 || out->num_rows() != 3) {
    return arrow::Status::Invalid("unexpected output shape");
  }
  if (out->schema()->field(0)->name() != "min_v" || out->schema()->field(1)->name() != "max_v" ||
      out->schema()->field(2)->name() != "k") {
    return arrow::Status::Invalid("unexpected output schema");
  }

  ARROW_ASSIGN_OR_RAISE(auto expect_k, MakeInt32Array({1, 2, std::nullopt}));
  ARROW_ASSIGN_OR_RAISE(auto expect_min, MakeDecimal128Array(kPrecision, kScale, {-50, std::nullopt, 200}));
  ARROW_ASSIGN_OR_RAISE(auto expect_max, MakeDecimal128Array(kPrecision, kScale, {123, std::nullopt, 200}));

  if (!expect_min->Equals(*out->column(0)) || !expect_max->Equals(*out->column(1)) ||
      !expect_k->Equals(*out->column(2))) {
    return arrow::Status::Invalid("unexpected output values");
  }
  return arrow::Status::OK();
}

arrow::Status RunHashAggMinMaxDecimal256Smoke() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, PipelineBuilder::Create(engine.get()));

  std::vector<AggKey> keys = {{"k", MakeFieldRef("k")}};
  std::vector<AggFunc> aggs;
  aggs.push_back({"min_v", "min", MakeFieldRef("v")});
  aggs.push_back({"max_v", "max", MakeFieldRef("v")});

  ARROW_RETURN_NOT_OK(builder->AppendTransform(
      [engine_ptr = engine.get(), keys, aggs]() -> arrow::Result<TransformOpPtr> {
        return std::make_unique<HashAggTransformOp>(engine_ptr, keys, aggs);
      }));

  ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());
  ARROW_ASSIGN_OR_RAISE(auto task, pipeline->CreateTask());

  ARROW_ASSIGN_OR_RAISE(auto initial_state, task->Step());
  if (initial_state != TaskState::kNeedInput) {
    return arrow::Status::Invalid("expected TaskState::kNeedInput");
  }

  constexpr int32_t kPrecision = 50;
  constexpr int32_t kScale = 2;
  auto schema = arrow::schema({arrow::field("k", arrow::int32()),
                               arrow::field("v", arrow::decimal256(kPrecision, kScale))});
  ARROW_ASSIGN_OR_RAISE(auto k_array, MakeInt32Array({1, 1, 2, std::nullopt}));
  ARROW_ASSIGN_OR_RAISE(
      auto v_array,
      MakeDecimal256Array(kPrecision, kScale, {123, -50, std::nullopt, 200}));
  auto batch = arrow::RecordBatch::Make(schema, k_array->length(), {k_array, v_array});

  ARROW_RETURN_NOT_OK(task->PushInput(batch));
  ARROW_RETURN_NOT_OK(task->CloseInput());

  std::vector<std::shared_ptr<arrow::RecordBatch>> outputs;
  while (true) {
    ARROW_ASSIGN_OR_RAISE(auto state, task->Step());
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
  if (out->num_columns() != 3 || out->num_rows() != 3) {
    return arrow::Status::Invalid("unexpected output shape");
  }
  if (out->schema()->field(0)->name() != "min_v" || out->schema()->field(1)->name() != "max_v" ||
      out->schema()->field(2)->name() != "k") {
    return arrow::Status::Invalid("unexpected output schema");
  }

  ARROW_ASSIGN_OR_RAISE(auto expect_k, MakeInt32Array({1, 2, std::nullopt}));
  ARROW_ASSIGN_OR_RAISE(auto expect_min,
                        MakeDecimal256Array(kPrecision, kScale, {-50, std::nullopt, 200}));
  ARROW_ASSIGN_OR_RAISE(auto expect_max,
                        MakeDecimal256Array(kPrecision, kScale, {123, std::nullopt, 200}));

  if (!expect_min->Equals(*out->column(0)) || !expect_max->Equals(*out->column(1)) ||
      !expect_k->Equals(*out->column(2))) {
    return arrow::Status::Invalid("unexpected output values");
  }
  return arrow::Status::OK();
}

arrow::Status RunHashAggMinMaxDoubleSmoke() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, PipelineBuilder::Create(engine.get()));

  std::vector<AggKey> keys = {{"k", MakeFieldRef("k")}};
  std::vector<AggFunc> aggs;
  aggs.push_back({"min_v", "min", MakeFieldRef("v")});
  aggs.push_back({"max_v", "max", MakeFieldRef("v")});

  ARROW_RETURN_NOT_OK(builder->AppendTransform(
      [engine_ptr = engine.get(), keys, aggs]() -> arrow::Result<TransformOpPtr> {
        return std::make_unique<HashAggTransformOp>(engine_ptr, keys, aggs);
      }));

  ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());
  ARROW_ASSIGN_OR_RAISE(auto task, pipeline->CreateTask());

  ARROW_ASSIGN_OR_RAISE(auto initial_state, task->Step());
  if (initial_state != TaskState::kNeedInput) {
    return arrow::Status::Invalid("expected TaskState::kNeedInput");
  }

  const double nan = std::numeric_limits<double>::quiet_NaN();
  auto schema = arrow::schema({arrow::field("k", arrow::int32()), arrow::field("v", arrow::float64())});
  ARROW_ASSIGN_OR_RAISE(auto k_array, MakeInt32Array({1, 1, 1, 2, 2}));
  ARROW_ASSIGN_OR_RAISE(auto v_array, MakeDoubleArray({nan, 3.0, 1.0, std::nullopt, std::nullopt}));
  auto batch = arrow::RecordBatch::Make(schema, k_array->length(), {k_array, v_array});

  ARROW_RETURN_NOT_OK(task->PushInput(batch));
  ARROW_RETURN_NOT_OK(task->CloseInput());

  std::vector<std::shared_ptr<arrow::RecordBatch>> outputs;
  while (true) {
    ARROW_ASSIGN_OR_RAISE(auto state, task->Step());
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
  if (out->num_columns() != 3 || out->num_rows() != 2) {
    return arrow::Status::Invalid("unexpected output shape");
  }
  if (out->schema()->field(0)->name() != "min_v" || out->schema()->field(1)->name() != "max_v" ||
      out->schema()->field(2)->name() != "k") {
    return arrow::Status::Invalid("unexpected output schema");
  }

  ARROW_ASSIGN_OR_RAISE(auto expect_k, MakeInt32Array({1, 2}));
  if (!expect_k->Equals(*out->column(2))) {
    return arrow::Status::Invalid("unexpected group key output");
  }

  const auto& min_arr = static_cast<const arrow::DoubleArray&>(*out->column(0));
  const auto& max_arr = static_cast<const arrow::DoubleArray&>(*out->column(1));
  if (min_arr.IsNull(0) || max_arr.IsNull(0)) {
    return arrow::Status::Invalid("expected non-null min/max for group 1");
  }
  if (!std::isnan(min_arr.Value(0)) || !std::isnan(max_arr.Value(0))) {
    return arrow::Status::Invalid("expected NaN min/max for group 1 (TiFlash semantics)");
  }
  if (!min_arr.IsNull(1) || !max_arr.IsNull(1)) {
    return arrow::Status::Invalid("expected null min/max for group 2");
  }
  return arrow::Status::OK();
}

}  // namespace

TEST(TiForthHashAggTest, CountAllSum) {
  auto status = RunHashAggSmoke();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthHashAggTest, CountAllCountSumMinMax) {
  auto status = RunHashAggCountMinMaxSmoke();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthHashAggTest, GlobalAggEmptyInput) {
  auto status = RunHashAggGlobalEmptyInputSmoke();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthHashAggTest, TwoKeyGroupByBinaryAndInt32) {
  auto status = RunHashAggTwoKeySmoke();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthHashAggTest, GeneralCiStringKey) {
  auto status = RunHashAggGeneralCiStringKeySmoke();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthHashAggTest, Unicode0900StringKey) {
  auto status = RunHashAggUnicode0900StringKeySmoke();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthHashAggTest, Avg) {
  auto status = RunHashAggAvgSmoke();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthHashAggTest, AvgDecimal) {
  auto status = RunHashAggAvgDecimalSmoke();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthHashAggTest, SumDouble) {
  auto status = RunHashAggSumDoubleSmoke();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthHashAggTest, SumFloat) {
  auto status = RunHashAggSumFloatSmoke();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthHashAggTest, SumDecimal128) {
  auto status = RunHashAggSumDecimal128Smoke();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthHashAggTest, SumDecimal256) {
  auto status = RunHashAggSumDecimal256Smoke();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthHashAggTest, MinMaxFloat) {
  auto status = RunHashAggMinMaxFloatSmoke();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthHashAggTest, MinMaxDecimal128) {
  auto status = RunHashAggMinMaxDecimal128Smoke();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthHashAggTest, MinMaxDecimal256) {
  auto status = RunHashAggMinMaxDecimal256Smoke();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthHashAggTest, MinMaxDouble) {
  auto status = RunHashAggMinMaxDoubleSmoke();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

}  // namespace tiforth
