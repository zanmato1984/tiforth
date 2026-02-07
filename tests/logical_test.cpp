// Copyright 2026 TiForth Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/compute/exec.h>
#include <arrow/result.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>
#include <arrow/testing/gtest_util.h>
#include <arrow/type.h>

#include <gtest/gtest.h>

#include <cmath>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "tiforth/engine.h"
#include "tiforth/expr.h"

namespace tiforth {

namespace {

template <typename BuilderT, typename CppT>
arrow::Result<std::shared_ptr<arrow::Array>> MakePrimitiveArray(
    const std::vector<std::optional<CppT>>& values) {
  BuilderT builder;
  for (const auto& v : values) {
    if (v.has_value()) {
      ARROW_RETURN_NOT_OK(builder.Append(*v));
    } else {
      ARROW_RETURN_NOT_OK(builder.AppendNull());
    }
  }
  std::shared_ptr<arrow::Array> out;
  ARROW_RETURN_NOT_OK(builder.Finish(&out));
  return out;
}

arrow::Result<std::shared_ptr<arrow::Array>> EvalExprOnBatch(
    const Engine& engine, const arrow::RecordBatch& batch, const Expr& expr) {
  arrow::compute::ExecContext ctx(engine.memory_pool(), /*executor=*/nullptr,
                                  engine.function_registry());
  return EvalExprAsArray(batch, expr, &engine, &ctx);
}

arrow::Result<std::shared_ptr<arrow::Array>> EvalVarArgsAllFields(
    const Engine& engine, std::string_view function_name,
    const std::vector<std::shared_ptr<arrow::Array>>& args) {
  if (args.size() < 2) {
    return arrow::Status::Invalid("expected at least 2 args");
  }

  std::vector<std::shared_ptr<arrow::Field>> fields;
  fields.reserve(args.size());
  std::vector<std::shared_ptr<arrow::Array>> columns;
  columns.reserve(args.size());
  std::vector<ExprPtr> call_args;
  call_args.reserve(args.size());

  const int64_t rows = args[0] != nullptr ? args[0]->length() : 0;
  for (std::size_t i = 0; i < args.size(); ++i) {
    if (args[i] == nullptr) {
      return arrow::Status::Invalid("arg must not be null");
    }
    if (args[i]->length() != rows) {
      return arrow::Status::Invalid("arg length mismatch");
    }

    std::string name = "arg" + std::to_string(i);
    fields.push_back(arrow::field(name, args[i]->type()));
    columns.push_back(args[i]);
    call_args.push_back(MakeFieldRef(name));
  }

  auto batch = arrow::RecordBatch::Make(arrow::schema(fields), rows, std::move(columns));
  if (batch == nullptr) {
    return arrow::Status::Invalid("failed to build batch");
  }

  auto expr = MakeCall(std::string(function_name), std::move(call_args));
  return EvalExprOnBatch(engine, *batch, *expr);
}

}  // namespace

TEST(TiForthLogicalTest, AndOrXorNotBasicNullable) {
  ASSERT_OK_AND_ASSIGN(auto engine, Engine::Create(EngineOptions{}));

  ASSERT_OK_AND_ASSIGN(
      auto a, (MakePrimitiveArray<arrow::Int32Builder, int32_t>(
                  {int32_t{1}, int32_t{0}, std::nullopt, int32_t{2}})));
  ASSERT_OK_AND_ASSIGN(
      auto b, (MakePrimitiveArray<arrow::Int32Builder, int32_t>(
                  {int32_t{1}, int32_t{1}, int32_t{0}, std::nullopt})));

  {
    ASSERT_OK_AND_ASSIGN(auto out, EvalVarArgsAllFields(*engine, "and", {a, b}));
    ASSERT_OK_AND_ASSIGN(
        auto expected,
        (MakePrimitiveArray<arrow::BooleanBuilder, bool>({true, false, false, std::nullopt})));
    ASSERT_TRUE(expected->Equals(*out));
  }
  {
    ASSERT_OK_AND_ASSIGN(auto out, EvalVarArgsAllFields(*engine, "or", {a, b}));
    ASSERT_OK_AND_ASSIGN(
        auto expected,
        (MakePrimitiveArray<arrow::BooleanBuilder, bool>({true, true, std::nullopt, true})));
    ASSERT_TRUE(expected->Equals(*out));
  }
  {
    ASSERT_OK_AND_ASSIGN(auto out, EvalVarArgsAllFields(*engine, "xor", {a, b}));
    ASSERT_OK_AND_ASSIGN(
        auto expected,
        (MakePrimitiveArray<arrow::BooleanBuilder, bool>({false, true, std::nullopt, std::nullopt})));
    ASSERT_TRUE(expected->Equals(*out));
  }
  {
    auto schema = arrow::schema({arrow::field("a", a->type())});
    auto batch = arrow::RecordBatch::Make(schema, a->length(), {a});
    auto expr = MakeCall("not", {MakeFieldRef("a")});
    ASSERT_OK_AND_ASSIGN(auto out, EvalExprOnBatch(*engine, *batch, *expr));
    ASSERT_OK_AND_ASSIGN(
        auto expected,
        (MakePrimitiveArray<arrow::BooleanBuilder, bool>({false, true, std::nullopt, false})));
    ASSERT_TRUE(expected->Equals(*out));
  }
}

TEST(TiForthLogicalTest, AndOrXorVarArgs) {
  ASSERT_OK_AND_ASSIGN(auto engine, Engine::Create(EngineOptions{}));

  ASSERT_OK_AND_ASSIGN(
      auto a, (MakePrimitiveArray<arrow::Int64Builder, int64_t>(
                  {int64_t{1}, int64_t{0}, std::nullopt, int64_t{2}})));
  ASSERT_OK_AND_ASSIGN(
      auto b, (MakePrimitiveArray<arrow::Int64Builder, int64_t>(
                  {int64_t{1}, int64_t{1}, int64_t{0}, std::nullopt})));
  ASSERT_OK_AND_ASSIGN(
      auto c, (MakePrimitiveArray<arrow::Int64Builder, int64_t>(
                  {int64_t{1}, int64_t{0}, int64_t{1}, int64_t{1}})));

  {
    ASSERT_OK_AND_ASSIGN(auto out, EvalVarArgsAllFields(*engine, "and", {a, b, c}));
    ASSERT_OK_AND_ASSIGN(
        auto expected,
        (MakePrimitiveArray<arrow::BooleanBuilder, bool>({true, false, false, std::nullopt})));
    ASSERT_TRUE(expected->Equals(*out));
  }
  {
    ASSERT_OK_AND_ASSIGN(auto out, EvalVarArgsAllFields(*engine, "or", {a, b, c}));
    ASSERT_OK_AND_ASSIGN(
        auto expected,
        (MakePrimitiveArray<arrow::BooleanBuilder, bool>({true, true, true, true})));
    ASSERT_TRUE(expected->Equals(*out));
  }
  {
    ASSERT_OK_AND_ASSIGN(auto out, EvalVarArgsAllFields(*engine, "xor", {a, b, c}));
    ASSERT_OK_AND_ASSIGN(
        auto expected,
        (MakePrimitiveArray<arrow::BooleanBuilder, bool>({true, true, std::nullopt, std::nullopt})));
    ASSERT_TRUE(expected->Equals(*out));
  }
}

TEST(TiForthLogicalTest, NumericTruthinessFloatNaN) {
  ASSERT_OK_AND_ASSIGN(auto engine, Engine::Create(EngineOptions{}));

  const float qnan = std::numeric_limits<float>::quiet_NaN();
  ASSERT_OK_AND_ASSIGN(
      auto in, (MakePrimitiveArray<arrow::FloatBuilder, float>(
                   {0.0f, -0.0f, 1.5f, qnan, std::nullopt})));

  auto schema = arrow::schema({arrow::field("x", in->type())});
  auto batch = arrow::RecordBatch::Make(schema, in->length(), {in});
  auto expr = MakeCall("not", {MakeFieldRef("x")});
  ASSERT_OK_AND_ASSIGN(auto out, EvalExprOnBatch(*engine, *batch, *expr));

  ASSERT_OK_AND_ASSIGN(
      auto expected,
      (MakePrimitiveArray<arrow::BooleanBuilder, bool>({true, true, false, false, std::nullopt})));
  ASSERT_TRUE(expected->Equals(*out));
}

TEST(TiForthLogicalTest, ScalarBroadcast) {
  ASSERT_OK_AND_ASSIGN(auto engine, Engine::Create(EngineOptions{}));

  ASSERT_OK_AND_ASSIGN(
      auto a,
      (MakePrimitiveArray<arrow::Int32Builder, int32_t>({int32_t{1}, std::nullopt, int32_t{0}})));

  auto schema = arrow::schema({arrow::field("a", a->type())});
  auto batch = arrow::RecordBatch::Make(schema, a->length(), {a});
  if (batch == nullptr) {
    FAIL() << "failed to build input batch";
  }

  {
    auto expr = MakeCall("and", {MakeFieldRef("a"),
                                 MakeLiteral(std::make_shared<arrow::Int32Scalar>(1))});
    ASSERT_OK_AND_ASSIGN(auto out, EvalExprOnBatch(*engine, *batch, *expr));
    ASSERT_OK_AND_ASSIGN(
        auto expected,
        (MakePrimitiveArray<arrow::BooleanBuilder, bool>({true, std::nullopt, false})));
    ASSERT_TRUE(expected->Equals(*out));
  }
  {
    auto expr = MakeCall("and", {MakeFieldRef("a"),
                                 MakeLiteral(std::make_shared<arrow::Int32Scalar>(0))});
    ASSERT_OK_AND_ASSIGN(auto out, EvalExprOnBatch(*engine, *batch, *expr));
    ASSERT_OK_AND_ASSIGN(
        auto expected,
        (MakePrimitiveArray<arrow::BooleanBuilder, bool>({false, false, false})));
    ASSERT_TRUE(expected->Equals(*out));
  }
  {
    auto expr = MakeCall("or", {MakeFieldRef("a"),
                                MakeLiteral(std::make_shared<arrow::Int32Scalar>(0))});
    ASSERT_OK_AND_ASSIGN(auto out, EvalExprOnBatch(*engine, *batch, *expr));
    ASSERT_OK_AND_ASSIGN(
        auto expected,
        (MakePrimitiveArray<arrow::BooleanBuilder, bool>({true, std::nullopt, false})));
    ASSERT_TRUE(expected->Equals(*out));
  }
}

TEST(TiForthLogicalTest, WorksWithComparisonOutputs) {
  ASSERT_OK_AND_ASSIGN(auto engine, Engine::Create(EngineOptions{}));

  ASSERT_OK_AND_ASSIGN(
      auto x, (MakePrimitiveArray<arrow::Int32Builder, int32_t>(
                  {int32_t{1}, int32_t{2}, int32_t{2}, std::nullopt})));

  auto schema = arrow::schema({arrow::field("x", x->type())});
  auto batch = arrow::RecordBatch::Make(schema, x->length(), {x});
  if (batch == nullptr) {
    FAIL() << "failed to build input batch";
  }

  auto eq2 = MakeCall("equal", {MakeFieldRef("x"),
                                MakeLiteral(std::make_shared<arrow::Int32Scalar>(2))});
  auto eq1 = MakeCall("equal", {MakeFieldRef("x"),
                                MakeLiteral(std::make_shared<arrow::Int32Scalar>(1))});
  auto expr = MakeCall("and", {eq2, MakeCall("not", {eq1})});

  ASSERT_OK_AND_ASSIGN(auto out, EvalExprOnBatch(*engine, *batch, *expr));
  ASSERT_OK_AND_ASSIGN(
      auto expected,
      (MakePrimitiveArray<arrow::BooleanBuilder, bool>({false, true, true, std::nullopt})));
  ASSERT_TRUE(expected->Equals(*out));
}

}  // namespace tiforth
