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

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
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

arrow::Result<std::shared_ptr<arrow::Array>> EvalBinary(const Engine& engine,
                                                        std::string_view function_name,
                                                        const std::shared_ptr<arrow::Array>& lhs,
                                                        const std::shared_ptr<arrow::Array>& rhs) {
  if (lhs == nullptr || rhs == nullptr) {
    return arrow::Status::Invalid("lhs/rhs must not be null");
  }
  if (lhs->length() != rhs->length()) {
    return arrow::Status::Invalid("lhs/rhs length mismatch");
  }

  auto schema = arrow::schema({arrow::field("lhs", lhs->type()),
                               arrow::field("rhs", rhs->type())});
  auto batch = arrow::RecordBatch::Make(schema, lhs->length(), {lhs, rhs});
  if (batch == nullptr) {
    return arrow::Status::Invalid("failed to build input batch");
  }

  auto expr = MakeCall(std::string(function_name), {MakeFieldRef("lhs"), MakeFieldRef("rhs")});
  arrow::compute::ExecContext ctx(engine.memory_pool(), /*executor=*/nullptr,
                                  engine.function_registry());
  return EvalExprAsArray(*batch, *expr, &engine, &ctx);
}

}  // namespace

TEST(TiForthComparisonTest, BasicInt32) {
  ASSERT_OK_AND_ASSIGN(auto engine, Engine::Create(EngineOptions{}));

  ASSERT_OK_AND_ASSIGN(
      auto a, (MakePrimitiveArray<arrow::Int32Builder, int32_t>(
                  {int32_t{1}, int32_t{2}, std::nullopt, int32_t{2}})));
  ASSERT_OK_AND_ASSIGN(
      auto b, (MakePrimitiveArray<arrow::Int32Builder, int32_t>(
                  {int32_t{1}, int32_t{1}, int32_t{1}, std::nullopt})));

  {
    ASSERT_OK_AND_ASSIGN(auto out, EvalBinary(*engine, "equal", a, b));
    ASSERT_OK_AND_ASSIGN(
        auto expected,
        (MakePrimitiveArray<arrow::BooleanBuilder, bool>({true, false, std::nullopt, std::nullopt})));
    ASSERT_TRUE(expected->Equals(*out));
  }
  {
    ASSERT_OK_AND_ASSIGN(auto out, EvalBinary(*engine, "not_equal", a, b));
    ASSERT_OK_AND_ASSIGN(
        auto expected,
        (MakePrimitiveArray<arrow::BooleanBuilder, bool>({false, true, std::nullopt, std::nullopt})));
    ASSERT_TRUE(expected->Equals(*out));
  }
  {
    ASSERT_OK_AND_ASSIGN(auto out, EvalBinary(*engine, "less", a, b));
    ASSERT_OK_AND_ASSIGN(
        auto expected,
        (MakePrimitiveArray<arrow::BooleanBuilder, bool>({false, false, std::nullopt, std::nullopt})));
    ASSERT_TRUE(expected->Equals(*out));
  }
  {
    ASSERT_OK_AND_ASSIGN(auto out, EvalBinary(*engine, "less_equal", a, b));
    ASSERT_OK_AND_ASSIGN(
        auto expected,
        (MakePrimitiveArray<arrow::BooleanBuilder, bool>({true, false, std::nullopt, std::nullopt})));
    ASSERT_TRUE(expected->Equals(*out));
  }
  {
    ASSERT_OK_AND_ASSIGN(auto out, EvalBinary(*engine, "greater", a, b));
    ASSERT_OK_AND_ASSIGN(
        auto expected,
        (MakePrimitiveArray<arrow::BooleanBuilder, bool>({false, true, std::nullopt, std::nullopt})));
    ASSERT_TRUE(expected->Equals(*out));
  }
  {
    ASSERT_OK_AND_ASSIGN(auto out, EvalBinary(*engine, "greater_equal", a, b));
    ASSERT_OK_AND_ASSIGN(
        auto expected,
        (MakePrimitiveArray<arrow::BooleanBuilder, bool>({true, true, std::nullopt, std::nullopt})));
    ASSERT_TRUE(expected->Equals(*out));
  }
}

TEST(TiForthComparisonTest, TiFlashNameAliasesInt32) {
  ASSERT_OK_AND_ASSIGN(auto engine, Engine::Create(EngineOptions{}));

  ASSERT_OK_AND_ASSIGN(
      auto a, (MakePrimitiveArray<arrow::Int32Builder, int32_t>(
                  {int32_t{1}, int32_t{2}, std::nullopt, int32_t{2}})));
  ASSERT_OK_AND_ASSIGN(
      auto b, (MakePrimitiveArray<arrow::Int32Builder, int32_t>(
                  {int32_t{1}, int32_t{1}, int32_t{1}, std::nullopt})));

  {
    ASSERT_OK_AND_ASSIGN(auto out, EvalBinary(*engine, "equals", a, b));
    ASSERT_OK_AND_ASSIGN(
        auto expected,
        (MakePrimitiveArray<arrow::BooleanBuilder, bool>({true, false, std::nullopt, std::nullopt})));
    ASSERT_TRUE(expected->Equals(*out));
  }
  {
    ASSERT_OK_AND_ASSIGN(auto out, EvalBinary(*engine, "notEquals", a, b));
    ASSERT_OK_AND_ASSIGN(
        auto expected,
        (MakePrimitiveArray<arrow::BooleanBuilder, bool>({false, true, std::nullopt, std::nullopt})));
    ASSERT_TRUE(expected->Equals(*out));
  }
  {
    ASSERT_OK_AND_ASSIGN(auto out, EvalBinary(*engine, "lessOrEquals", a, b));
    ASSERT_OK_AND_ASSIGN(
        auto expected,
        (MakePrimitiveArray<arrow::BooleanBuilder, bool>({true, false, std::nullopt, std::nullopt})));
    ASSERT_TRUE(expected->Equals(*out));
  }
  {
    ASSERT_OK_AND_ASSIGN(auto out, EvalBinary(*engine, "greaterOrEquals", a, b));
    ASSERT_OK_AND_ASSIGN(
        auto expected,
        (MakePrimitiveArray<arrow::BooleanBuilder, bool>({true, true, std::nullopt, std::nullopt})));
    ASSERT_TRUE(expected->Equals(*out));
  }
}

}  // namespace tiforth
