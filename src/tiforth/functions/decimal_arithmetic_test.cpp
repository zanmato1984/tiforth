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
#include <arrow/type.h>
#include <arrow/util/decimal.h>
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
arrow::Result<std::shared_ptr<arrow::Array>> MakeNumericArray(
    const std::vector<std::optional<CppT>>& values) {
  BuilderT builder;
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

arrow::Result<std::shared_ptr<arrow::Array>> MakeDecimal128Array(
    int32_t precision, int32_t scale,
    const std::vector<std::optional<int64_t>>& unscaled_values) {
  auto type = arrow::decimal128(precision, scale);
  arrow::Decimal128Builder builder(type);
  for (const auto raw : unscaled_values) {
    if (raw.has_value()) {
      ARROW_RETURN_NOT_OK(builder.Append(arrow::Decimal128(*raw)));
    } else {
      ARROW_RETURN_NOT_OK(builder.AppendNull());
    }
  }
  std::shared_ptr<arrow::Array> out;
  ARROW_RETURN_NOT_OK(builder.Finish(&out));
  return out;
}

arrow::Result<std::shared_ptr<arrow::Array>> MakeDecimal256Array(
    int32_t precision, int32_t scale,
    const std::vector<std::optional<arrow::Decimal256>>& unscaled_values) {
  auto type = arrow::decimal256(precision, scale);
  arrow::Decimal256Builder builder(type);
  for (const auto& raw : unscaled_values) {
    if (raw.has_value()) {
      ARROW_RETURN_NOT_OK(builder.Append(*raw));
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

  auto expr =
      MakeCall(std::string(function_name), {MakeFieldRef("lhs"), MakeFieldRef("rhs")});
  arrow::compute::ExecContext ctx(engine.memory_pool(), /*executor=*/nullptr,
                                  engine.function_registry());
  return EvalExprAsArray(*batch, *expr, &engine, &ctx);
}

arrow::Result<std::shared_ptr<arrow::Array>> EvalBinaryRhsLiteral(
    const Engine& engine, std::string_view function_name,
    const std::shared_ptr<arrow::Array>& lhs,
    const std::shared_ptr<arrow::Scalar>& rhs_literal) {
  if (lhs == nullptr) {
    return arrow::Status::Invalid("lhs must not be null");
  }
  if (rhs_literal == nullptr) {
    return arrow::Status::Invalid("rhs literal must not be null");
  }

  auto schema = arrow::schema({arrow::field("lhs", lhs->type())});
  auto batch = arrow::RecordBatch::Make(schema, lhs->length(), {lhs});
  if (batch == nullptr) {
    return arrow::Status::Invalid("failed to build input batch");
  }

  auto expr = MakeCall(std::string(function_name),
                       {MakeFieldRef("lhs"), MakeLiteral(rhs_literal)});
  arrow::compute::ExecContext ctx(engine.memory_pool(), /*executor=*/nullptr,
                                  engine.function_registry());
  return EvalExprAsArray(*batch, *expr, &engine, &ctx);
}

arrow::Status RunDecimalSubtractMixedScales() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));

  ARROW_ASSIGN_OR_RAISE(auto lhs,
                        MakeDecimal128Array(/*precision=*/10, /*scale=*/2,
                                            {int64_t{123}, std::nullopt, int64_t{1000}}));
  ARROW_ASSIGN_OR_RAISE(
      auto rhs,
      MakeDecimal128Array(/*precision=*/10, /*scale=*/4, {int64_t{100}, int64_t{200}, int64_t{-500}}));

  ARROW_ASSIGN_OR_RAISE(auto out, EvalBinary(*engine, "subtract", lhs, rhs));
  if (!out->type()->Equals(*arrow::decimal128(/*precision=*/13, /*scale=*/4))) {
    return arrow::Status::Invalid("unexpected subtract output type: ", out->type()->ToString());
  }

  ARROW_ASSIGN_OR_RAISE(
      auto expect,
      MakeDecimal128Array(/*precision=*/13, /*scale=*/4, {int64_t{12200}, std::nullopt, int64_t{100500}}));
  if (!expect->Equals(*out)) {
    return arrow::Status::Invalid("unexpected subtract output values");
  }

  // Non-commutative check: rhs - lhs.
  ARROW_ASSIGN_OR_RAISE(auto out2, EvalBinary(*engine, "subtract", rhs, lhs));
  if (!out2->type()->Equals(*arrow::decimal128(/*precision=*/13, /*scale=*/4))) {
    return arrow::Status::Invalid("unexpected subtract (swapped) output type: ",
                                  out2->type()->ToString());
  }
  ARROW_ASSIGN_OR_RAISE(
      auto expect2,
      MakeDecimal128Array(/*precision=*/13, /*scale=*/4, {int64_t{-12200}, std::nullopt, int64_t{-100500}}));
  if (!expect2->Equals(*out2)) {
    return arrow::Status::Invalid("unexpected subtract (swapped) output values");
  }

  return arrow::Status::OK();
}

arrow::Status RunDecimalMultiplyScaleClampTruncatesTowardZero() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));

  // scale 20 + 20 is clamped to 30, so the unscaled product is divided by 10^10 (trunc toward 0).
  ARROW_ASSIGN_OR_RAISE(auto lhs_val, arrow::Decimal256::FromString("-12345678901234567890"));
  ARROW_ASSIGN_OR_RAISE(auto rhs_val, arrow::Decimal256::FromString("1"));
  ARROW_ASSIGN_OR_RAISE(auto lhs,
                        MakeDecimal256Array(/*precision=*/30, /*scale=*/20, {lhs_val}));
  ARROW_ASSIGN_OR_RAISE(auto rhs,
                        MakeDecimal256Array(/*precision=*/30, /*scale=*/20, {rhs_val}));

  ARROW_ASSIGN_OR_RAISE(auto out, EvalBinary(*engine, "multiply", lhs, rhs));
  if (!out->type()->Equals(*arrow::decimal256(/*precision=*/60, /*scale=*/30))) {
    return arrow::Status::Invalid("unexpected multiply output type: ", out->type()->ToString());
  }

  ARROW_ASSIGN_OR_RAISE(auto expect_val, arrow::Decimal256::FromString("-1234567890"));
  ARROW_ASSIGN_OR_RAISE(auto expect,
                        MakeDecimal256Array(/*precision=*/60, /*scale=*/30, {expect_val}));
  if (!expect->Equals(*out)) {
    return arrow::Status::Invalid("unexpected multiply output values");
  }
  return arrow::Status::OK();
}

arrow::Status RunDecimalMultiplyOverflowIsError() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));

  const std::string max_65_digits(65, '9');
  ARROW_ASSIGN_OR_RAISE(const auto lhs_val, arrow::Decimal256::FromString(max_65_digits));
  ARROW_ASSIGN_OR_RAISE(const auto rhs_val, arrow::Decimal256::FromString("10"));

  ARROW_ASSIGN_OR_RAISE(auto lhs, MakeDecimal256Array(/*precision=*/65, /*scale=*/0, {lhs_val}));
  ARROW_ASSIGN_OR_RAISE(auto rhs, MakeDecimal128Array(/*precision=*/2, /*scale=*/0, {int64_t{10}}));

  const auto out_res = EvalBinary(*engine, "multiply", lhs, rhs);
  if (out_res.ok()) {
    return arrow::Status::Invalid("expected overflow error, got OK");
  }
  const auto message = out_res.status().ToString();
  if (message.find("decimal math overflow") == std::string::npos) {
    return arrow::Status::Invalid("unexpected multiply overflow error message: ", message);
  }
  return arrow::Status::OK();
}

arrow::Status RunDecimalDivideTruncationAndType() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));

  ARROW_ASSIGN_OR_RAISE(auto lhs,
                        MakeDecimal128Array(/*precision=*/10, /*scale=*/2,
                                            {int64_t{123}, int64_t{-123}}));
  ARROW_ASSIGN_OR_RAISE(auto rhs,
                        MakeDecimal128Array(/*precision=*/10, /*scale=*/2,
                                            {int64_t{200}, int64_t{200}}));

  ARROW_ASSIGN_OR_RAISE(auto out, EvalBinary(*engine, "divide", lhs, rhs));
  if (!out->type()->Equals(*arrow::decimal128(/*precision=*/16, /*scale=*/6))) {
    return arrow::Status::Invalid("unexpected divide output type: ", out->type()->ToString());
  }

  ARROW_ASSIGN_OR_RAISE(auto expect, MakeDecimal128Array(/*precision=*/16, /*scale=*/6,
                                                         {int64_t{615000}, int64_t{-615000}}));
  if (!expect->Equals(*out)) {
    return arrow::Status::Invalid("unexpected divide output values");
  }
  return arrow::Status::OK();
}

arrow::Status RunDecimalDivideByZeroIsError() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));

  ARROW_ASSIGN_OR_RAISE(auto lhs, MakeDecimal128Array(/*precision=*/10, /*scale=*/0, {int64_t{1}}));
  ARROW_ASSIGN_OR_RAISE(auto rhs, MakeDecimal128Array(/*precision=*/10, /*scale=*/0, {int64_t{0}}));

  const auto out_res = EvalBinary(*engine, "divide", lhs, rhs);
  if (out_res.ok()) {
    return arrow::Status::Invalid("expected division-by-zero error, got OK");
  }
  const auto message = out_res.status().ToString();
  if (message.find("division by zero") == std::string::npos) {
    return arrow::Status::Invalid("unexpected division-by-zero error message: ", message);
  }
  return arrow::Status::OK();
}

arrow::Status RunDecimalDivideOverflowIsError() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));

  const std::string max_65_digits(65, '9');
  ARROW_ASSIGN_OR_RAISE(const auto lhs_val, arrow::Decimal256::FromString(max_65_digits));

  // Decimal(65,0) / 1 yields a result type Decimal(65,4) and overflows because the integer part
  // cannot fit into precision 65 after adding the default scale increment.
  ARROW_ASSIGN_OR_RAISE(auto lhs, MakeDecimal256Array(/*precision=*/65, /*scale=*/0, {lhs_val}));
  ARROW_ASSIGN_OR_RAISE(auto rhs, MakeDecimal128Array(/*precision=*/1, /*scale=*/0, {int64_t{1}}));

  const auto out_res = EvalBinary(*engine, "divide", lhs, rhs);
  if (out_res.ok()) {
    return arrow::Status::Invalid("expected overflow error, got OK");
  }
  const auto message = out_res.status().ToString();
  if (message.find("decimal math overflow") == std::string::npos) {
    return arrow::Status::Invalid("unexpected divide overflow error message: ", message);
  }
  return arrow::Status::OK();
}

arrow::Status RunDecimalTiDBDivideRoundsHalfUp() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));

  // Matches TiFlash gtest_arithmetic_functions.cpp: TiDBDivideDecimalRound (int/decimal).
  ARROW_ASSIGN_OR_RAISE(
      auto lhs, MakeNumericArray<arrow::Int32Builder>(std::vector<std::optional<int32_t>>{1, 1, 1, 1, 1}));
  ARROW_ASSIGN_OR_RAISE(
      auto rhs, MakeDecimal128Array(/*precision=*/20, /*scale=*/4,
                                    {int64_t{100000000}, int64_t{100010000}, int64_t{199990000},
                                     int64_t{200000000}, int64_t{200010000}}));

  ARROW_ASSIGN_OR_RAISE(auto out, EvalBinary(*engine, "tidbDivide", lhs, rhs));
  if (!out->type()->Equals(*arrow::decimal128(/*precision=*/18, /*scale=*/4))) {
    return arrow::Status::Invalid("unexpected tidbDivide output type: ", out->type()->ToString());
  }
  ARROW_ASSIGN_OR_RAISE(
      auto expect,
      MakeDecimal128Array(/*precision=*/18, /*scale=*/4, {int64_t{1}, int64_t{1}, int64_t{1}, int64_t{1}, int64_t{0}}));
  if (!expect->Equals(*out)) {
    return arrow::Status::Invalid("unexpected tidbDivide output values (int/decimal)");
  }

  // Matches TiFlash gtest_arithmetic_functions.cpp: TiDBDivideDecimalRound (decimal/decimal).
  ARROW_ASSIGN_OR_RAISE(
      auto lhs2, MakeDecimal128Array(/*precision=*/18, /*scale=*/4,
                                     {int64_t{10000}, int64_t{10000}, int64_t{10000}, int64_t{10000}, int64_t{10000}}));
  ARROW_ASSIGN_OR_RAISE(
      auto rhs2, MakeDecimal128Array(/*precision=*/18, /*scale=*/4,
                                     {int64_t{100000000}, int64_t{100010000}, int64_t{199990000},
                                      int64_t{200000000}, int64_t{200010000}}));
  ARROW_ASSIGN_OR_RAISE(auto out2, EvalBinary(*engine, "tidbDivide", lhs2, rhs2));
  if (!out2->type()->Equals(*arrow::decimal128(/*precision=*/26, /*scale=*/8))) {
    return arrow::Status::Invalid("unexpected tidbDivide output type (dec/dec): ",
                                  out2->type()->ToString());
  }
  ARROW_ASSIGN_OR_RAISE(
      auto expect2,
      MakeDecimal128Array(/*precision=*/26, /*scale=*/8,
                          {int64_t{10000}, int64_t{9999}, int64_t{5000}, int64_t{5000}, int64_t{5000}}));
  if (!expect2->Equals(*out2)) {
    return arrow::Status::Invalid("unexpected tidbDivide output values (dec/dec)");
  }

  return arrow::Status::OK();
}

arrow::Status RunDecimalTiDBDivideByZeroIsNull() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));

  ARROW_ASSIGN_OR_RAISE(auto lhs, MakeDecimal128Array(/*precision=*/10, /*scale=*/0, {int64_t{1}, int64_t{2}}));
  ARROW_ASSIGN_OR_RAISE(auto rhs, MakeDecimal128Array(/*precision=*/10, /*scale=*/0, {int64_t{0}, int64_t{1}}));

  ARROW_ASSIGN_OR_RAISE(auto out, EvalBinary(*engine, "tidbDivide", lhs, rhs));
  if (!out->type()->Equals(*arrow::decimal128(/*precision=*/14, /*scale=*/4))) {
    return arrow::Status::Invalid("unexpected tidbDivide output type: ", out->type()->ToString());
  }
  ARROW_ASSIGN_OR_RAISE(auto expect,
                        MakeDecimal128Array(/*precision=*/14, /*scale=*/4, {std::nullopt, int64_t{20000}}));
  if (!expect->Equals(*out)) {
    return arrow::Status::Invalid("unexpected tidbDivide null-on-zero output values");
  }
  return arrow::Status::OK();
}

arrow::Status RunDecimalModuloScaleAndSign() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));

  // Basic sign behavior: sign follows dividend.
  ARROW_ASSIGN_OR_RAISE(auto lhs, MakeDecimal128Array(/*precision=*/10, /*scale=*/2, {int64_t{123}, int64_t{-123}}));
  ARROW_ASSIGN_OR_RAISE(auto rhs, MakeDecimal128Array(/*precision=*/10, /*scale=*/2, {int64_t{100}, int64_t{100}}));
  ARROW_ASSIGN_OR_RAISE(auto out, EvalBinary(*engine, "modulo", lhs, rhs));
  if (!out->type()->Equals(*arrow::decimal128(/*precision=*/10, /*scale=*/2))) {
    return arrow::Status::Invalid("unexpected modulo output type: ", out->type()->ToString());
  }
  ARROW_ASSIGN_OR_RAISE(auto expect, MakeDecimal128Array(/*precision=*/10, /*scale=*/2, {int64_t{23}, int64_t{-23}}));
  if (!expect->Equals(*out)) {
    return arrow::Status::Invalid("unexpected modulo sign output values");
  }

  // lhs_scale > rhs_scale: divisor is scaled up.
  ARROW_ASSIGN_OR_RAISE(auto lhs2, MakeDecimal128Array(/*precision=*/18, /*scale=*/2, {int64_t{12345}}));
  ARROW_ASSIGN_OR_RAISE(auto rhs2, MakeDecimal128Array(/*precision=*/18, /*scale=*/0, {int64_t{100}}));
  ARROW_ASSIGN_OR_RAISE(auto out2, EvalBinary(*engine, "modulo", lhs2, rhs2));
  ARROW_ASSIGN_OR_RAISE(auto expect2, MakeDecimal128Array(/*precision=*/18, /*scale=*/2, {int64_t{2345}}));
  if (!expect2->Equals(*out2)) {
    return arrow::Status::Invalid("unexpected modulo scaling (lhs_scale > rhs_scale)");
  }

  // lhs_scale < rhs_scale: remainder is carried through digit-by-digit.
  ARROW_ASSIGN_OR_RAISE(auto lhs3, MakeDecimal128Array(/*precision=*/18, /*scale=*/0, {int64_t{123}}));
  ARROW_ASSIGN_OR_RAISE(auto rhs3, MakeDecimal128Array(/*precision=*/18, /*scale=*/2, {int64_t{10000}}));
  ARROW_ASSIGN_OR_RAISE(auto out3, EvalBinary(*engine, "modulo", lhs3, rhs3));
  ARROW_ASSIGN_OR_RAISE(auto expect3, MakeDecimal128Array(/*precision=*/18, /*scale=*/2, {int64_t{2300}}));
  if (!expect3->Equals(*out3)) {
    return arrow::Status::Invalid("unexpected modulo scaling (lhs_scale < rhs_scale)");
  }

  // rhs_scale < lhs_scale but scaling divisor overflows: remainder is dividend.
  const std::string max_65_digits(65, '9');
  ARROW_ASSIGN_OR_RAISE(const auto rhs_big, arrow::Decimal256::FromString(max_65_digits));
  ARROW_ASSIGN_OR_RAISE(const auto lhs_small, arrow::Decimal256::FromString("1"));
  ARROW_ASSIGN_OR_RAISE(auto lhs4, MakeDecimal256Array(/*precision=*/65, /*scale=*/30, {lhs_small}));
  ARROW_ASSIGN_OR_RAISE(auto rhs4, MakeDecimal256Array(/*precision=*/65, /*scale=*/0, {rhs_big}));
  ARROW_ASSIGN_OR_RAISE(auto out4, EvalBinary(*engine, "modulo", lhs4, rhs4));
  if (!out4->type()->Equals(*arrow::decimal256(/*precision=*/65, /*scale=*/30))) {
    return arrow::Status::Invalid("unexpected modulo overflow-path output type: ", out4->type()->ToString());
  }
  ARROW_ASSIGN_OR_RAISE(auto expect4, MakeDecimal256Array(/*precision=*/65, /*scale=*/30, {lhs_small}));
  if (!expect4->Equals(*out4)) {
    return arrow::Status::Invalid("unexpected modulo overflow-path output values");
  }

  return arrow::Status::OK();
}

arrow::Status RunDecimalModuloByZeroIsNull() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));

  ARROW_ASSIGN_OR_RAISE(auto lhs, MakeDecimal128Array(/*precision=*/10, /*scale=*/0, {int64_t{1}, int64_t{2}}));
  ARROW_ASSIGN_OR_RAISE(auto rhs, MakeDecimal128Array(/*precision=*/10, /*scale=*/0, {int64_t{0}, int64_t{1}}));

  ARROW_ASSIGN_OR_RAISE(auto out, EvalBinary(*engine, "modulo", lhs, rhs));
  if (!out->type()->Equals(*arrow::decimal128(/*precision=*/10, /*scale=*/0))) {
    return arrow::Status::Invalid("unexpected modulo output type: ", out->type()->ToString());
  }
  ARROW_ASSIGN_OR_RAISE(auto expect,
                        MakeDecimal128Array(/*precision=*/10, /*scale=*/0, {std::nullopt, int64_t{0}}));
  if (!expect->Equals(*out)) {
    return arrow::Status::Invalid("unexpected modulo null-on-zero output values");
  }
  return arrow::Status::OK();
}

arrow::Status RunDecimalAddWithIntLiteralUsesDecimalKernel() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));

  ARROW_ASSIGN_OR_RAISE(auto lhs, MakeDecimal128Array(/*precision=*/10, /*scale=*/2, {int64_t{123}}));
  auto rhs_lit = std::make_shared<arrow::Int32Scalar>(2);

  ARROW_ASSIGN_OR_RAISE(auto out, EvalBinaryRhsLiteral(*engine, "add", lhs, rhs_lit));
  if (!out->type()->Equals(*arrow::decimal128(/*precision=*/13, /*scale=*/2))) {
    return arrow::Status::Invalid("unexpected add(decimal,int literal) output type: ", out->type()->ToString());
  }
  ARROW_ASSIGN_OR_RAISE(auto expect, MakeDecimal128Array(/*precision=*/13, /*scale=*/2, {int64_t{323}}));
  if (!expect->Equals(*out)) {
    return arrow::Status::Invalid("unexpected add(decimal,int literal) output values");
  }
  return arrow::Status::OK();
}

}  // namespace

TEST(TiForthDecimalArithmeticTest, SubtractMixedScales) {
  auto status = RunDecimalSubtractMixedScales();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthDecimalArithmeticTest, MultiplyScaleClampTruncatesTowardZero) {
  auto status = RunDecimalMultiplyScaleClampTruncatesTowardZero();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthDecimalArithmeticTest, MultiplyOverflowIsError) {
  auto status = RunDecimalMultiplyOverflowIsError();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthDecimalArithmeticTest, DivideTruncationAndType) {
  auto status = RunDecimalDivideTruncationAndType();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthDecimalArithmeticTest, DivideByZeroIsError) {
  auto status = RunDecimalDivideByZeroIsError();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthDecimalArithmeticTest, DivideOverflowIsError) {
  auto status = RunDecimalDivideOverflowIsError();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthDecimalArithmeticTest, TiDBDivideRoundsHalfUp) {
  auto status = RunDecimalTiDBDivideRoundsHalfUp();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthDecimalArithmeticTest, TiDBDivideByZeroIsNull) {
  auto status = RunDecimalTiDBDivideByZeroIsNull();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthDecimalArithmeticTest, ModuloScaleAndSign) {
  auto status = RunDecimalModuloScaleAndSign();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthDecimalArithmeticTest, ModuloByZeroIsNull) {
  auto status = RunDecimalModuloByZeroIsNull();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthDecimalArithmeticTest, AddWithIntLiteralUsesDecimalKernel) {
  auto status = RunDecimalAddWithIntLiteralUsesDecimalKernel();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

}  // namespace tiforth
