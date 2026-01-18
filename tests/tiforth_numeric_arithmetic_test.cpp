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
#include <limits>
#include <optional>
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

arrow::Result<std::shared_ptr<arrow::Array>> MakeDecimal128Array(
    int32_t precision, int32_t scale,
    const std::vector<std::optional<int64_t>>& unscaled_values) {
  auto type = arrow::decimal128(precision, scale);
  arrow::Decimal128Builder builder(type);
  for (const auto& raw : unscaled_values) {
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

arrow::Result<std::shared_ptr<arrow::Array>> EvalUnary(const Engine& engine,
                                                       std::string_view function_name,
                                                       const std::shared_ptr<arrow::Array>& arg) {
  if (arg == nullptr) {
    return arrow::Status::Invalid("arg must not be null");
  }

  auto schema = arrow::schema({arrow::field("a", arg->type())});
  auto batch = arrow::RecordBatch::Make(schema, arg->length(), {arg});
  if (batch == nullptr) {
    return arrow::Status::Invalid("failed to build input batch");
  }

  auto expr = MakeCall(std::string(function_name), {MakeFieldRef("a")});
  arrow::compute::ExecContext ctx(engine.memory_pool(), /*executor=*/nullptr,
                                  engine.function_registry());
  return EvalExprAsArray(*batch, *expr, &engine, &ctx);
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

arrow::Status RunBitwiseOpsBasic() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));

  ARROW_ASSIGN_OR_RAISE(auto lhs,
                        (MakePrimitiveArray<arrow::Int64Builder, int64_t>(
                            {int64_t{-1}, int64_t{1}, std::nullopt})));
  ARROW_ASSIGN_OR_RAISE(
      auto rhs,
      (MakePrimitiveArray<arrow::Int64Builder, int64_t>({int64_t{0}, int64_t{0}, int64_t{0}})));

  {
    ARROW_ASSIGN_OR_RAISE(auto out, EvalBinary(*engine, "bitAnd", lhs, rhs));
    if (!out->type()->Equals(*arrow::uint64())) {
      return arrow::Status::Invalid("unexpected bitAnd output type: ", out->type()->ToString());
    }
    ARROW_ASSIGN_OR_RAISE(
        auto expect,
        (MakePrimitiveArray<arrow::UInt64Builder, uint64_t>({uint64_t{0}, uint64_t{0}, std::nullopt})));
    if (!expect->Equals(*out)) {
      return arrow::Status::Invalid("unexpected bitAnd output values");
    }
  }

  {
    ARROW_ASSIGN_OR_RAISE(auto out, EvalBinary(*engine, "bitOr", lhs, rhs));
    if (!out->type()->Equals(*arrow::uint64())) {
      return arrow::Status::Invalid("unexpected bitOr output type: ", out->type()->ToString());
    }
    ARROW_ASSIGN_OR_RAISE(auto expect,
                          (MakePrimitiveArray<arrow::UInt64Builder, uint64_t>(
                              {std::numeric_limits<uint64_t>::max(), uint64_t{1}, std::nullopt})));
    if (!expect->Equals(*out)) {
      return arrow::Status::Invalid("unexpected bitOr output values");
    }
  }

  {
    ARROW_ASSIGN_OR_RAISE(auto out, EvalBinary(*engine, "bitXor", lhs, rhs));
    if (!out->type()->Equals(*arrow::uint64())) {
      return arrow::Status::Invalid("unexpected bitXor output type: ", out->type()->ToString());
    }
    ARROW_ASSIGN_OR_RAISE(auto expect,
                          (MakePrimitiveArray<arrow::UInt64Builder, uint64_t>(
                              {std::numeric_limits<uint64_t>::max(), uint64_t{1}, std::nullopt})));
    if (!expect->Equals(*out)) {
      return arrow::Status::Invalid("unexpected bitXor output values");
    }
  }

  return arrow::Status::OK();
}

arrow::Status RunBitNotAndShift() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));

  ARROW_ASSIGN_OR_RAISE(auto in,
                        (MakePrimitiveArray<arrow::Int64Builder, int64_t>(
                            {int64_t{-1}, int64_t{1}, int64_t{0}})));

  {
    ARROW_ASSIGN_OR_RAISE(auto out, EvalUnary(*engine, "bitNot", in));
    if (!out->type()->Equals(*arrow::uint64())) {
      return arrow::Status::Invalid("unexpected bitNot output type: ", out->type()->ToString());
    }
    ARROW_ASSIGN_OR_RAISE(
        auto expect,
        (MakePrimitiveArray<arrow::UInt64Builder, uint64_t>(
            {uint64_t{0}, std::numeric_limits<uint64_t>::max() - 1, std::numeric_limits<uint64_t>::max()})));
    if (!expect->Equals(*out)) {
      return arrow::Status::Invalid("unexpected bitNot output values");
    }
  }

  ARROW_ASSIGN_OR_RAISE(auto lhs,
                        (MakePrimitiveArray<arrow::Int8Builder, int8_t>({int8_t{-1}})));
  ARROW_ASSIGN_OR_RAISE(auto rhs,
                        (MakePrimitiveArray<arrow::Int16Builder, int16_t>({int16_t{1}})));

  {
    ARROW_ASSIGN_OR_RAISE(auto out, EvalBinary(*engine, "bitShiftLeft", lhs, rhs));
    ARROW_ASSIGN_OR_RAISE(auto expect, (MakePrimitiveArray<arrow::UInt64Builder, uint64_t>(
                                           {18446744073709551614ULL})));
    if (!expect->Equals(*out)) {
      return arrow::Status::Invalid("unexpected bitShiftLeft output");
    }
  }

  {
    ARROW_ASSIGN_OR_RAISE(auto out, EvalBinary(*engine, "bitShiftRight", lhs, rhs));
    ARROW_ASSIGN_OR_RAISE(auto expect,
                          (MakePrimitiveArray<arrow::UInt64Builder, uint64_t>(
                              {9223372036854775807ULL})));
    if (!expect->Equals(*out)) {
      return arrow::Status::Invalid("unexpected bitShiftRight output");
    }
  }

  // Shift >= 64 yields 0.
  ARROW_ASSIGN_OR_RAISE(auto rhs64,
                        (MakePrimitiveArray<arrow::UInt64Builder, uint64_t>({uint64_t{64}})));
  {
    ARROW_ASSIGN_OR_RAISE(auto out, EvalBinary(*engine, "bitShiftLeft", lhs, rhs64));
    ARROW_ASSIGN_OR_RAISE(auto expect,
                          (MakePrimitiveArray<arrow::UInt64Builder, uint64_t>({uint64_t{0}})));
    if (!expect->Equals(*out)) {
      return arrow::Status::Invalid("unexpected bitShiftLeft(shift>=64) output");
    }
  }

  return arrow::Status::OK();
}

arrow::Status RunAbsAndNegate() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));

  // abs(Int32) -> UInt32
  ARROW_ASSIGN_OR_RAISE(auto a_i32, (MakePrimitiveArray<arrow::Int32Builder, int32_t>(
                                      {int32_t{-1}, int32_t{0}, std::numeric_limits<int32_t>::min()})));
  {
    ARROW_ASSIGN_OR_RAISE(auto out, EvalUnary(*engine, "abs", a_i32));
    if (!out->type()->Equals(*arrow::uint32())) {
      return arrow::Status::Invalid("unexpected abs(Int32) output type: ", out->type()->ToString());
    }
    ARROW_ASSIGN_OR_RAISE(auto expect,
                          (MakePrimitiveArray<arrow::UInt32Builder, uint32_t>(
                              {uint32_t{1}, uint32_t{0}, uint32_t{2147483648U}})));
    if (!expect->Equals(*out)) {
      return arrow::Status::Invalid("unexpected abs(Int32) output values");
    }
  }

  // abs(Int64 min) -> error.
  ARROW_ASSIGN_OR_RAISE(auto a_i64, (MakePrimitiveArray<arrow::Int64Builder, int64_t>(
                                      {std::numeric_limits<int64_t>::min()})));
  {
    const auto out = EvalUnary(*engine, "abs", a_i64);
    if (out.ok()) {
      return arrow::Status::Invalid("expected abs(Int64 min) failure, got OK");
    }
  }

  // negate(UInt64 max) -> Int64 1 (two's complement cast + negate).
  ARROW_ASSIGN_OR_RAISE(auto u64, (MakePrimitiveArray<arrow::UInt64Builder, uint64_t>(
                                      {std::numeric_limits<uint64_t>::max()})));
  {
    ARROW_ASSIGN_OR_RAISE(auto out, EvalUnary(*engine, "negate", u64));
    if (!out->type()->Equals(*arrow::int64())) {
      return arrow::Status::Invalid("unexpected negate(UInt64) output type: ", out->type()->ToString());
    }
    ARROW_ASSIGN_OR_RAISE(auto expect,
                          (MakePrimitiveArray<arrow::Int64Builder, int64_t>({int64_t{1}})));
    if (!expect->Equals(*out)) {
      return arrow::Status::Invalid("unexpected negate(UInt64) output values");
    }
  }

  // negate(Int8 min) wraps (two's complement) to itself.
  ARROW_ASSIGN_OR_RAISE(auto i8,
                        (MakePrimitiveArray<arrow::Int8Builder, int8_t>(
                            {std::numeric_limits<int8_t>::min()})));
  {
    ARROW_ASSIGN_OR_RAISE(auto out, EvalUnary(*engine, "negate", i8));
    if (!out->type()->Equals(*arrow::int8())) {
      return arrow::Status::Invalid("unexpected negate(Int8) output type: ", out->type()->ToString());
    }
    ARROW_ASSIGN_OR_RAISE(auto expect,
                          (MakePrimitiveArray<arrow::Int8Builder, int8_t>(
                              {std::numeric_limits<int8_t>::min()})));
    if (!expect->Equals(*out)) {
      return arrow::Status::Invalid("unexpected negate(Int8 min) output values");
    }
  }

  // abs/negate on decimal keeps decimal type.
  ARROW_ASSIGN_OR_RAISE(auto dec,
                        MakeDecimal128Array(/*precision=*/10, /*scale=*/2,
                                            {int64_t{-123}, int64_t{123}}));
  {
    ARROW_ASSIGN_OR_RAISE(auto out, EvalUnary(*engine, "abs", dec));
    if (!out->type()->Equals(*arrow::decimal128(/*precision=*/10, /*scale=*/2))) {
      return arrow::Status::Invalid("unexpected abs(decimal) output type: ", out->type()->ToString());
    }
  }
  {
    ARROW_ASSIGN_OR_RAISE(auto out, EvalUnary(*engine, "negate", dec));
    if (!out->type()->Equals(*arrow::decimal128(/*precision=*/10, /*scale=*/2))) {
      return arrow::Status::Invalid("unexpected negate(decimal) output type: ", out->type()->ToString());
    }
  }

  return arrow::Status::OK();
}

arrow::Status RunModulo() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));

  // UInt64 % Int64 ignores sign of divisor, result type follows lhs (UInt64).
  ARROW_ASSIGN_OR_RAISE(auto a_u64,
                        (MakePrimitiveArray<arrow::UInt64Builder, uint64_t>({uint64_t{5}})));
  ARROW_ASSIGN_OR_RAISE(auto b_i64,
                        (MakePrimitiveArray<arrow::Int64Builder, int64_t>({int64_t{-3}})));
  {
    ARROW_ASSIGN_OR_RAISE(auto out, EvalBinary(*engine, "modulo", a_u64, b_i64));
    if (!out->type()->Equals(*arrow::uint64())) {
      return arrow::Status::Invalid("unexpected modulo(UInt64,Int64) output type: ", out->type()->ToString());
    }
    ARROW_ASSIGN_OR_RAISE(auto expect,
                          (MakePrimitiveArray<arrow::UInt64Builder, uint64_t>({uint64_t{2}})));
    if (!expect->Equals(*out)) {
      return arrow::Status::Invalid("unexpected modulo(UInt64,Int64) output values");
    }
  }

  // Int64 % UInt64 keeps sign of dividend.
  ARROW_ASSIGN_OR_RAISE(auto a_i64,
                        (MakePrimitiveArray<arrow::Int64Builder, int64_t>({int64_t{-5}})));
  ARROW_ASSIGN_OR_RAISE(auto b_u64,
                        (MakePrimitiveArray<arrow::UInt64Builder, uint64_t>({uint64_t{3}})));
  {
    ARROW_ASSIGN_OR_RAISE(auto out, EvalBinary(*engine, "modulo", a_i64, b_u64));
    if (!out->type()->Equals(*arrow::int64())) {
      return arrow::Status::Invalid("unexpected modulo(Int64,UInt64) output type: ", out->type()->ToString());
    }
    ARROW_ASSIGN_OR_RAISE(auto expect,
                          (MakePrimitiveArray<arrow::Int64Builder, int64_t>({int64_t{-2}})));
    if (!expect->Equals(*out)) {
      return arrow::Status::Invalid("unexpected modulo(Int64,UInt64) output values");
    }
  }

  // Division by zero yields NULL.
  ARROW_ASSIGN_OR_RAISE(auto z, (MakePrimitiveArray<arrow::Int32Builder, int32_t>({int32_t{0}})));
  ARROW_ASSIGN_OR_RAISE(auto one, (MakePrimitiveArray<arrow::Int32Builder, int32_t>({int32_t{1}})));
  {
    ARROW_ASSIGN_OR_RAISE(auto out, EvalBinary(*engine, "modulo", one, z));
    if (out->null_count() != 1) {
      return arrow::Status::Invalid("expected modulo(1,0) to be NULL");
    }
  }

  // Floating modulo promotes to Float64.
  ARROW_ASSIGN_OR_RAISE(auto af,
                        (MakePrimitiveArray<arrow::FloatBuilder, float>({float{5.5}})));
  {
    ARROW_ASSIGN_OR_RAISE(auto out, EvalBinary(*engine, "modulo", af, one));
    if (!out->type()->Equals(*arrow::float64())) {
      return arrow::Status::Invalid("unexpected modulo(Float32,Int32) output type: ", out->type()->ToString());
    }
  }

  return arrow::Status::OK();
}

arrow::Status RunIntDiv() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));

  ARROW_ASSIGN_OR_RAISE(auto a, (MakePrimitiveArray<arrow::Int64Builder, int64_t>({int64_t{5}, int64_t{-5}})));
  ARROW_ASSIGN_OR_RAISE(auto b, (MakePrimitiveArray<arrow::Int64Builder, int64_t>({int64_t{2}, int64_t{2}})));
  {
    ARROW_ASSIGN_OR_RAISE(auto out, EvalBinary(*engine, "intDiv", a, b));
    ARROW_ASSIGN_OR_RAISE(auto expect,
                          (MakePrimitiveArray<arrow::Int64Builder, int64_t>({int64_t{2}, int64_t{-2}})));
    if (!expect->Equals(*out)) {
      return arrow::Status::Invalid("unexpected intDiv output values");
    }
  }

  // Division by zero is an error for intDiv, but 0 for intDivOrZero.
  ARROW_ASSIGN_OR_RAISE(auto z, (MakePrimitiveArray<arrow::Int64Builder, int64_t>({int64_t{0}})));
  {
    const auto out = EvalBinary(*engine, "intDiv", z, z);
    if (out.ok()) {
      return arrow::Status::Invalid("expected intDiv division-by-zero failure, got OK");
    }
  }
  {
    ARROW_ASSIGN_OR_RAISE(auto out, EvalBinary(*engine, "intDivOrZero", z, z));
    ARROW_ASSIGN_OR_RAISE(auto expect,
                          (MakePrimitiveArray<arrow::Int64Builder, int64_t>({int64_t{0}})));
    if (!expect->Equals(*out)) {
      return arrow::Status::Invalid("unexpected intDivOrZero division-by-zero output");
    }
  }

  // min / -1 is error for intDiv, 0 for intDivOrZero (Int8).
  ARROW_ASSIGN_OR_RAISE(auto min_i8, (MakePrimitiveArray<arrow::Int8Builder, int8_t>(
                                        {std::numeric_limits<int8_t>::min()})));
  ARROW_ASSIGN_OR_RAISE(auto minus_one_i8,
                        (MakePrimitiveArray<arrow::Int8Builder, int8_t>({int8_t{-1}})));
  {
    const auto out = EvalBinary(*engine, "intDiv", min_i8, minus_one_i8);
    if (out.ok()) {
      return arrow::Status::Invalid("expected intDiv(Int8 min,-1) failure, got OK");
    }
  }
  {
    ARROW_ASSIGN_OR_RAISE(auto out, EvalBinary(*engine, "intDivOrZero", min_i8, minus_one_i8));
    ARROW_ASSIGN_OR_RAISE(auto expect,
                          (MakePrimitiveArray<arrow::Int8Builder, int8_t>({int8_t{0}})));
    if (!expect->Equals(*out)) {
      return arrow::Status::Invalid("unexpected intDivOrZero(Int8 min,-1) output");
    }
  }

  return arrow::Status::OK();
}

arrow::Status RunGcdLcm() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));

  ARROW_ASSIGN_OR_RAISE(auto a, (MakePrimitiveArray<arrow::Int32Builder, int32_t>({int32_t{6}})));
  ARROW_ASSIGN_OR_RAISE(auto b, (MakePrimitiveArray<arrow::Int32Builder, int32_t>({int32_t{4}})));
  {
    ARROW_ASSIGN_OR_RAISE(auto out, EvalBinary(*engine, "gcd", a, b));
    if (!out->type()->Equals(*arrow::int64())) {
      return arrow::Status::Invalid("unexpected gcd(Int32,Int32) output type: ", out->type()->ToString());
    }
    ARROW_ASSIGN_OR_RAISE(auto expect,
                          (MakePrimitiveArray<arrow::Int64Builder, int64_t>({int64_t{2}})));
    if (!expect->Equals(*out)) {
      return arrow::Status::Invalid("unexpected gcd output values");
    }
  }
  {
    ARROW_ASSIGN_OR_RAISE(auto out, EvalBinary(*engine, "lcm", a, b));
    ARROW_ASSIGN_OR_RAISE(auto expect,
                          (MakePrimitiveArray<arrow::Int64Builder, int64_t>({int64_t{12}})));
    if (!expect->Equals(*out)) {
      return arrow::Status::Invalid("unexpected lcm output values");
    }
  }

  // Zero is illegal for gcd/lcm.
  ARROW_ASSIGN_OR_RAISE(auto z, (MakePrimitiveArray<arrow::Int32Builder, int32_t>({int32_t{0}})));
  {
    const auto out = EvalBinary(*engine, "gcd", z, b);
    if (out.ok()) {
      return arrow::Status::Invalid("expected gcd(0,4) failure, got OK");
    }
  }

  // Float input promotes to Float64 (cast to int64 for gcd).
  ARROW_ASSIGN_OR_RAISE(auto af, (MakePrimitiveArray<arrow::FloatBuilder, float>({float{6.9}})));
  {
    ARROW_ASSIGN_OR_RAISE(auto out, EvalBinary(*engine, "gcd", af, b));
    if (!out->type()->Equals(*arrow::float64())) {
      return arrow::Status::Invalid("unexpected gcd(Float32,Int32) output type: ", out->type()->ToString());
    }
  }

  return arrow::Status::OK();
}

}  // namespace

TEST(TiForthNumericArithmeticTest, BitwiseOpsBasic) {
  const auto st = RunBitwiseOpsBasic();
  ASSERT_TRUE(st.ok()) << st.ToString();
}

TEST(TiForthNumericArithmeticTest, BitNotAndShift) {
  const auto st = RunBitNotAndShift();
  ASSERT_TRUE(st.ok()) << st.ToString();
}

TEST(TiForthNumericArithmeticTest, AbsAndNegate) {
  const auto st = RunAbsAndNegate();
  ASSERT_TRUE(st.ok()) << st.ToString();
}

TEST(TiForthNumericArithmeticTest, Modulo) {
  const auto st = RunModulo();
  ASSERT_TRUE(st.ok()) << st.ToString();
}

TEST(TiForthNumericArithmeticTest, IntDiv) {
  const auto st = RunIntDiv();
  ASSERT_TRUE(st.ok()) << st.ToString();
}

TEST(TiForthNumericArithmeticTest, GcdLcm) {
  const auto st = RunGcdLcm();
  ASSERT_TRUE(st.ok()) << st.ToString();
}

}  // namespace tiforth
