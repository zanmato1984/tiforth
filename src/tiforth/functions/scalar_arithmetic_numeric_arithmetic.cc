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

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstdint>
#include <limits>
#include <memory>
#include <numeric>
#include <optional>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/compute/exec.h>
#include <arrow/compute/function.h>
#include <arrow/compute/kernel.h>
#include <arrow/compute/registry.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/util/decimal.h>
#include <arrow/util/logging.h>

namespace tiforth::function {

namespace {

bool IsIntegerType(arrow::Type::type type_id) {
  switch (type_id) {
    case arrow::Type::INT8:
    case arrow::Type::UINT8:
    case arrow::Type::INT16:
    case arrow::Type::UINT16:
    case arrow::Type::INT32:
    case arrow::Type::UINT32:
    case arrow::Type::INT64:
    case arrow::Type::UINT64:
      return true;
    default:
      return false;
  }
}

bool IsSignedIntegerType(arrow::Type::type type_id) {
  switch (type_id) {
    case arrow::Type::INT8:
    case arrow::Type::INT16:
    case arrow::Type::INT32:
    case arrow::Type::INT64:
      return true;
    default:
      return false;
  }
}

bool IsUnsignedIntegerType(arrow::Type::type type_id) {
  switch (type_id) {
    case arrow::Type::UINT8:
    case arrow::Type::UINT16:
    case arrow::Type::UINT32:
    case arrow::Type::UINT64:
      return true;
    default:
      return false;
  }
}

bool IsFloatingType(arrow::Type::type type_id) {
  return type_id == arrow::Type::FLOAT || type_id == arrow::Type::DOUBLE;
}

bool IsDecimalType(arrow::Type::type type_id) {
  return type_id == arrow::Type::DECIMAL128 || type_id == arrow::Type::DECIMAL256;
}

arrow::Result<int32_t> ByteWidthOf(arrow::Type::type type_id) {
  switch (type_id) {
    case arrow::Type::INT8:
    case arrow::Type::UINT8:
      return 1;
    case arrow::Type::INT16:
    case arrow::Type::UINT16:
      return 2;
    case arrow::Type::INT32:
    case arrow::Type::UINT32:
    case arrow::Type::FLOAT:
      return 4;
    case arrow::Type::INT64:
    case arrow::Type::UINT64:
    case arrow::Type::DOUBLE:
      return 8;
    default:
      return arrow::Status::Invalid("unsupported type for byte width: ", static_cast<int>(type_id));
  }
}

int32_t NextSizeBytes(int32_t size_bytes) { return std::min(size_bytes * 2, 8); }

arrow::Result<std::shared_ptr<arrow::DataType>> MakeIntegerType(bool is_signed, int32_t byte_width) {
  if (byte_width <= 0 || byte_width > 8) {
    return arrow::Status::Invalid("invalid integer byte width: ", byte_width);
  }
  switch (byte_width) {
    case 1:
      return is_signed ? arrow::int8() : arrow::uint8();
    case 2:
      return is_signed ? arrow::int16() : arrow::uint16();
    case 4:
      return is_signed ? arrow::int32() : arrow::uint32();
    case 8:
      return is_signed ? arrow::int64() : arrow::uint64();
    default:
      break;
  }
  return arrow::Status::Invalid("unsupported integer byte width: ", byte_width);
}

arrow::Result<int64_t> MinSignedFor(arrow::Type::type type_id) {
  switch (type_id) {
    case arrow::Type::INT8:
      return std::numeric_limits<int8_t>::min();
    case arrow::Type::INT16:
      return std::numeric_limits<int16_t>::min();
    case arrow::Type::INT32:
      return std::numeric_limits<int32_t>::min();
    case arrow::Type::INT64:
      return std::numeric_limits<int64_t>::min();
    case arrow::Type::FLOAT:
    case arrow::Type::DOUBLE:
      return std::numeric_limits<int64_t>::min();
    default:
      break;
  }
  return arrow::Status::Invalid("no signed min for type: ", static_cast<int>(type_id));
}

template <typename ArrayT, typename ScalarT, typename ValueT>
struct PrimitiveReader {
  const ArrayT* array = nullptr;
  const ScalarT* scalar = nullptr;
  bool scalar_valid = false;
  ValueT scalar_value{};

  arrow::Status Init(const std::shared_ptr<arrow::Array>& input_array, const arrow::Scalar* input_scalar) {
    array = input_array != nullptr ? static_cast<const ArrayT*>(input_array.get()) : nullptr;
    scalar = input_scalar != nullptr ? dynamic_cast<const ScalarT*>(input_scalar) : nullptr;
    if (input_scalar != nullptr && scalar == nullptr) {
      return arrow::Status::Invalid("unexpected scalar type");
    }
    scalar_valid = scalar != nullptr && scalar->is_valid;
    scalar_value = scalar_valid ? scalar->value : ValueT{};
    return arrow::Status::OK();
  }

  bool IsNull(int64_t i) const { return array != nullptr ? array->IsNull(i) : !scalar_valid; }
  ValueT Value(int64_t i) const { return array != nullptr ? array->Value(i) : scalar_value; }
};

template <typename DecimalT, typename ScalarT, int ExpectedByteWidth>
struct DecimalReader {
  const arrow::FixedSizeBinaryArray* array = nullptr;
  const ScalarT* scalar = nullptr;
  bool scalar_valid = false;
  DecimalT scalar_value{};

  arrow::Status Init(const std::shared_ptr<arrow::Array>& input_array, const arrow::Scalar* input_scalar) {
    array = input_array != nullptr ? static_cast<const arrow::FixedSizeBinaryArray*>(input_array.get()) : nullptr;
    if (array != nullptr && array->byte_width() != ExpectedByteWidth) {
      return arrow::Status::Invalid("unexpected decimal byte width");
    }
    scalar = input_scalar != nullptr ? dynamic_cast<const ScalarT*>(input_scalar) : nullptr;
    if (input_scalar != nullptr && scalar == nullptr) {
      return arrow::Status::Invalid("unexpected decimal scalar type");
    }
    scalar_valid = scalar != nullptr && scalar->is_valid;
    scalar_value = scalar_valid ? scalar->value : DecimalT{};
    return arrow::Status::OK();
  }

  bool IsNull(int64_t i) const { return array != nullptr ? array->IsNull(i) : !scalar_valid; }

  DecimalT Value(int64_t i) const {
    if (array == nullptr) {
      return scalar_value;
    }
    return DecimalT(reinterpret_cast<const uint8_t*>(array->GetValue(i)));
  }
};

using Int8Reader = PrimitiveReader<arrow::Int8Array, arrow::Int8Scalar, int8_t>;
using UInt8Reader = PrimitiveReader<arrow::UInt8Array, arrow::UInt8Scalar, uint8_t>;
using Int16Reader = PrimitiveReader<arrow::Int16Array, arrow::Int16Scalar, int16_t>;
using UInt16Reader = PrimitiveReader<arrow::UInt16Array, arrow::UInt16Scalar, uint16_t>;
using Int32Reader = PrimitiveReader<arrow::Int32Array, arrow::Int32Scalar, int32_t>;
using UInt32Reader = PrimitiveReader<arrow::UInt32Array, arrow::UInt32Scalar, uint32_t>;
using Int64Reader = PrimitiveReader<arrow::Int64Array, arrow::Int64Scalar, int64_t>;
using UInt64Reader = PrimitiveReader<arrow::UInt64Array, arrow::UInt64Scalar, uint64_t>;
using Float32Reader = PrimitiveReader<arrow::FloatArray, arrow::FloatScalar, float>;
using Float64Reader = PrimitiveReader<arrow::DoubleArray, arrow::DoubleScalar, double>;

using Dec128Reader = DecimalReader<arrow::Decimal128, arrow::Decimal128Scalar, 16>;
using Dec256Reader = DecimalReader<arrow::Decimal256, arrow::Decimal256Scalar, 32>;

using IntegerReader = std::variant<Int8Reader, UInt8Reader, Int16Reader, UInt16Reader, Int32Reader, UInt32Reader,
                                   Int64Reader, UInt64Reader>;
using NumericReader =
    std::variant<Int8Reader, UInt8Reader, Int16Reader, UInt16Reader, Int32Reader, UInt32Reader, Int64Reader,
                 UInt64Reader, Float32Reader, Float64Reader>;
using ExtendedNumericReader = std::variant<Int8Reader, UInt8Reader, Int16Reader, UInt16Reader, Int32Reader,
                                           UInt32Reader, Int64Reader, UInt64Reader, Float32Reader, Float64Reader,
                                           Dec128Reader, Dec256Reader>;

arrow::Result<IntegerReader> MakeIntegerReader(const arrow::DataType& type,
                                               const std::shared_ptr<arrow::Array>& input_array,
                                               const arrow::Scalar* input_scalar) {
  switch (type.id()) {
    case arrow::Type::INT8: {
      Int8Reader reader;
      ARROW_RETURN_NOT_OK(reader.Init(input_array, input_scalar));
      return IntegerReader{std::move(reader)};
    }
    case arrow::Type::UINT8: {
      UInt8Reader reader;
      ARROW_RETURN_NOT_OK(reader.Init(input_array, input_scalar));
      return IntegerReader{std::move(reader)};
    }
    case arrow::Type::INT16: {
      Int16Reader reader;
      ARROW_RETURN_NOT_OK(reader.Init(input_array, input_scalar));
      return IntegerReader{std::move(reader)};
    }
    case arrow::Type::UINT16: {
      UInt16Reader reader;
      ARROW_RETURN_NOT_OK(reader.Init(input_array, input_scalar));
      return IntegerReader{std::move(reader)};
    }
    case arrow::Type::INT32: {
      Int32Reader reader;
      ARROW_RETURN_NOT_OK(reader.Init(input_array, input_scalar));
      return IntegerReader{std::move(reader)};
    }
    case arrow::Type::UINT32: {
      UInt32Reader reader;
      ARROW_RETURN_NOT_OK(reader.Init(input_array, input_scalar));
      return IntegerReader{std::move(reader)};
    }
    case arrow::Type::INT64: {
      Int64Reader reader;
      ARROW_RETURN_NOT_OK(reader.Init(input_array, input_scalar));
      return IntegerReader{std::move(reader)};
    }
    case arrow::Type::UINT64: {
      UInt64Reader reader;
      ARROW_RETURN_NOT_OK(reader.Init(input_array, input_scalar));
      return IntegerReader{std::move(reader)};
    }
    default:
      break;
  }
  return arrow::Status::Invalid("expected integer type");
}

arrow::Result<NumericReader> MakeNumericReader(const arrow::DataType& type, const std::shared_ptr<arrow::Array>& input_array,
                                               const arrow::Scalar* input_scalar) {
  if (IsIntegerType(type.id())) {
    ARROW_ASSIGN_OR_RAISE(auto integer_reader, MakeIntegerReader(type, input_array, input_scalar));
    return std::visit([](auto&& reader) -> NumericReader { return NumericReader{std::move(reader)}; },
                      std::move(integer_reader));
  }
  switch (type.id()) {
    case arrow::Type::FLOAT: {
      Float32Reader reader;
      ARROW_RETURN_NOT_OK(reader.Init(input_array, input_scalar));
      return NumericReader{std::move(reader)};
    }
    case arrow::Type::DOUBLE: {
      Float64Reader reader;
      ARROW_RETURN_NOT_OK(reader.Init(input_array, input_scalar));
      return NumericReader{std::move(reader)};
    }
    default:
      break;
  }
  return arrow::Status::Invalid("expected numeric type");
}

arrow::Result<ExtendedNumericReader> MakeExtendedNumericReader(
    const arrow::DataType& type, const std::shared_ptr<arrow::Array>& input_array, const arrow::Scalar* input_scalar) {
  if (IsIntegerType(type.id()) || IsFloatingType(type.id())) {
    ARROW_ASSIGN_OR_RAISE(auto numeric_reader, MakeNumericReader(type, input_array, input_scalar));
    return std::visit([](auto&& reader) -> ExtendedNumericReader { return ExtendedNumericReader{std::move(reader)}; },
                      std::move(numeric_reader));
  }
  switch (type.id()) {
    case arrow::Type::DECIMAL128: {
      Dec128Reader reader;
      ARROW_RETURN_NOT_OK(reader.Init(input_array, input_scalar));
      return ExtendedNumericReader{std::move(reader)};
    }
    case arrow::Type::DECIMAL256: {
      Dec256Reader reader;
      ARROW_RETURN_NOT_OK(reader.Init(input_array, input_scalar));
      return ExtendedNumericReader{std::move(reader)};
    }
    default:
      break;
  }
  return arrow::Status::Invalid("expected numeric or decimal type");
}

struct ExecInputs {
  std::shared_ptr<arrow::Array> lhs_array;
  std::shared_ptr<arrow::Array> rhs_array;
  const arrow::Scalar* lhs_scalar = nullptr;
  const arrow::Scalar* rhs_scalar = nullptr;
};

ExecInputs GetExecInputs(const arrow::compute::ExecSpan& batch) {
  ExecInputs in;
  if (batch[0].is_array()) {
    in.lhs_array = arrow::MakeArray(batch[0].array.ToArrayData());
  } else if (batch[0].is_scalar()) {
    in.lhs_scalar = batch[0].scalar;
  }
  if (batch[1].is_array()) {
    in.rhs_array = arrow::MakeArray(batch[1].array.ToArrayData());
  } else if (batch[1].is_scalar()) {
    in.rhs_scalar = batch[1].scalar;
  }
  return in;
}

struct ExecInput {
  std::shared_ptr<arrow::Array> array;
  const arrow::Scalar* scalar = nullptr;
};

ExecInput GetExecInput(const arrow::compute::ExecSpan& batch) {
  ExecInput in;
  if (batch[0].is_array()) {
    in.array = arrow::MakeArray(batch[0].array.ToArrayData());
  } else if (batch[0].is_scalar()) {
    in.scalar = batch[0].scalar;
  }
  return in;
}

template <typename BuilderT, typename ReaderT, typename ComputeFn>
arrow::Status ExecUnaryLoop(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                            arrow::compute::ExecResult* out, const std::shared_ptr<arrow::DataType>& out_type,
                            const ReaderT& in, ComputeFn&& compute) {
  if (ctx == nullptr || out == nullptr) {
    return arrow::Status::Invalid("kernel context/result must not be null");
  }
  const int64_t rows = batch.length;
  BuilderT builder = [&]() -> BuilderT {
    if constexpr (std::is_same_v<BuilderT, arrow::Decimal128Builder> ||
                  std::is_same_v<BuilderT, arrow::Decimal256Builder>) {
      return BuilderT(out_type, ctx->memory_pool());
    } else {
      return BuilderT(ctx->memory_pool());
    }
  }();
  ARROW_RETURN_NOT_OK(builder.Reserve(rows));
  for (int64_t i = 0; i < rows; ++i) {
    if (in.IsNull(i)) {
      builder.UnsafeAppendNull();
      continue;
    }
    ARROW_ASSIGN_OR_RAISE(auto maybe_value, compute(i));
    if (!maybe_value.has_value()) {
      builder.UnsafeAppendNull();
      continue;
    }
    builder.UnsafeAppend(*maybe_value);
  }
  std::shared_ptr<arrow::Array> out_array;
  ARROW_RETURN_NOT_OK(builder.Finish(&out_array));
  out->value = out_array->data();
  return arrow::Status::OK();
}

template <typename BuilderT, typename LhsReaderT, typename RhsReaderT, typename ComputeFn>
arrow::Status ExecBinaryLoop(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                             arrow::compute::ExecResult* out, const std::shared_ptr<arrow::DataType>& out_type,
                             const LhsReaderT& lhs, const RhsReaderT& rhs, ComputeFn&& compute) {
  if (ctx == nullptr || out == nullptr) {
    return arrow::Status::Invalid("kernel context/result must not be null");
  }
  const int64_t rows = batch.length;
  BuilderT builder = [&]() -> BuilderT {
    if constexpr (std::is_same_v<BuilderT, arrow::Decimal128Builder> ||
                  std::is_same_v<BuilderT, arrow::Decimal256Builder>) {
      return BuilderT(out_type, ctx->memory_pool());
    } else {
      return BuilderT(ctx->memory_pool());
    }
  }();
  ARROW_RETURN_NOT_OK(builder.Reserve(rows));
  for (int64_t i = 0; i < rows; ++i) {
    if (lhs.IsNull(i) || rhs.IsNull(i)) {
      builder.UnsafeAppendNull();
      continue;
    }
    ARROW_ASSIGN_OR_RAISE(auto maybe_value, compute(i));
    if (!maybe_value.has_value()) {
      builder.UnsafeAppendNull();
      continue;
    }
    builder.UnsafeAppend(*maybe_value);
  }
  std::shared_ptr<arrow::Array> out_array;
  ARROW_RETURN_NOT_OK(builder.Finish(&out_array));
  out->value = out_array->data();
  return arrow::Status::OK();
}

arrow::Result<arrow::TypeHolder> ResolveBitwiseOutputType(arrow::compute::KernelContext*,
                                                          const std::vector<arrow::TypeHolder>& types) {
  if (types.size() != 2) {
    return arrow::Status::Invalid("bitwise op requires 2 args");
  }
  if (types[0].type == nullptr || types[1].type == nullptr) {
    return arrow::Status::Invalid("bitwise op input type must not be null");
  }
  if (!IsIntegerType(types[0].type->id()) || !IsIntegerType(types[1].type->id())) {
    return arrow::Status::Invalid("bitwise op requires integer args");
  }
  return arrow::TypeHolder(arrow::uint64());
}

arrow::Result<arrow::TypeHolder> ResolveBitNotOutputType(arrow::compute::KernelContext*,
                                                         const std::vector<arrow::TypeHolder>& types) {
  if (types.size() != 1) {
    return arrow::Status::Invalid("bitNot requires 1 arg");
  }
  if (types[0].type == nullptr) {
    return arrow::Status::Invalid("bitNot input type must not be null");
  }
  if (!IsIntegerType(types[0].type->id())) {
    return arrow::Status::Invalid("bitNot requires integer arg");
  }
  return arrow::TypeHolder(arrow::uint64());
}

arrow::Result<arrow::TypeHolder> ResolveAbsOutputType(arrow::compute::KernelContext*,
                                                      const std::vector<arrow::TypeHolder>& types) {
  if (types.size() != 1) {
    return arrow::Status::Invalid("abs requires 1 arg");
  }
  if (types[0].type == nullptr) {
    return arrow::Status::Invalid("abs input type must not be null");
  }
  const auto id = types[0].type->id();
  if (IsUnsignedIntegerType(id) || IsFloatingType(id) || IsDecimalType(id)) {
    return arrow::TypeHolder(types[0].type);
  }
  if (IsSignedIntegerType(id)) {
    ARROW_ASSIGN_OR_RAISE(const auto bytes, ByteWidthOf(id));
    ARROW_ASSIGN_OR_RAISE(auto out_type, MakeIntegerType(/*is_signed=*/false, bytes));
    return arrow::TypeHolder(std::move(out_type));
  }
  return arrow::Status::Invalid("abs requires numeric/decimal arg");
}

arrow::Result<arrow::TypeHolder> ResolveNegateOutputType(arrow::compute::KernelContext*,
                                                         const std::vector<arrow::TypeHolder>& types) {
  if (types.size() != 1) {
    return arrow::Status::Invalid("negate requires 1 arg");
  }
  if (types[0].type == nullptr) {
    return arrow::Status::Invalid("negate input type must not be null");
  }
  const auto id = types[0].type->id();
  if (IsSignedIntegerType(id) || IsFloatingType(id) || IsDecimalType(id)) {
    return arrow::TypeHolder(types[0].type);
  }
  if (IsUnsignedIntegerType(id)) {
    ARROW_ASSIGN_OR_RAISE(const auto bytes, ByteWidthOf(id));
    ARROW_ASSIGN_OR_RAISE(auto out_type, MakeIntegerType(/*is_signed=*/true, NextSizeBytes(bytes)));
    return arrow::TypeHolder(std::move(out_type));
  }
  return arrow::Status::Invalid("negate requires numeric/decimal arg");
}

arrow::Result<arrow::TypeHolder> ResolveModuloOutputType(arrow::compute::KernelContext*,
                                                         const std::vector<arrow::TypeHolder>& types) {
  if (types.size() != 2) {
    return arrow::Status::Invalid("modulo requires 2 args");
  }
  if (types[0].type == nullptr || types[1].type == nullptr) {
    return arrow::Status::Invalid("modulo input type must not be null");
  }
  const auto lhs_id = types[0].type->id();
  const auto rhs_id = types[1].type->id();
  if (IsDecimalType(lhs_id) || IsDecimalType(rhs_id)) {
    return arrow::Status::Invalid("modulo requires non-decimal args (decimal cases are rewritten)");
  }
  if (IsFloatingType(lhs_id) || IsFloatingType(rhs_id)) {
    return arrow::TypeHolder(arrow::float64());
  }
  if (!IsIntegerType(lhs_id) || !IsIntegerType(rhs_id)) {
    return arrow::Status::Invalid("modulo requires numeric args");
  }
  ARROW_ASSIGN_OR_RAISE(const auto lhs_bytes, ByteWidthOf(lhs_id));
  ARROW_ASSIGN_OR_RAISE(const auto rhs_bytes, ByteWidthOf(rhs_id));
  const int32_t out_bytes = std::max(lhs_bytes, rhs_bytes);
  ARROW_ASSIGN_OR_RAISE(auto out_type, MakeIntegerType(/*is_signed=*/IsSignedIntegerType(lhs_id), out_bytes));
  return arrow::TypeHolder(std::move(out_type));
}

arrow::Result<arrow::TypeHolder> ResolveIntDivOutputType(arrow::compute::KernelContext*,
                                                         const std::vector<arrow::TypeHolder>& types) {
  if (types.size() != 2) {
    return arrow::Status::Invalid("intDiv requires 2 args");
  }
  if (types[0].type == nullptr || types[1].type == nullptr) {
    return arrow::Status::Invalid("intDiv input type must not be null");
  }
  const auto lhs_id = types[0].type->id();
  const auto rhs_id = types[1].type->id();
  if (!IsIntegerType(lhs_id) || !IsIntegerType(rhs_id)) {
    return arrow::Status::Invalid("intDiv requires integer args");
  }
  ARROW_ASSIGN_OR_RAISE(const auto lhs_bytes, ByteWidthOf(lhs_id));
  ARROW_ASSIGN_OR_RAISE(auto out_type, MakeIntegerType(/*is_signed=*/IsSignedIntegerType(lhs_id) || IsSignedIntegerType(rhs_id),
                                                       lhs_bytes));
  return arrow::TypeHolder(std::move(out_type));
}

arrow::Result<arrow::TypeHolder> ResolveGcdLcmOutputType(arrow::compute::KernelContext*,
                                                         const std::vector<arrow::TypeHolder>& types) {
  if (types.size() != 2) {
    return arrow::Status::Invalid("gcd/lcm requires 2 args");
  }
  if (types[0].type == nullptr || types[1].type == nullptr) {
    return arrow::Status::Invalid("gcd/lcm input type must not be null");
  }
  const auto lhs_id = types[0].type->id();
  const auto rhs_id = types[1].type->id();
  if (IsDecimalType(lhs_id) || IsDecimalType(rhs_id)) {
    return arrow::Status::Invalid("gcd/lcm does not support decimal args");
  }
  if (IsFloatingType(lhs_id) || IsFloatingType(rhs_id)) {
    return arrow::TypeHolder(arrow::float64());
  }
  if (!IsIntegerType(lhs_id) || !IsIntegerType(rhs_id)) {
    return arrow::Status::Invalid("gcd/lcm requires numeric args");
  }
  ARROW_ASSIGN_OR_RAISE(const auto lhs_bytes, ByteWidthOf(lhs_id));
  ARROW_ASSIGN_OR_RAISE(const auto rhs_bytes, ByteWidthOf(rhs_id));
  const int32_t out_bytes = NextSizeBytes(std::max(lhs_bytes, rhs_bytes));
  ARROW_ASSIGN_OR_RAISE(auto out_type, MakeIntegerType(/*is_signed=*/IsSignedIntegerType(lhs_id) || IsSignedIntegerType(rhs_id),
                                                       out_bytes));
  return arrow::TypeHolder(std::move(out_type));
}

template <typename T>
uint64_t ToUInt64(T value) {
  return static_cast<uint64_t>(value);
}

uint64_t AbsToUInt64(int64_t v);

arrow::Status ExecBitAnd(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                         arrow::compute::ExecResult* out) {
  if (ctx == nullptr || out == nullptr) {
    return arrow::Status::Invalid("kernel context/result must not be null");
  }
  if (batch.num_values() != 2) {
    return arrow::Status::Invalid("bitAnd requires 2 args");
  }
  const auto* lhs_type = batch[0].type();
  const auto* rhs_type = batch[1].type();
  if (lhs_type == nullptr || rhs_type == nullptr) {
    return arrow::Status::Invalid("bitAnd input type must not be null");
  }
  if (!IsIntegerType(lhs_type->id()) || !IsIntegerType(rhs_type->id())) {
    return arrow::Status::Invalid("bitAnd requires integer args");
  }
  const auto in = GetExecInputs(batch);
  ARROW_ASSIGN_OR_RAISE(auto lhs_reader, MakeIntegerReader(*lhs_type, in.lhs_array, in.lhs_scalar));
  ARROW_ASSIGN_OR_RAISE(auto rhs_reader, MakeIntegerReader(*rhs_type, in.rhs_array, in.rhs_scalar));
  const auto out_type = arrow::uint64();
  return std::visit(
      [&](const auto& lhs, const auto& rhs) -> arrow::Status {
        const auto compute = [&](int64_t i) -> arrow::Result<std::optional<uint64_t>> {
          return std::optional<uint64_t>(ToUInt64(lhs.Value(i)) & ToUInt64(rhs.Value(i)));
        };
        return ExecBinaryLoop<arrow::UInt64Builder>(ctx, batch, out, out_type, lhs, rhs, compute);
      },
      lhs_reader, rhs_reader);
}

arrow::Status ExecBitOr(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                        arrow::compute::ExecResult* out) {
  if (ctx == nullptr || out == nullptr) {
    return arrow::Status::Invalid("kernel context/result must not be null");
  }
  if (batch.num_values() != 2) {
    return arrow::Status::Invalid("bitOr requires 2 args");
  }
  const auto* lhs_type = batch[0].type();
  const auto* rhs_type = batch[1].type();
  if (lhs_type == nullptr || rhs_type == nullptr) {
    return arrow::Status::Invalid("bitOr input type must not be null");
  }
  if (!IsIntegerType(lhs_type->id()) || !IsIntegerType(rhs_type->id())) {
    return arrow::Status::Invalid("bitOr requires integer args");
  }
  const auto in = GetExecInputs(batch);
  ARROW_ASSIGN_OR_RAISE(auto lhs_reader, MakeIntegerReader(*lhs_type, in.lhs_array, in.lhs_scalar));
  ARROW_ASSIGN_OR_RAISE(auto rhs_reader, MakeIntegerReader(*rhs_type, in.rhs_array, in.rhs_scalar));
  const auto out_type = arrow::uint64();
  return std::visit(
      [&](const auto& lhs, const auto& rhs) -> arrow::Status {
        const auto compute = [&](int64_t i) -> arrow::Result<std::optional<uint64_t>> {
          return std::optional<uint64_t>(ToUInt64(lhs.Value(i)) | ToUInt64(rhs.Value(i)));
        };
        return ExecBinaryLoop<arrow::UInt64Builder>(ctx, batch, out, out_type, lhs, rhs, compute);
      },
      lhs_reader, rhs_reader);
}

arrow::Status ExecBitXor(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                         arrow::compute::ExecResult* out) {
  if (ctx == nullptr || out == nullptr) {
    return arrow::Status::Invalid("kernel context/result must not be null");
  }
  if (batch.num_values() != 2) {
    return arrow::Status::Invalid("bitXor requires 2 args");
  }
  const auto* lhs_type = batch[0].type();
  const auto* rhs_type = batch[1].type();
  if (lhs_type == nullptr || rhs_type == nullptr) {
    return arrow::Status::Invalid("bitXor input type must not be null");
  }
  if (!IsIntegerType(lhs_type->id()) || !IsIntegerType(rhs_type->id())) {
    return arrow::Status::Invalid("bitXor requires integer args");
  }
  const auto in = GetExecInputs(batch);
  ARROW_ASSIGN_OR_RAISE(auto lhs_reader, MakeIntegerReader(*lhs_type, in.lhs_array, in.lhs_scalar));
  ARROW_ASSIGN_OR_RAISE(auto rhs_reader, MakeIntegerReader(*rhs_type, in.rhs_array, in.rhs_scalar));
  const auto out_type = arrow::uint64();
  return std::visit(
      [&](const auto& lhs, const auto& rhs) -> arrow::Status {
        const auto compute = [&](int64_t i) -> arrow::Result<std::optional<uint64_t>> {
          return std::optional<uint64_t>(ToUInt64(lhs.Value(i)) ^ ToUInt64(rhs.Value(i)));
        };
        return ExecBinaryLoop<arrow::UInt64Builder>(ctx, batch, out, out_type, lhs, rhs, compute);
      },
      lhs_reader, rhs_reader);
}

arrow::Status ExecBitShiftLeft(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                               arrow::compute::ExecResult* out) {
  if (ctx == nullptr || out == nullptr) {
    return arrow::Status::Invalid("kernel context/result must not be null");
  }
  if (batch.num_values() != 2) {
    return arrow::Status::Invalid("bitShiftLeft requires 2 args");
  }
  const auto* lhs_type = batch[0].type();
  const auto* rhs_type = batch[1].type();
  if (lhs_type == nullptr || rhs_type == nullptr) {
    return arrow::Status::Invalid("bitShiftLeft input type must not be null");
  }
  if (!IsIntegerType(lhs_type->id()) || !IsIntegerType(rhs_type->id())) {
    return arrow::Status::Invalid("bitShiftLeft requires integer args");
  }
  const auto in = GetExecInputs(batch);
  ARROW_ASSIGN_OR_RAISE(auto lhs_reader, MakeIntegerReader(*lhs_type, in.lhs_array, in.lhs_scalar));
  ARROW_ASSIGN_OR_RAISE(auto rhs_reader, MakeIntegerReader(*rhs_type, in.rhs_array, in.rhs_scalar));
  const auto out_type = arrow::uint64();
  return std::visit(
      [&](const auto& lhs, const auto& rhs) -> arrow::Status {
        const auto compute = [&](int64_t i) -> arrow::Result<std::optional<uint64_t>> {
          const uint64_t shift = ToUInt64(rhs.Value(i));
          if (shift >= 64) {
            return std::optional<uint64_t>(0);
          }
          return std::optional<uint64_t>(ToUInt64(lhs.Value(i)) << static_cast<uint32_t>(shift));
        };
        return ExecBinaryLoop<arrow::UInt64Builder>(ctx, batch, out, out_type, lhs, rhs, compute);
      },
      lhs_reader, rhs_reader);
}

arrow::Status ExecBitShiftRight(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                                arrow::compute::ExecResult* out) {
  if (ctx == nullptr || out == nullptr) {
    return arrow::Status::Invalid("kernel context/result must not be null");
  }
  if (batch.num_values() != 2) {
    return arrow::Status::Invalid("bitShiftRight requires 2 args");
  }
  const auto* lhs_type = batch[0].type();
  const auto* rhs_type = batch[1].type();
  if (lhs_type == nullptr || rhs_type == nullptr) {
    return arrow::Status::Invalid("bitShiftRight input type must not be null");
  }
  if (!IsIntegerType(lhs_type->id()) || !IsIntegerType(rhs_type->id())) {
    return arrow::Status::Invalid("bitShiftRight requires integer args");
  }
  const auto in = GetExecInputs(batch);
  ARROW_ASSIGN_OR_RAISE(auto lhs_reader, MakeIntegerReader(*lhs_type, in.lhs_array, in.lhs_scalar));
  ARROW_ASSIGN_OR_RAISE(auto rhs_reader, MakeIntegerReader(*rhs_type, in.rhs_array, in.rhs_scalar));
  const auto out_type = arrow::uint64();
  return std::visit(
      [&](const auto& lhs, const auto& rhs) -> arrow::Status {
        const auto compute = [&](int64_t i) -> arrow::Result<std::optional<uint64_t>> {
          const uint64_t shift = ToUInt64(rhs.Value(i));
          if (shift >= 64) {
            return std::optional<uint64_t>(0);
          }
          return std::optional<uint64_t>(ToUInt64(lhs.Value(i)) >> static_cast<uint32_t>(shift));
        };
        return ExecBinaryLoop<arrow::UInt64Builder>(ctx, batch, out, out_type, lhs, rhs, compute);
      },
      lhs_reader, rhs_reader);
}

arrow::Status ExecBitNot(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                         arrow::compute::ExecResult* out) {
  if (ctx == nullptr || out == nullptr) {
    return arrow::Status::Invalid("kernel context/result must not be null");
  }
  if (batch.num_values() != 1) {
    return arrow::Status::Invalid("bitNot requires 1 arg");
  }
  const auto* in_type = batch[0].type();
  if (in_type == nullptr) {
    return arrow::Status::Invalid("bitNot input type must not be null");
  }
  if (!IsIntegerType(in_type->id())) {
    return arrow::Status::Invalid("bitNot requires integer arg");
  }
  const auto in = GetExecInput(batch);
  ARROW_ASSIGN_OR_RAISE(auto reader, MakeIntegerReader(*in_type, in.array, in.scalar));
  const auto out_type = arrow::uint64();
  return std::visit(
      [&](const auto& arg) -> arrow::Status {
        const auto compute = [&](int64_t i) -> arrow::Result<std::optional<uint64_t>> {
          return std::optional<uint64_t>(~ToUInt64(arg.Value(i)));
        };
        return ExecUnaryLoop<arrow::UInt64Builder>(ctx, batch, out, out_type, arg, compute);
      },
      reader);
}

arrow::Status ExecAbs(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                      arrow::compute::ExecResult* out) {
  if (ctx == nullptr || out == nullptr) {
    return arrow::Status::Invalid("kernel context/result must not be null");
  }
  if (batch.num_values() != 1) {
    return arrow::Status::Invalid("abs requires 1 arg");
  }
  const auto* in_type = batch[0].type();
  if (in_type == nullptr) {
    return arrow::Status::Invalid("abs input type must not be null");
  }

  const auto id = in_type->id();
  const auto in = GetExecInput(batch);
  ARROW_ASSIGN_OR_RAISE(auto reader, MakeExtendedNumericReader(*in_type, in.array, in.scalar));

  if (IsSignedIntegerType(id)) {
    ARROW_ASSIGN_OR_RAISE(const auto bytes, ByteWidthOf(id));
    ARROW_ASSIGN_OR_RAISE(auto out_type, MakeIntegerType(/*is_signed=*/false, bytes));
    return std::visit(
        [&](const auto& arg) -> arrow::Status {
          using InT = decltype(arg.Value(0));
          if constexpr (!std::is_integral_v<InT> || std::is_unsigned_v<InT>) {
            return arrow::Status::Invalid("abs signed-integer kernel type mismatch");
          } else {
            const auto compute_abs_u64 = [&](int64_t i) -> arrow::Result<uint64_t> {
              const int64_t v = static_cast<int64_t>(arg.Value(i));
              if (bytes == 8 && v == std::numeric_limits<int64_t>::min()) {
                return arrow::Status::Invalid(
                    "BIGINT value is out of range in 'abs(-9223372036854775808)'");
              }
              return v < 0 ? static_cast<uint64_t>(-v) : static_cast<uint64_t>(v);
            };
            switch (out_type->id()) {
              case arrow::Type::UINT8: {
                const auto compute = [&](int64_t i) -> arrow::Result<std::optional<uint8_t>> {
                  ARROW_ASSIGN_OR_RAISE(const auto abs_v, compute_abs_u64(i));
                  return std::optional<uint8_t>(static_cast<uint8_t>(abs_v));
                };
                return ExecUnaryLoop<arrow::UInt8Builder>(ctx, batch, out, out_type, arg, compute);
              }
              case arrow::Type::UINT16: {
                const auto compute = [&](int64_t i) -> arrow::Result<std::optional<uint16_t>> {
                  ARROW_ASSIGN_OR_RAISE(const auto abs_v, compute_abs_u64(i));
                  return std::optional<uint16_t>(static_cast<uint16_t>(abs_v));
                };
                return ExecUnaryLoop<arrow::UInt16Builder>(ctx, batch, out, out_type, arg, compute);
              }
              case arrow::Type::UINT32: {
                const auto compute = [&](int64_t i) -> arrow::Result<std::optional<uint32_t>> {
                  ARROW_ASSIGN_OR_RAISE(const auto abs_v, compute_abs_u64(i));
                  return std::optional<uint32_t>(static_cast<uint32_t>(abs_v));
                };
                return ExecUnaryLoop<arrow::UInt32Builder>(ctx, batch, out, out_type, arg, compute);
              }
              case arrow::Type::UINT64: {
                const auto compute = [&](int64_t i) -> arrow::Result<std::optional<uint64_t>> {
                  ARROW_ASSIGN_OR_RAISE(const auto abs_v, compute_abs_u64(i));
                  return std::optional<uint64_t>(abs_v);
                };
                return ExecUnaryLoop<arrow::UInt64Builder>(ctx, batch, out, out_type, arg, compute);
              }
              default:
                break;
            }
            return arrow::Status::Invalid("abs produced unsupported output type: ", out_type->ToString());
          }
        },
        reader);
  }

  if (IsUnsignedIntegerType(id)) {
    ARROW_ASSIGN_OR_RAISE(const auto bytes, ByteWidthOf(id));
    ARROW_ASSIGN_OR_RAISE(auto out_type, MakeIntegerType(/*is_signed=*/false, bytes));
    return std::visit(
        [&](const auto& arg) -> arrow::Status {
          using InT = decltype(arg.Value(0));
          if constexpr (!std::is_integral_v<InT> || std::is_signed_v<InT>) {
            return arrow::Status::Invalid("abs unsigned-integer kernel type mismatch");
          } else {
            switch (out_type->id()) {
              case arrow::Type::UINT8: {
                const auto compute = [&](int64_t i) -> arrow::Result<std::optional<uint8_t>> {
                  return std::optional<uint8_t>(static_cast<uint8_t>(arg.Value(i)));
                };
                return ExecUnaryLoop<arrow::UInt8Builder>(ctx, batch, out, out_type, arg, compute);
              }
              case arrow::Type::UINT16: {
                const auto compute = [&](int64_t i) -> arrow::Result<std::optional<uint16_t>> {
                  return std::optional<uint16_t>(static_cast<uint16_t>(arg.Value(i)));
                };
                return ExecUnaryLoop<arrow::UInt16Builder>(ctx, batch, out, out_type, arg, compute);
              }
              case arrow::Type::UINT32: {
                const auto compute = [&](int64_t i) -> arrow::Result<std::optional<uint32_t>> {
                  return std::optional<uint32_t>(static_cast<uint32_t>(arg.Value(i)));
                };
                return ExecUnaryLoop<arrow::UInt32Builder>(ctx, batch, out, out_type, arg, compute);
              }
              case arrow::Type::UINT64: {
                const auto compute = [&](int64_t i) -> arrow::Result<std::optional<uint64_t>> {
                  return std::optional<uint64_t>(static_cast<uint64_t>(arg.Value(i)));
                };
                return ExecUnaryLoop<arrow::UInt64Builder>(ctx, batch, out, out_type, arg, compute);
              }
              default:
                break;
            }
            return arrow::Status::Invalid("abs produced unsupported output type: ", out_type->ToString());
          }
        },
        reader);
  }

  if (id == arrow::Type::FLOAT) {
    const auto out_type = arrow::float32();
    return std::visit(
        [&](const auto& arg) -> arrow::Status {
          using InT = decltype(arg.Value(0));
          if constexpr (!std::is_same_v<InT, float>) {
            return arrow::Status::Invalid("abs float kernel type mismatch");
          } else {
            const auto compute = [&](int64_t i) -> arrow::Result<std::optional<float>> {
              return std::optional<float>(std::abs(arg.Value(i)));
            };
            return ExecUnaryLoop<arrow::FloatBuilder>(ctx, batch, out, out_type, arg, compute);
          }
        },
        reader);
  }

  if (id == arrow::Type::DOUBLE) {
    const auto out_type = arrow::float64();
    return std::visit(
        [&](const auto& arg) -> arrow::Status {
          using InT = decltype(arg.Value(0));
          if constexpr (!std::is_same_v<InT, double>) {
            return arrow::Status::Invalid("abs double kernel type mismatch");
          } else {
            const auto compute = [&](int64_t i) -> arrow::Result<std::optional<double>> {
              return std::optional<double>(std::abs(arg.Value(i)));
            };
            return ExecUnaryLoop<arrow::DoubleBuilder>(ctx, batch, out, out_type, arg, compute);
          }
        },
        reader);
  }

  if (id == arrow::Type::DECIMAL128) {
    const auto& dec = static_cast<const arrow::DecimalType&>(*in_type);
    const auto out_type = arrow::decimal128(dec.precision(), dec.scale());
    return std::visit(
        [&](const auto& arg) -> arrow::Status {
          using InT = decltype(arg.Value(0));
          if constexpr (!std::is_same_v<InT, arrow::Decimal128>) {
            return arrow::Status::Invalid("abs decimal128 kernel type mismatch");
          } else {
            const auto compute = [&](int64_t i) -> arrow::Result<std::optional<arrow::Decimal128>> {
              auto v = arg.Value(i);
              if (v.Sign() < 0) {
                v = -v;
              }
              return std::optional<arrow::Decimal128>(v);
            };
            return ExecUnaryLoop<arrow::Decimal128Builder>(ctx, batch, out, out_type, arg, compute);
          }
        },
        reader);
  }

  if (id == arrow::Type::DECIMAL256) {
    const auto& dec = static_cast<const arrow::DecimalType&>(*in_type);
    const auto out_type = arrow::decimal256(dec.precision(), dec.scale());
    return std::visit(
        [&](const auto& arg) -> arrow::Status {
          using InT = decltype(arg.Value(0));
          if constexpr (!std::is_same_v<InT, arrow::Decimal256>) {
            return arrow::Status::Invalid("abs decimal256 kernel type mismatch");
          } else {
            const auto compute = [&](int64_t i) -> arrow::Result<std::optional<arrow::Decimal256>> {
              auto v = arg.Value(i);
              if (v.Sign() < 0) {
                v = -v;
              }
              return std::optional<arrow::Decimal256>(v);
            };
            return ExecUnaryLoop<arrow::Decimal256Builder>(ctx, batch, out, out_type, arg, compute);
          }
        },
        reader);
  }

  return arrow::Status::Invalid("abs requires numeric/decimal arg");
}

arrow::Status ExecNegate(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                         arrow::compute::ExecResult* out) {
  if (ctx == nullptr || out == nullptr) {
    return arrow::Status::Invalid("kernel context/result must not be null");
  }
  if (batch.num_values() != 1) {
    return arrow::Status::Invalid("negate requires 1 arg");
  }
  const auto* in_type = batch[0].type();
  if (in_type == nullptr) {
    return arrow::Status::Invalid("negate input type must not be null");
  }
  const auto id = in_type->id();
  const auto in = GetExecInput(batch);
  ARROW_ASSIGN_OR_RAISE(auto reader, MakeExtendedNumericReader(*in_type, in.array, in.scalar));

  if (IsSignedIntegerType(id)) {
    ARROW_ASSIGN_OR_RAISE(const auto bytes, ByteWidthOf(id));
    ARROW_ASSIGN_OR_RAISE(auto out_type, MakeIntegerType(/*is_signed=*/true, bytes));
    return std::visit(
        [&](const auto& arg) -> arrow::Status {
          using InT = decltype(arg.Value(0));
          if constexpr (!std::is_integral_v<InT> || std::is_unsigned_v<InT>) {
            return arrow::Status::Invalid("negate signed-integer kernel type mismatch");
          } else {
            const auto negate_twos_complement = [](auto v) {
              using SignedT = std::decay_t<decltype(v)>;
              using UnsignedT = std::make_unsigned_t<SignedT>;
              const UnsignedT u = static_cast<UnsignedT>(v);
              const UnsignedT neg_u = static_cast<UnsignedT>(0) - u;
              return std::bit_cast<SignedT>(neg_u);
            };
            switch (out_type->id()) {
              case arrow::Type::INT8: {
                const auto compute = [&](int64_t i) -> arrow::Result<std::optional<int8_t>> {
                  return std::optional<int8_t>(negate_twos_complement(static_cast<int8_t>(arg.Value(i))));
                };
                return ExecUnaryLoop<arrow::Int8Builder>(ctx, batch, out, out_type, arg, compute);
              }
              case arrow::Type::INT16: {
                const auto compute = [&](int64_t i) -> arrow::Result<std::optional<int16_t>> {
                  return std::optional<int16_t>(negate_twos_complement(static_cast<int16_t>(arg.Value(i))));
                };
                return ExecUnaryLoop<arrow::Int16Builder>(ctx, batch, out, out_type, arg, compute);
              }
              case arrow::Type::INT32: {
                const auto compute = [&](int64_t i) -> arrow::Result<std::optional<int32_t>> {
                  return std::optional<int32_t>(negate_twos_complement(static_cast<int32_t>(arg.Value(i))));
                };
                return ExecUnaryLoop<arrow::Int32Builder>(ctx, batch, out, out_type, arg, compute);
              }
              case arrow::Type::INT64: {
                const auto compute = [&](int64_t i) -> arrow::Result<std::optional<int64_t>> {
                  return std::optional<int64_t>(negate_twos_complement(static_cast<int64_t>(arg.Value(i))));
                };
                return ExecUnaryLoop<arrow::Int64Builder>(ctx, batch, out, out_type, arg, compute);
              }
              default:
                break;
            }
            return arrow::Status::Invalid("negate produced unsupported output type: ", out_type->ToString());
          }
        },
        reader);
  }

  if (IsUnsignedIntegerType(id)) {
    ARROW_ASSIGN_OR_RAISE(const auto bytes, ByteWidthOf(id));
    ARROW_ASSIGN_OR_RAISE(auto out_type, MakeIntegerType(/*is_signed=*/true, NextSizeBytes(bytes)));
    return std::visit(
        [&](const auto& arg) -> arrow::Status {
          using InT = decltype(arg.Value(0));
          if constexpr (!std::is_integral_v<InT> || std::is_signed_v<InT>) {
            return arrow::Status::Invalid("negate unsigned-integer kernel type mismatch");
          } else {
            switch (out_type->id()) {
              case arrow::Type::INT16: {
                const auto compute = [&](int64_t i) -> arrow::Result<std::optional<int16_t>> {
                  return std::optional<int16_t>(static_cast<int16_t>(-static_cast<int32_t>(arg.Value(i))));
                };
                return ExecUnaryLoop<arrow::Int16Builder>(ctx, batch, out, out_type, arg, compute);
              }
              case arrow::Type::INT32: {
                const auto compute = [&](int64_t i) -> arrow::Result<std::optional<int32_t>> {
                  return std::optional<int32_t>(static_cast<int32_t>(-static_cast<int64_t>(arg.Value(i))));
                };
                return ExecUnaryLoop<arrow::Int32Builder>(ctx, batch, out, out_type, arg, compute);
              }
              case arrow::Type::INT64: {
                const auto compute = [&](int64_t i) -> arrow::Result<std::optional<int64_t>> {
                  const uint64_t u = static_cast<uint64_t>(arg.Value(i));
                  const uint64_t neg_u = 0ULL - u;
                  return std::optional<int64_t>(std::bit_cast<int64_t>(neg_u));
                };
                return ExecUnaryLoop<arrow::Int64Builder>(ctx, batch, out, out_type, arg, compute);
              }
              default:
                break;
            }
            return arrow::Status::Invalid("negate produced unsupported output type: ", out_type->ToString());
          }
        },
        reader);
  }

  if (id == arrow::Type::FLOAT) {
    const auto out_type = arrow::float32();
    return std::visit(
        [&](const auto& arg) -> arrow::Status {
          using InT = decltype(arg.Value(0));
          if constexpr (!std::is_same_v<InT, float>) {
            return arrow::Status::Invalid("negate float kernel type mismatch");
          } else {
            const auto compute = [&](int64_t i) -> arrow::Result<std::optional<float>> {
              return std::optional<float>(-arg.Value(i));
            };
            return ExecUnaryLoop<arrow::FloatBuilder>(ctx, batch, out, out_type, arg, compute);
          }
        },
        reader);
  }

  if (id == arrow::Type::DOUBLE) {
    const auto out_type = arrow::float64();
    return std::visit(
        [&](const auto& arg) -> arrow::Status {
          using InT = decltype(arg.Value(0));
          if constexpr (!std::is_same_v<InT, double>) {
            return arrow::Status::Invalid("negate double kernel type mismatch");
          } else {
            const auto compute = [&](int64_t i) -> arrow::Result<std::optional<double>> {
              return std::optional<double>(-arg.Value(i));
            };
            return ExecUnaryLoop<arrow::DoubleBuilder>(ctx, batch, out, out_type, arg, compute);
          }
        },
        reader);
  }

  if (id == arrow::Type::DECIMAL128) {
    const auto& dec = static_cast<const arrow::DecimalType&>(*in_type);
    const auto out_type = arrow::decimal128(dec.precision(), dec.scale());
    return std::visit(
        [&](const auto& arg) -> arrow::Status {
          using InT = decltype(arg.Value(0));
          if constexpr (!std::is_same_v<InT, arrow::Decimal128>) {
            return arrow::Status::Invalid("negate decimal128 kernel type mismatch");
          } else {
            const auto compute = [&](int64_t i) -> arrow::Result<std::optional<arrow::Decimal128>> {
              return std::optional<arrow::Decimal128>(-arg.Value(i));
            };
            return ExecUnaryLoop<arrow::Decimal128Builder>(ctx, batch, out, out_type, arg, compute);
          }
        },
        reader);
  }

  if (id == arrow::Type::DECIMAL256) {
    const auto& dec = static_cast<const arrow::DecimalType&>(*in_type);
    const auto out_type = arrow::decimal256(dec.precision(), dec.scale());
    return std::visit(
        [&](const auto& arg) -> arrow::Status {
          using InT = decltype(arg.Value(0));
          if constexpr (!std::is_same_v<InT, arrow::Decimal256>) {
            return arrow::Status::Invalid("negate decimal256 kernel type mismatch");
          } else {
            const auto compute = [&](int64_t i) -> arrow::Result<std::optional<arrow::Decimal256>> {
              return std::optional<arrow::Decimal256>(-arg.Value(i));
            };
            return ExecUnaryLoop<arrow::Decimal256Builder>(ctx, batch, out, out_type, arg, compute);
          }
        },
        reader);
  }

  return arrow::Status::Invalid("negate requires numeric/decimal arg");
}

template <typename SignedT>
arrow::Result<SignedT> CheckedDiv(SignedT a, SignedT b) {
  if (b == 0) {
    return arrow::Status::Invalid("Division by zero");
  }
  if constexpr (std::is_signed_v<SignedT>) {
    if (a == std::numeric_limits<SignedT>::min() && b == static_cast<SignedT>(-1)) {
      return arrow::Status::Invalid("Division of minimal signed number by minus one");
    }
  }
  return static_cast<SignedT>(a / b);
}

arrow::Status ExecIntDiv(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                         arrow::compute::ExecResult* out, bool or_zero) {
  if (ctx == nullptr || out == nullptr) {
    return arrow::Status::Invalid("kernel context/result must not be null");
  }
  if (batch.num_values() != 2) {
    return arrow::Status::Invalid("intDiv requires 2 args");
  }
  const auto* lhs_type = batch[0].type();
  const auto* rhs_type = batch[1].type();
  if (lhs_type == nullptr || rhs_type == nullptr) {
    return arrow::Status::Invalid("intDiv input type must not be null");
  }
  if (!IsIntegerType(lhs_type->id()) || !IsIntegerType(rhs_type->id())) {
    return arrow::Status::Invalid("intDiv requires integer args");
  }
  const auto in = GetExecInputs(batch);
  ARROW_ASSIGN_OR_RAISE(auto lhs_reader, MakeIntegerReader(*lhs_type, in.lhs_array, in.lhs_scalar));
  ARROW_ASSIGN_OR_RAISE(auto rhs_reader, MakeIntegerReader(*rhs_type, in.rhs_array, in.rhs_scalar));

  ARROW_ASSIGN_OR_RAISE(const auto lhs_bytes, ByteWidthOf(lhs_type->id()));
  ARROW_ASSIGN_OR_RAISE(auto out_type, MakeIntegerType(
                                          /*is_signed=*/IsSignedIntegerType(lhs_type->id()) ||
                                              IsSignedIntegerType(rhs_type->id()),
                                          lhs_bytes));

  const auto exec_signed = [&](auto out_type_tag) -> arrow::Status {
    using OutT = decltype(out_type_tag);
    using BuilderT = std::conditional_t<std::is_same_v<OutT, int8_t>, arrow::Int8Builder,
                                        std::conditional_t<std::is_same_v<OutT, int16_t>, arrow::Int16Builder,
                                                           std::conditional_t<std::is_same_v<OutT, int32_t>,
                                                                              arrow::Int32Builder, arrow::Int64Builder>>>;
    return std::visit(
        [&](const auto& lhs, const auto& rhs) -> arrow::Status {
          const auto compute = [&](int64_t i) -> arrow::Result<std::optional<OutT>> {
            const OutT a = static_cast<OutT>(lhs.Value(i));
            const OutT b = static_cast<OutT>(rhs.Value(i));
            if (b == 0) {
              if (or_zero) {
                return std::optional<OutT>(static_cast<OutT>(0));
              }
              return arrow::Status::Invalid("Division by zero");
            }
            if (a == std::numeric_limits<OutT>::min() && b == static_cast<OutT>(-1)) {
              if (or_zero) {
                return std::optional<OutT>(static_cast<OutT>(0));
              }
              return arrow::Status::Invalid("Division of minimal signed number by minus one");
            }
            return std::optional<OutT>(static_cast<OutT>(a / b));
          };
          return ExecBinaryLoop<BuilderT>(ctx, batch, out, out_type, lhs, rhs, compute);
        },
        lhs_reader, rhs_reader);
  };

  const auto exec_unsigned = [&](auto out_type_tag) -> arrow::Status {
    using OutT = decltype(out_type_tag);
    using BuilderT = std::conditional_t<std::is_same_v<OutT, uint8_t>, arrow::UInt8Builder,
                                        std::conditional_t<std::is_same_v<OutT, uint16_t>, arrow::UInt16Builder,
                                                           std::conditional_t<std::is_same_v<OutT, uint32_t>,
                                                                              arrow::UInt32Builder, arrow::UInt64Builder>>>;
    return std::visit(
        [&](const auto& lhs, const auto& rhs) -> arrow::Status {
          const auto compute = [&](int64_t i) -> arrow::Result<std::optional<OutT>> {
            const OutT a = static_cast<OutT>(lhs.Value(i));
            const OutT b = static_cast<OutT>(rhs.Value(i));
            if (b == 0) {
              if (or_zero) {
                return std::optional<OutT>(static_cast<OutT>(0));
              }
              return arrow::Status::Invalid("Division by zero");
            }
            return std::optional<OutT>(static_cast<OutT>(a / b));
          };
          return ExecBinaryLoop<BuilderT>(ctx, batch, out, out_type, lhs, rhs, compute);
        },
        lhs_reader, rhs_reader);
  };

  switch (out_type->id()) {
    case arrow::Type::INT8:
      return exec_signed(int8_t{});
    case arrow::Type::INT16:
      return exec_signed(int16_t{});
    case arrow::Type::INT32:
      return exec_signed(int32_t{});
    case arrow::Type::INT64:
      return exec_signed(int64_t{});
    case arrow::Type::UINT8:
      return exec_unsigned(uint8_t{});
    case arrow::Type::UINT16:
      return exec_unsigned(uint16_t{});
    case arrow::Type::UINT32:
      return exec_unsigned(uint32_t{});
    case arrow::Type::UINT64:
      return exec_unsigned(uint64_t{});
    default:
      break;
  }
  return arrow::Status::Invalid("intDiv produced unsupported output type: ", out_type->ToString());
}

arrow::Status ExecIntDivStrict(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                               arrow::compute::ExecResult* out) {
  return ExecIntDiv(ctx, batch, out, /*or_zero=*/false);
}

arrow::Status ExecIntDivOrZero(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                               arrow::compute::ExecResult* out) {
  return ExecIntDiv(ctx, batch, out, /*or_zero=*/true);
}

arrow::Status ExecModulo(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                         arrow::compute::ExecResult* out) {
  if (ctx == nullptr || out == nullptr) {
    return arrow::Status::Invalid("kernel context/result must not be null");
  }
  if (batch.num_values() != 2) {
    return arrow::Status::Invalid("modulo requires 2 args");
  }
  const auto* lhs_type = batch[0].type();
  const auto* rhs_type = batch[1].type();
  if (lhs_type == nullptr || rhs_type == nullptr) {
    return arrow::Status::Invalid("modulo input type must not be null");
  }
  if (IsDecimalType(lhs_type->id()) || IsDecimalType(rhs_type->id())) {
    return arrow::Status::Invalid("modulo requires non-decimal args (decimal cases are rewritten)");
  }
  if (!IsIntegerType(lhs_type->id()) && !IsFloatingType(lhs_type->id())) {
    return arrow::Status::Invalid("modulo lhs must be numeric");
  }
  if (!IsIntegerType(rhs_type->id()) && !IsFloatingType(rhs_type->id())) {
    return arrow::Status::Invalid("modulo rhs must be numeric");
  }

  const auto in = GetExecInputs(batch);
  ARROW_ASSIGN_OR_RAISE(auto lhs_reader, MakeNumericReader(*lhs_type, in.lhs_array, in.lhs_scalar));
  ARROW_ASSIGN_OR_RAISE(auto rhs_reader, MakeNumericReader(*rhs_type, in.rhs_array, in.rhs_scalar));

  std::shared_ptr<arrow::DataType> out_type;
  if (IsFloatingType(lhs_type->id()) || IsFloatingType(rhs_type->id())) {
    out_type = arrow::float64();
  } else {
    ARROW_ASSIGN_OR_RAISE(const auto lhs_bytes, ByteWidthOf(lhs_type->id()));
    ARROW_ASSIGN_OR_RAISE(const auto rhs_bytes, ByteWidthOf(rhs_type->id()));
    ARROW_ASSIGN_OR_RAISE(out_type,
                          MakeIntegerType(/*is_signed=*/IsSignedIntegerType(lhs_type->id()),
                                          std::max(lhs_bytes, rhs_bytes)));
  }

  if (out_type->id() == arrow::Type::DOUBLE) {
    return std::visit(
        [&](const auto& lhs, const auto& rhs) -> arrow::Status {
          const auto compute = [&](int64_t i) -> arrow::Result<std::optional<double>> {
            const double a = static_cast<double>(lhs.Value(i));
            const double b = static_cast<double>(rhs.Value(i));
            if (b == 0.0) {
              return std::nullopt;
            }
            if (!std::isfinite(a) || !std::isfinite(b)) {
              return arrow::Status::Invalid("modulo requires finite float arguments");
            }
            return std::optional<double>(std::fmod(a, b));
          };
          return ExecBinaryLoop<arrow::DoubleBuilder>(ctx, batch, out, out_type, lhs, rhs, compute);
        },
        lhs_reader, rhs_reader);
  }

  if (!IsIntegerType(out_type->id())) {
    return arrow::Status::Invalid("modulo produced unsupported output type: ", out_type->ToString());
  }

  const bool lhs_signed = IsSignedIntegerType(lhs_type->id());
  const bool rhs_signed = IsSignedIntegerType(rhs_type->id());

  const auto exec_signed = [&](auto out_type_tag) -> arrow::Status {
    using OutT = decltype(out_type_tag);
    using BuilderT = std::conditional_t<std::is_same_v<OutT, int8_t>, arrow::Int8Builder,
                                        std::conditional_t<std::is_same_v<OutT, int16_t>, arrow::Int16Builder,
                                                           std::conditional_t<std::is_same_v<OutT, int32_t>,
                                                                              arrow::Int32Builder, arrow::Int64Builder>>>;
    return std::visit(
        [&](const auto& lhs, const auto& rhs) -> arrow::Status {
          const auto compute = [&](int64_t i) -> arrow::Result<std::optional<OutT>> {
            const int64_t a_i64 = static_cast<int64_t>(lhs.Value(i));
            const int64_t b_i64 = static_cast<int64_t>(rhs.Value(i));
            const uint64_t x = lhs_signed ? AbsToUInt64(a_i64) : static_cast<uint64_t>(lhs.Value(i));
            const uint64_t y = rhs_signed ? AbsToUInt64(b_i64) : static_cast<uint64_t>(rhs.Value(i));
            if (y == 0) {
              return std::nullopt;
            }
            const uint64_t rem = x % y;
            int64_t signed_rem = static_cast<int64_t>(rem);
            if (lhs_signed && a_i64 < 0) {
              signed_rem = -signed_rem;
            }
            return std::optional<OutT>(static_cast<OutT>(signed_rem));
          };
          return ExecBinaryLoop<BuilderT>(ctx, batch, out, out_type, lhs, rhs, compute);
        },
        lhs_reader, rhs_reader);
  };

  const auto exec_unsigned = [&](auto out_type_tag) -> arrow::Status {
    using OutT = decltype(out_type_tag);
    using BuilderT = std::conditional_t<std::is_same_v<OutT, uint8_t>, arrow::UInt8Builder,
                                        std::conditional_t<std::is_same_v<OutT, uint16_t>, arrow::UInt16Builder,
                                                           std::conditional_t<std::is_same_v<OutT, uint32_t>,
                                                                              arrow::UInt32Builder, arrow::UInt64Builder>>>;
    return std::visit(
        [&](const auto& lhs, const auto& rhs) -> arrow::Status {
          const auto compute = [&](int64_t i) -> arrow::Result<std::optional<OutT>> {
            const uint64_t x = lhs_signed ? AbsToUInt64(static_cast<int64_t>(lhs.Value(i)))
                                          : static_cast<uint64_t>(lhs.Value(i));
            const uint64_t y = rhs_signed ? AbsToUInt64(static_cast<int64_t>(rhs.Value(i)))
                                          : static_cast<uint64_t>(rhs.Value(i));
            if (y == 0) {
              return std::nullopt;
            }
            const uint64_t rem = x % y;
            return std::optional<OutT>(static_cast<OutT>(rem));
          };
          return ExecBinaryLoop<BuilderT>(ctx, batch, out, out_type, lhs, rhs, compute);
        },
        lhs_reader, rhs_reader);
  };

  switch (out_type->id()) {
    case arrow::Type::INT8:
      return exec_signed(int8_t{});
    case arrow::Type::INT16:
      return exec_signed(int16_t{});
    case arrow::Type::INT32:
      return exec_signed(int32_t{});
    case arrow::Type::INT64:
      return exec_signed(int64_t{});
    case arrow::Type::UINT8:
      return exec_unsigned(uint8_t{});
    case arrow::Type::UINT16:
      return exec_unsigned(uint16_t{});
    case arrow::Type::UINT32:
      return exec_unsigned(uint32_t{});
    case arrow::Type::UINT64:
      return exec_unsigned(uint64_t{});
    default:
      break;
  }

  return arrow::Status::Invalid("modulo produced unsupported output type: ", out_type->ToString());
}

arrow::Status ThrowIfDivisionLeadsToFpeForGcdLcm(arrow::Type::type dividend_type, int64_t dividend,
                                                arrow::Type::type divisor_type, int64_t divisor) {
  if (divisor == 0) {
    return arrow::Status::Invalid("Division by zero");
  }
  if (IsSignedIntegerType(dividend_type) || IsFloatingType(dividend_type)) {
    if (IsSignedIntegerType(divisor_type) || IsFloatingType(divisor_type)) {
      ARROW_ASSIGN_OR_RAISE(const auto min_v, MinSignedFor(dividend_type));
      if (dividend == min_v && divisor == -1) {
        return arrow::Status::Invalid("Division of minimal signed number by minus one");
      }
    }
  }
  return arrow::Status::OK();
}

uint64_t AbsToUInt64(int64_t v) {
  const uint64_t u = static_cast<uint64_t>(v);
  return v < 0 ? (0ULL - u) : u;
}

arrow::Status ExecGcdLcm(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                         arrow::compute::ExecResult* out, bool is_gcd) {
  if (ctx == nullptr || out == nullptr) {
    return arrow::Status::Invalid("kernel context/result must not be null");
  }
  if (batch.num_values() != 2) {
    return arrow::Status::Invalid("gcd/lcm requires 2 args");
  }
  const auto* lhs_type = batch[0].type();
  const auto* rhs_type = batch[1].type();
  if (lhs_type == nullptr || rhs_type == nullptr) {
    return arrow::Status::Invalid("gcd/lcm input type must not be null");
  }
  if (IsDecimalType(lhs_type->id()) || IsDecimalType(rhs_type->id())) {
    return arrow::Status::Invalid("gcd/lcm does not support decimal args");
  }
  if (!IsIntegerType(lhs_type->id()) && !IsFloatingType(lhs_type->id())) {
    return arrow::Status::Invalid("gcd/lcm lhs must be numeric");
  }
  if (!IsIntegerType(rhs_type->id()) && !IsFloatingType(rhs_type->id())) {
    return arrow::Status::Invalid("gcd/lcm rhs must be numeric");
  }

  const auto in = GetExecInputs(batch);
  ARROW_ASSIGN_OR_RAISE(auto lhs_reader, MakeNumericReader(*lhs_type, in.lhs_array, in.lhs_scalar));
  ARROW_ASSIGN_OR_RAISE(auto rhs_reader, MakeNumericReader(*rhs_type, in.rhs_array, in.rhs_scalar));

  std::shared_ptr<arrow::DataType> out_type;
  if (IsFloatingType(lhs_type->id()) || IsFloatingType(rhs_type->id())) {
    out_type = arrow::float64();
  } else {
    ARROW_ASSIGN_OR_RAISE(const auto lhs_bytes, ByteWidthOf(lhs_type->id()));
    ARROW_ASSIGN_OR_RAISE(const auto rhs_bytes, ByteWidthOf(rhs_type->id()));
    const int32_t out_bytes = NextSizeBytes(std::max(lhs_bytes, rhs_bytes));
    ARROW_ASSIGN_OR_RAISE(out_type,
                          MakeIntegerType(/*is_signed=*/IsSignedIntegerType(lhs_type->id()) ||
                                              IsSignedIntegerType(rhs_type->id()),
                                          out_bytes));
  }

  if (out_type->id() == arrow::Type::DOUBLE) {
    return std::visit(
        [&](const auto& lhs, const auto& rhs) -> arrow::Status {
          const auto compute = [&](int64_t i) -> arrow::Result<std::optional<double>> {
            const int64_t a = static_cast<int64_t>(lhs.Value(i));
            const int64_t b = static_cast<int64_t>(rhs.Value(i));
            ARROW_RETURN_NOT_OK(ThrowIfDivisionLeadsToFpeForGcdLcm(lhs_type->id(), a, rhs_type->id(), b));
            ARROW_RETURN_NOT_OK(ThrowIfDivisionLeadsToFpeForGcdLcm(rhs_type->id(), b, lhs_type->id(), a));
            const uint64_t x = AbsToUInt64(a);
            const uint64_t y = AbsToUInt64(b);
            const uint64_t g = std::gcd(x, y);
            if (is_gcd) {
              return std::optional<double>(static_cast<double>(g));
            }
            const uint64_t l = (x / g) * y;
            return std::optional<double>(static_cast<double>(l));
          };
          return ExecBinaryLoop<arrow::DoubleBuilder>(ctx, batch, out, out_type, lhs, rhs, compute);
        },
        lhs_reader, rhs_reader);
  }

  if (!IsIntegerType(out_type->id())) {
    return arrow::Status::Invalid("gcd/lcm produced unsupported output type: ", out_type->ToString());
  }

  const auto exec_signed = [&](auto out_type_tag) -> arrow::Status {
    using OutT = decltype(out_type_tag);
    using BuilderT = std::conditional_t<std::is_same_v<OutT, int16_t>, arrow::Int16Builder,
                                        std::conditional_t<std::is_same_v<OutT, int32_t>, arrow::Int32Builder,
                                                           arrow::Int64Builder>>;
    return std::visit(
        [&](const auto& lhs, const auto& rhs) -> arrow::Status {
          const auto compute = [&](int64_t i) -> arrow::Result<std::optional<OutT>> {
            const int64_t a_check = static_cast<int64_t>(lhs.Value(i));
            const int64_t b_check = static_cast<int64_t>(rhs.Value(i));
            ARROW_RETURN_NOT_OK(ThrowIfDivisionLeadsToFpeForGcdLcm(lhs_type->id(), a_check, rhs_type->id(), b_check));
            ARROW_RETURN_NOT_OK(ThrowIfDivisionLeadsToFpeForGcdLcm(rhs_type->id(), b_check, lhs_type->id(), a_check));

            const OutT a = static_cast<OutT>(lhs.Value(i));
            const OutT b = static_cast<OutT>(rhs.Value(i));
            const uint64_t x = AbsToUInt64(static_cast<int64_t>(a));
            const uint64_t y = AbsToUInt64(static_cast<int64_t>(b));
            const uint64_t g = std::gcd(x, y);
            uint64_t res = g;
            if (!is_gcd) {
              res = (x / g) * y;
            }
            return std::optional<OutT>(static_cast<OutT>(res));
          };
          return ExecBinaryLoop<BuilderT>(ctx, batch, out, out_type, lhs, rhs, compute);
        },
        lhs_reader, rhs_reader);
  };

  const auto exec_unsigned = [&](auto out_type_tag) -> arrow::Status {
    using OutT = decltype(out_type_tag);
    using BuilderT = std::conditional_t<std::is_same_v<OutT, uint16_t>, arrow::UInt16Builder,
                                        std::conditional_t<std::is_same_v<OutT, uint32_t>, arrow::UInt32Builder,
                                                           arrow::UInt64Builder>>;
    return std::visit(
        [&](const auto& lhs, const auto& rhs) -> arrow::Status {
          const auto compute = [&](int64_t i) -> arrow::Result<std::optional<OutT>> {
            const int64_t a_check = static_cast<int64_t>(lhs.Value(i));
            const int64_t b_check = static_cast<int64_t>(rhs.Value(i));
            ARROW_RETURN_NOT_OK(ThrowIfDivisionLeadsToFpeForGcdLcm(lhs_type->id(), a_check, rhs_type->id(), b_check));
            ARROW_RETURN_NOT_OK(ThrowIfDivisionLeadsToFpeForGcdLcm(rhs_type->id(), b_check, lhs_type->id(), a_check));

            const OutT a = static_cast<OutT>(lhs.Value(i));
            const OutT b = static_cast<OutT>(rhs.Value(i));
            const uint64_t x = static_cast<uint64_t>(a);
            const uint64_t y = static_cast<uint64_t>(b);
            const uint64_t g = std::gcd(x, y);
            uint64_t res = g;
            if (!is_gcd) {
              res = (x / g) * y;
            }
            return std::optional<OutT>(static_cast<OutT>(res));
          };
          return ExecBinaryLoop<BuilderT>(ctx, batch, out, out_type, lhs, rhs, compute);
        },
        lhs_reader, rhs_reader);
  };

  switch (out_type->id()) {
    case arrow::Type::INT16:
      return exec_signed(int16_t{});
    case arrow::Type::INT32:
      return exec_signed(int32_t{});
    case arrow::Type::INT64:
      return exec_signed(int64_t{});
    case arrow::Type::UINT16:
      return exec_unsigned(uint16_t{});
    case arrow::Type::UINT32:
      return exec_unsigned(uint32_t{});
    case arrow::Type::UINT64:
      return exec_unsigned(uint64_t{});
    default:
      break;
  }

  return arrow::Status::Invalid("gcd/lcm produced unsupported output type: ", out_type->ToString());
}

arrow::Status ExecGcd(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                      arrow::compute::ExecResult* out) {
  return ExecGcdLcm(ctx, batch, out, /*is_gcd=*/true);
}

arrow::Status ExecLcm(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                      arrow::compute::ExecResult* out) {
  return ExecGcdLcm(ctx, batch, out, /*is_gcd=*/false);
}

}  // namespace

arrow::Status RegisterNumericArithmeticFunctions(arrow::compute::FunctionRegistry* registry,
                                                arrow::compute::FunctionRegistry* fallback_registry) {
  if (registry == nullptr) {
    return arrow::Status::Invalid("function registry must not be null");
  }
  if (fallback_registry == nullptr) {
    return arrow::Status::Invalid("fallback function registry must not be null");
  }

  const auto make_unary_kernel = [](arrow::Type::type arg_id, arrow::compute::OutputType::Resolver out_resolver,
                                    arrow::compute::ArrayKernelExec exec) -> arrow::compute::ScalarKernel {
    arrow::compute::ScalarKernel kernel({arrow::compute::InputType(arg_id)}, arrow::compute::OutputType(std::move(out_resolver)),
                                        std::move(exec));
    kernel.null_handling = arrow::compute::NullHandling::COMPUTED_NO_PREALLOCATE;
    kernel.mem_allocation = arrow::compute::MemAllocation::NO_PREALLOCATE;
    kernel.can_write_into_slices = false;
    return kernel;
  };
  const auto make_binary_kernel = [](arrow::Type::type lhs_id, arrow::Type::type rhs_id,
                                     arrow::compute::OutputType::Resolver out_resolver,
                                     arrow::compute::ArrayKernelExec exec) -> arrow::compute::ScalarKernel {
    arrow::compute::ScalarKernel kernel({arrow::compute::InputType(lhs_id), arrow::compute::InputType(rhs_id)},
                                        arrow::compute::OutputType(std::move(out_resolver)), std::move(exec));
    kernel.null_handling = arrow::compute::NullHandling::COMPUTED_NO_PREALLOCATE;
    kernel.mem_allocation = arrow::compute::MemAllocation::NO_PREALLOCATE;
    kernel.can_write_into_slices = false;
    return kernel;
  };

  const std::vector<arrow::Type::type> int_ids = {
      arrow::Type::INT8,  arrow::Type::UINT8,  arrow::Type::INT16, arrow::Type::UINT16,
      arrow::Type::INT32, arrow::Type::UINT32, arrow::Type::INT64, arrow::Type::UINT64,
  };
  const std::vector<arrow::Type::type> numeric_ids = {
      arrow::Type::INT8,  arrow::Type::UINT8,  arrow::Type::INT16, arrow::Type::UINT16, arrow::Type::INT32,
      arrow::Type::UINT32, arrow::Type::INT64, arrow::Type::UINT64, arrow::Type::FLOAT, arrow::Type::DOUBLE,
  };

  auto bit_and_fn = std::make_shared<arrow::compute::ScalarFunction>(
      "bitAnd", arrow::compute::Arity::Binary(), arrow::compute::FunctionDoc::Empty());
  auto bit_or_fn = std::make_shared<arrow::compute::ScalarFunction>(
      "bitOr", arrow::compute::Arity::Binary(), arrow::compute::FunctionDoc::Empty());
  auto bit_xor_fn = std::make_shared<arrow::compute::ScalarFunction>(
      "bitXor", arrow::compute::Arity::Binary(), arrow::compute::FunctionDoc::Empty());
  auto bit_shift_left_fn = std::make_shared<arrow::compute::ScalarFunction>(
      "bitShiftLeft", arrow::compute::Arity::Binary(), arrow::compute::FunctionDoc::Empty());
  auto bit_shift_right_fn = std::make_shared<arrow::compute::ScalarFunction>(
      "bitShiftRight", arrow::compute::Arity::Binary(), arrow::compute::FunctionDoc::Empty());
  for (const auto lhs_id : int_ids) {
    for (const auto rhs_id : int_ids) {
      ARROW_RETURN_NOT_OK(bit_and_fn->AddKernel(make_binary_kernel(lhs_id, rhs_id, ResolveBitwiseOutputType, ExecBitAnd)));
      ARROW_RETURN_NOT_OK(bit_or_fn->AddKernel(make_binary_kernel(lhs_id, rhs_id, ResolveBitwiseOutputType, ExecBitOr)));
      ARROW_RETURN_NOT_OK(bit_xor_fn->AddKernel(make_binary_kernel(lhs_id, rhs_id, ResolveBitwiseOutputType, ExecBitXor)));
      ARROW_RETURN_NOT_OK(
          bit_shift_left_fn->AddKernel(make_binary_kernel(lhs_id, rhs_id, ResolveBitwiseOutputType, ExecBitShiftLeft)));
      ARROW_RETURN_NOT_OK(bit_shift_right_fn->AddKernel(
          make_binary_kernel(lhs_id, rhs_id, ResolveBitwiseOutputType, ExecBitShiftRight)));
    }
  }
  ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(bit_and_fn), /*allow_overwrite=*/true));
  ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(bit_or_fn), /*allow_overwrite=*/true));
  ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(bit_xor_fn), /*allow_overwrite=*/true));
  ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(bit_shift_left_fn), /*allow_overwrite=*/true));
  ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(bit_shift_right_fn), /*allow_overwrite=*/true));

  auto bit_not_fn = std::make_shared<arrow::compute::ScalarFunction>(
      "bitNot", arrow::compute::Arity::Unary(), arrow::compute::FunctionDoc::Empty());
  for (const auto id : int_ids) {
    ARROW_RETURN_NOT_OK(bit_not_fn->AddKernel(make_unary_kernel(id, ResolveBitNotOutputType, ExecBitNot)));
  }
  ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(bit_not_fn), /*allow_overwrite=*/true));

  auto abs_fn = std::make_shared<arrow::compute::ScalarFunction>(
      "abs", arrow::compute::Arity::Unary(), arrow::compute::FunctionDoc::Empty());
  for (const auto id : numeric_ids) {
    ARROW_RETURN_NOT_OK(abs_fn->AddKernel(make_unary_kernel(id, ResolveAbsOutputType, ExecAbs)));
  }
  for (const auto id : {arrow::Type::DECIMAL128, arrow::Type::DECIMAL256}) {
    ARROW_RETURN_NOT_OK(abs_fn->AddKernel(make_unary_kernel(id, ResolveAbsOutputType, ExecAbs)));
  }
  ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(abs_fn), /*allow_overwrite=*/true));

  auto negate_fn = std::make_shared<arrow::compute::ScalarFunction>(
      "negate", arrow::compute::Arity::Unary(), arrow::compute::FunctionDoc::Empty());
  for (const auto id : numeric_ids) {
    ARROW_RETURN_NOT_OK(negate_fn->AddKernel(make_unary_kernel(id, ResolveNegateOutputType, ExecNegate)));
  }
  for (const auto id : {arrow::Type::DECIMAL128, arrow::Type::DECIMAL256}) {
    ARROW_RETURN_NOT_OK(negate_fn->AddKernel(make_unary_kernel(id, ResolveNegateOutputType, ExecNegate)));
  }
  ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(negate_fn), /*allow_overwrite=*/true));

  auto modulo_fn = std::make_shared<arrow::compute::ScalarFunction>(
      "modulo", arrow::compute::Arity::Binary(), arrow::compute::FunctionDoc::Empty());
  for (const auto lhs_id : numeric_ids) {
    for (const auto rhs_id : numeric_ids) {
      ARROW_RETURN_NOT_OK(modulo_fn->AddKernel(make_binary_kernel(lhs_id, rhs_id, ResolveModuloOutputType, ExecModulo)));
    }
  }
  ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(modulo_fn), /*allow_overwrite=*/true));

  auto int_div_fn = std::make_shared<arrow::compute::ScalarFunction>(
      "intDiv", arrow::compute::Arity::Binary(), arrow::compute::FunctionDoc::Empty());
  auto int_div_or_zero_fn = std::make_shared<arrow::compute::ScalarFunction>(
      "intDivOrZero", arrow::compute::Arity::Binary(), arrow::compute::FunctionDoc::Empty());
  for (const auto lhs_id : int_ids) {
    for (const auto rhs_id : int_ids) {
      ARROW_RETURN_NOT_OK(int_div_fn->AddKernel(make_binary_kernel(lhs_id, rhs_id, ResolveIntDivOutputType,
                                                                  ExecIntDivStrict)));
      ARROW_RETURN_NOT_OK(int_div_or_zero_fn->AddKernel(make_binary_kernel(lhs_id, rhs_id, ResolveIntDivOutputType,
                                                                          ExecIntDivOrZero)));
    }
  }
  ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(int_div_fn), /*allow_overwrite=*/true));
  ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(int_div_or_zero_fn), /*allow_overwrite=*/true));

  auto gcd_fn = std::make_shared<arrow::compute::ScalarFunction>(
      "gcd", arrow::compute::Arity::Binary(), arrow::compute::FunctionDoc::Empty());
  auto lcm_fn = std::make_shared<arrow::compute::ScalarFunction>(
      "lcm", arrow::compute::Arity::Binary(), arrow::compute::FunctionDoc::Empty());
  for (const auto lhs_id : numeric_ids) {
    for (const auto rhs_id : numeric_ids) {
      ARROW_RETURN_NOT_OK(gcd_fn->AddKernel(make_binary_kernel(lhs_id, rhs_id, ResolveGcdLcmOutputType, ExecGcd)));
      ARROW_RETURN_NOT_OK(lcm_fn->AddKernel(make_binary_kernel(lhs_id, rhs_id, ResolveGcdLcmOutputType, ExecLcm)));
    }
  }
  ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(gcd_fn), /*allow_overwrite=*/true));
  ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(lcm_fn), /*allow_overwrite=*/true));

  return arrow::Status::OK();
}

}  // namespace tiforth::function
