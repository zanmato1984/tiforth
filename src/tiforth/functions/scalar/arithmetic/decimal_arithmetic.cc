#include <algorithm>
#include <array>
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <variant>

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

namespace tiforth::functions {

namespace {

constexpr int32_t kDecimalMaxPrecision = 65;
constexpr int32_t kDecimalMaxScale = 30;
constexpr int32_t kDefaultDivPrecisionIncrement = 4;

struct DecimalSpec {
  int32_t precision = -1;
  int32_t scale = -1;
};

arrow::Result<DecimalSpec> GetDecimalSpec(const arrow::DataType& type) {
  if (type.id() != arrow::Type::DECIMAL128 && type.id() != arrow::Type::DECIMAL256) {
    return arrow::Status::Invalid("expected decimal type");
  }
  const auto& dec = static_cast<const arrow::DecimalType&>(type);
  if (dec.precision() <= 0) {
    return arrow::Status::Invalid("decimal precision must be positive");
  }
  if (dec.scale() < 0) {
    return arrow::Status::Invalid("decimal scale must not be negative");
  }
  if (dec.precision() > kDecimalMaxPrecision) {
    return arrow::Status::Invalid("decimal precision exceeds TiFlash max precision: ", dec.precision());
  }
  if (dec.scale() > kDecimalMaxScale) {
    return arrow::Status::Invalid("decimal scale exceeds TiFlash max scale: ", dec.scale());
  }
  return DecimalSpec{static_cast<int32_t>(dec.precision()), static_cast<int32_t>(dec.scale())};
}

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

arrow::Result<DecimalSpec> GetDecimalOrIntegerSpec(const arrow::DataType& type) {
  if (type.id() == arrow::Type::DECIMAL128 || type.id() == arrow::Type::DECIMAL256) {
    return GetDecimalSpec(type);
  }
  if (!IsIntegerType(type.id())) {
    return arrow::Status::Invalid("expected decimal or integer type");
  }
  ARROW_ASSIGN_OR_RAISE(const auto precision, arrow::MaxDecimalDigitsForInteger(type.id()));
  return DecimalSpec{precision, /*scale=*/0};
}

DecimalSpec InferDecimalAddSubResultSpec(const DecimalSpec& lhs, const DecimalSpec& rhs) {
  // Mirror TiFlash's PlusDecimalInferer (Common/Decimal.h).
  //
  // result_scale = max(s1, s2)
  // result_int = max(p1 - s1, p2 - s2)
  // result_prec = min(result_scale + result_int + 1, 65)
  const int32_t result_scale = std::max(lhs.scale, rhs.scale);
  const int32_t lhs_int = lhs.precision - lhs.scale;
  const int32_t rhs_int = rhs.precision - rhs.scale;
  const int32_t result_int = std::max(lhs_int, rhs_int);
  const int32_t result_prec = std::min<int32_t>(result_scale + result_int + 1, kDecimalMaxPrecision);
  return DecimalSpec{result_prec, result_scale};
}

DecimalSpec InferDecimalMultiplyResultSpec(const DecimalSpec& lhs, const DecimalSpec& rhs) {
  // Mirror TiFlash's MulDecimalInferer (Common/Decimal.h).
  return DecimalSpec{std::min<int32_t>(lhs.precision + rhs.precision, kDecimalMaxPrecision),
                     std::min<int32_t>(lhs.scale + rhs.scale, kDecimalMaxScale)};
}

DecimalSpec InferDecimalDivideResultSpec(const DecimalSpec& lhs, const DecimalSpec& rhs,
                                        int32_t div_precincrement) {
  // Mirror TiFlash's DivDecimalInferer (Common/Decimal.h).
  return DecimalSpec{
      std::min<int32_t>(lhs.precision + rhs.scale + div_precincrement, kDecimalMaxPrecision),
      std::min<int32_t>(lhs.scale + div_precincrement, kDecimalMaxScale),
  };
}

DecimalSpec InferDecimalModuloResultSpec(const DecimalSpec& lhs, const DecimalSpec& rhs) {
  // Mirror TiFlash's ModDecimalInferer (Common/Decimal.h).
  return DecimalSpec{std::max(lhs.precision, rhs.precision), std::max(lhs.scale, rhs.scale)};
}

arrow::Result<std::shared_ptr<arrow::DataType>> MakeArrowDecimalType(const DecimalSpec& spec) {
  if (spec.precision <= 0 || spec.scale < 0) {
    return arrow::Status::Invalid("invalid decimal precision/scale");
  }
  if (spec.precision > kDecimalMaxPrecision) {
    return arrow::Status::Invalid("decimal precision exceeds TiFlash max precision");
  }
  if (spec.scale > kDecimalMaxScale) {
    return arrow::Status::Invalid("decimal scale exceeds TiFlash max scale");
  }
  if (spec.scale > spec.precision) {
    return arrow::Status::Invalid("decimal scale must not exceed precision");
  }
  if (spec.precision <= arrow::Decimal128Type::kMaxPrecision) {
    return arrow::decimal128(spec.precision, spec.scale);
  }
  return arrow::decimal256(spec.precision, spec.scale);
}

arrow::Result<arrow::Decimal256> RescaleExact(const arrow::Decimal256& value, int32_t from_scale,
                                              int32_t to_scale) {
  if (from_scale == to_scale) {
    return value;
  }
  if (to_scale < from_scale) {
    return arrow::Status::Invalid("unexpected decimal scale down in TiFlash decimal op");
  }
  return value.Rescale(from_scale, to_scale);
}

arrow::Result<arrow::Decimal128> Decimal256ToDecimal128(const arrow::Decimal256& value) {
  const auto le = value.little_endian_array();  // low->high
  const uint64_t sign_ext = (le[3] & (uint64_t(1) << 63)) != 0 ? ~uint64_t{0} : uint64_t{0};
  if (le[2] != sign_ext || le[3] != sign_ext) {
    return arrow::Status::Invalid("decimal value does not fit in decimal128");
  }
  return arrow::Decimal128(arrow::Decimal128::LittleEndianArray, {le[0], le[1]});
}

template <typename DecimalT>
bool FitsInPrecision(const DecimalT& value, int32_t precision) {
  return value.FitsInPrecision(precision);
}

template <typename InDecimal, typename InScalar, int ExpectedByteWidth>
struct DecimalToDec256Reader {
  const arrow::FixedSizeBinaryArray* array = nullptr;
  const InScalar* scalar = nullptr;
  bool scalar_valid = false;
  InDecimal scalar_value{};
  int32_t scale = 0;
  arrow::Status Init(const std::shared_ptr<arrow::Array>& input_array, const arrow::Scalar* input_scalar,
                     int32_t scale_) {
    array = input_array != nullptr ? static_cast<const arrow::FixedSizeBinaryArray*>(input_array.get()) : nullptr;
    if (array != nullptr && array->byte_width() != ExpectedByteWidth) {
      return arrow::Status::Invalid("unexpected decimal byte width");
    }
    scalar = input_scalar != nullptr ? dynamic_cast<const InScalar*>(input_scalar) : nullptr;
    if (input_scalar != nullptr && scalar == nullptr) {
      return arrow::Status::Invalid("unexpected decimal scalar type");
    }
    scalar_valid = scalar != nullptr && scalar->is_valid;
    scalar_value = scalar_valid ? scalar->value : InDecimal{};
    scale = scale_;
    return arrow::Status::OK();
  }
  bool IsNull(int64_t i) const { return array != nullptr ? array->IsNull(i) : !scalar_valid; }
  arrow::Decimal256 Value(int64_t i) const {
    const InDecimal raw =
        array != nullptr ? InDecimal(reinterpret_cast<const uint8_t*>(array->GetValue(i))) : scalar_value;
    return arrow::Decimal256(raw);
  }
  int32_t Scale() const { return scale; }
};

template <typename InArray, typename InScalar, typename InValue>
struct IntegerToDec256Reader {
  const InArray* array = nullptr;
  const InScalar* scalar = nullptr;
  bool scalar_valid = false;
  InValue scalar_value{};
  arrow::Status Init(const std::shared_ptr<arrow::Array>& input_array, const arrow::Scalar* input_scalar) {
    array = input_array != nullptr ? static_cast<const InArray*>(input_array.get()) : nullptr;
    scalar = input_scalar != nullptr ? dynamic_cast<const InScalar*>(input_scalar) : nullptr;
    if (input_scalar != nullptr && scalar == nullptr) {
      return arrow::Status::Invalid("unexpected integer scalar type");
    }
    scalar_valid = scalar != nullptr && scalar->is_valid;
    scalar_value = scalar_valid ? scalar->value : InValue{};
    return arrow::Status::OK();
  }
  bool IsNull(int64_t i) const { return array != nullptr ? array->IsNull(i) : !scalar_valid; }
  arrow::Decimal256 Value(int64_t i) const {
    const InValue raw = array != nullptr ? array->Value(i) : scalar_value;
    return arrow::Decimal256(raw);
  }
  int32_t Scale() const { return 0; }
};

using Dec128Reader = DecimalToDec256Reader<arrow::Decimal128, arrow::Decimal128Scalar, 16>;
using Dec256Reader = DecimalToDec256Reader<arrow::Decimal256, arrow::Decimal256Scalar, 32>;

using Int8Reader = IntegerToDec256Reader<arrow::Int8Array, arrow::Int8Scalar, int8_t>;
using UInt8Reader = IntegerToDec256Reader<arrow::UInt8Array, arrow::UInt8Scalar, uint8_t>;
using Int16Reader = IntegerToDec256Reader<arrow::Int16Array, arrow::Int16Scalar, int16_t>;
using UInt16Reader = IntegerToDec256Reader<arrow::UInt16Array, arrow::UInt16Scalar, uint16_t>;
using Int32Reader = IntegerToDec256Reader<arrow::Int32Array, arrow::Int32Scalar, int32_t>;
using UInt32Reader = IntegerToDec256Reader<arrow::UInt32Array, arrow::UInt32Scalar, uint32_t>;
using Int64Reader = IntegerToDec256Reader<arrow::Int64Array, arrow::Int64Scalar, int64_t>;
using UInt64Reader = IntegerToDec256Reader<arrow::UInt64Array, arrow::UInt64Scalar, uint64_t>;

using Reader = std::variant<Dec128Reader, Dec256Reader, Int8Reader, UInt8Reader, Int16Reader, UInt16Reader,
                            Int32Reader, UInt32Reader, Int64Reader, UInt64Reader>;

arrow::Result<Reader> MakeDec256Reader(const arrow::DataType& type, const std::shared_ptr<arrow::Array>& input_array,
                                       const arrow::Scalar* input_scalar) {
  switch (type.id()) {
    case arrow::Type::DECIMAL128: {
      const auto& dec = static_cast<const arrow::DecimalType&>(type);
      Dec128Reader reader;
      ARROW_RETURN_NOT_OK(reader.Init(input_array, input_scalar, static_cast<int32_t>(dec.scale())));
      return Reader{std::move(reader)};
    }
    case arrow::Type::DECIMAL256: {
      const auto& dec = static_cast<const arrow::DecimalType&>(type);
      Dec256Reader reader;
      ARROW_RETURN_NOT_OK(reader.Init(input_array, input_scalar, static_cast<int32_t>(dec.scale())));
      return Reader{std::move(reader)};
    }
    case arrow::Type::INT8: {
      Int8Reader reader;
      ARROW_RETURN_NOT_OK(reader.Init(input_array, input_scalar));
      return Reader{std::move(reader)};
    }
    case arrow::Type::UINT8: {
      UInt8Reader reader;
      ARROW_RETURN_NOT_OK(reader.Init(input_array, input_scalar));
      return Reader{std::move(reader)};
    }
    case arrow::Type::INT16: {
      Int16Reader reader;
      ARROW_RETURN_NOT_OK(reader.Init(input_array, input_scalar));
      return Reader{std::move(reader)};
    }
    case arrow::Type::UINT16: {
      UInt16Reader reader;
      ARROW_RETURN_NOT_OK(reader.Init(input_array, input_scalar));
      return Reader{std::move(reader)};
    }
    case arrow::Type::INT32: {
      Int32Reader reader;
      ARROW_RETURN_NOT_OK(reader.Init(input_array, input_scalar));
      return Reader{std::move(reader)};
    }
    case arrow::Type::UINT32: {
      UInt32Reader reader;
      ARROW_RETURN_NOT_OK(reader.Init(input_array, input_scalar));
      return Reader{std::move(reader)};
    }
    case arrow::Type::INT64: {
      Int64Reader reader;
      ARROW_RETURN_NOT_OK(reader.Init(input_array, input_scalar));
      return Reader{std::move(reader)};
    }
    case arrow::Type::UINT64: {
      UInt64Reader reader;
      ARROW_RETURN_NOT_OK(reader.Init(input_array, input_scalar));
      return Reader{std::move(reader)};
    }
    default:
      break;
  }
  return arrow::Status::Invalid("unsupported decimal op input type: ", type.ToString());
}

using Words256 = std::array<uint64_t, 4>;
using Words512 = std::array<uint64_t, 8>;

Words512 MultiplyUnsigned256To512(const Words256& a, const Words256& b) {
  Words512 out{};
  out.fill(0);
  for (size_t i = 0; i < a.size(); ++i) {
    unsigned __int128 carry = 0;
    for (size_t j = 0; j < b.size(); ++j) {
      const size_t k = i + j;
      unsigned __int128 cur =
          static_cast<unsigned __int128>(a[i]) * static_cast<unsigned __int128>(b[j]) +
          static_cast<unsigned __int128>(out[k]) + carry;
      out[k] = static_cast<uint64_t>(cur);
      carry = cur >> 64;
    }
    // Propagate carry through remaining limbs.
    size_t k = i + b.size();
    while (carry != 0 && k < out.size()) {
      unsigned __int128 cur = static_cast<unsigned __int128>(out[k]) + carry;
      out[k] = static_cast<uint64_t>(cur);
      carry = cur >> 64;
      ++k;
    }
  }
  return out;
}

void DivideUnsigned512By10InPlace(Words512* value) {
  uint64_t rem = 0;
  for (int i = static_cast<int>(value->size()) - 1; i >= 0; --i) {
    const unsigned __int128 cur = (static_cast<unsigned __int128>(rem) << 64) |
                                  static_cast<unsigned __int128>((*value)[static_cast<size_t>(i)]);
    (*value)[static_cast<size_t>(i)] = static_cast<uint64_t>(cur / 10);
    rem = static_cast<uint64_t>(cur % 10);
  }
}

arrow::Result<arrow::Decimal256> DecimalMulTrunc(const arrow::Decimal256& lhs, int32_t lhs_scale,
                                                 const arrow::Decimal256& rhs, int32_t rhs_scale,
                                                 int32_t out_scale) {
  if (lhs_scale < 0 || rhs_scale < 0 || out_scale < 0) {
    return arrow::Status::Invalid("decimal scale must not be negative");
  }
  const bool negate = lhs.Sign() != rhs.Sign();
  const arrow::Decimal256 lhs_abs = arrow::Decimal256(arrow::Decimal256::Abs(lhs));
  const arrow::Decimal256 rhs_abs = arrow::Decimal256(arrow::Decimal256::Abs(rhs));
  const auto lhs_words = lhs_abs.little_endian_array();
  const auto rhs_words = rhs_abs.little_endian_array();
  Words256 a{lhs_words[0], lhs_words[1], lhs_words[2], lhs_words[3]};
  Words256 b{rhs_words[0], rhs_words[1], rhs_words[2], rhs_words[3]};
  auto product = MultiplyUnsigned256To512(a, b);
  const int32_t reduce_by = lhs_scale + rhs_scale - out_scale;
  if (reduce_by < 0) {
    return arrow::Status::Invalid("unexpected decimal multiply scale expansion");
  }
  if (reduce_by > 0) {
    // TiFlash truncates toward zero for scale reduction.
    for (int32_t i = 0; i < reduce_by; ++i) {
      DivideUnsigned512By10InPlace(&product);
    }
  }
  // If the quotient doesn't fit 256 bits, it's definitely not representable as Decimal(<=65,*).
  for (size_t i = 4; i < product.size(); ++i) {
    if (product[i] != 0) {
      return arrow::Status::Invalid("decimal math overflow");
    }
  }
  arrow::Decimal256 out(arrow::Decimal256::LittleEndianArray,
                        {product[0], product[1], product[2], product[3]});
  if (negate) {
    out.Negate();
  }
  return out;
}

arrow::Result<std::pair<arrow::Decimal256, arrow::Decimal256>> DecimalDivMod(const arrow::Decimal256& lhs,
                                                                             const arrow::Decimal256& rhs) {
  return lhs.Divide(rhs);
}

arrow::Result<arrow::Decimal256> DecimalDivTrunc(const arrow::Decimal256& lhs, int32_t lhs_scale,
                                                 const arrow::Decimal256& rhs, int32_t rhs_scale,
                                                 int32_t out_scale, int32_t out_precision) {
  if (rhs == 0) {
    return arrow::Status::Invalid("division by zero");
  }
  const bool negate = lhs.Sign() != rhs.Sign();
  const arrow::Decimal256 lhs_abs = arrow::Decimal256(arrow::Decimal256::Abs(lhs));
  const arrow::Decimal256 rhs_abs = arrow::Decimal256(arrow::Decimal256::Abs(rhs));
  const int32_t scale_factor = rhs_scale - lhs_scale + out_scale;
  if (scale_factor < 0) {
    return arrow::Status::Invalid("unexpected negative decimal division scale factor");
  }
  ARROW_ASSIGN_OR_RAISE(auto div0, DecimalDivMod(lhs_abs, rhs_abs));
  auto q = div0.first;
  auto r = div0.second;
  const arrow::Decimal256 ten(10);
  for (int32_t i = 0; i < scale_factor; ++i) {
    r *= ten;
    ARROW_ASSIGN_OR_RAISE(auto step, DecimalDivMod(r, rhs_abs));
    auto digit = step.first;
    r = step.second;
    ARROW_DCHECK(digit >= 0);
    ARROW_DCHECK(digit < ten);
    q *= ten;
    q += digit;
    if (!FitsInPrecision(q, out_precision)) {
      return arrow::Status::Invalid("decimal math overflow");
    }
  }
  if (negate) {
    q.Negate();
  }
  if (!FitsInPrecision(q, out_precision)) {
    return arrow::Status::Invalid("decimal math overflow");
  }
  return q;
}

arrow::Result<std::optional<arrow::Decimal256>> DecimalTiDBDivRound(const arrow::Decimal256& lhs, int32_t lhs_scale,
                                                                    const arrow::Decimal256& rhs, int32_t rhs_scale,
                                                                    int32_t out_scale, int32_t out_precision) {
  if (rhs == 0) {
    return std::nullopt;
  }
  const bool negate = lhs.Sign() != rhs.Sign();
  const arrow::Decimal256 lhs_abs = arrow::Decimal256(arrow::Decimal256::Abs(lhs));
  const arrow::Decimal256 rhs_abs = arrow::Decimal256(arrow::Decimal256::Abs(rhs));
  const int32_t scale_factor = rhs_scale - lhs_scale + out_scale;
  if (scale_factor < 0) {
    return arrow::Status::Invalid("unexpected negative decimal division scale factor");
  }
  ARROW_ASSIGN_OR_RAISE(auto div0, DecimalDivMod(lhs_abs, rhs_abs));
  auto q = div0.first;
  auto r = div0.second;
  const arrow::Decimal256 ten(10);
  for (int32_t i = 0; i < scale_factor; ++i) {
    r *= ten;
    ARROW_ASSIGN_OR_RAISE(auto step, DecimalDivMod(r, rhs_abs));
    auto digit = step.first;
    r = step.second;
    ARROW_DCHECK(digit >= 0);
    ARROW_DCHECK(digit < ten);
    q *= ten;
    q += digit;
    if (!FitsInPrecision(q, out_precision)) {
      return arrow::Status::Invalid("decimal math overflow");
    }
  }
  // Round half-up (TiDB semantics): if remainder >= ceil(divisor/2), increment abs(quotient).
  ARROW_ASSIGN_OR_RAISE(auto half_div, DecimalDivMod(rhs_abs, arrow::Decimal256(2)));
  auto half = half_div.first;
  if (half_div.second != 0) {
    half += arrow::Decimal256(1);
  }
  if (r >= half) {
    q += arrow::Decimal256(1);
  }
  if (negate) {
    q.Negate();
  }
  if (!FitsInPrecision(q, out_precision)) {
    return arrow::Status::Invalid("decimal math overflow");
  }
  return q;
}

arrow::Result<arrow::Decimal256> DecimalModuloScaled(const arrow::Decimal256& lhs, int32_t lhs_scale,
                                                     const arrow::Decimal256& rhs, int32_t rhs_scale,
                                                     int32_t out_scale) {
  if (rhs == 0) {
    return arrow::Status::Invalid("division by zero");
  }
  const arrow::Decimal256 divisor_abs = arrow::Decimal256(arrow::Decimal256::Abs(rhs));
  if (divisor_abs == 0) {
    return arrow::Status::Invalid("division by zero");
  }
  if (lhs_scale == rhs_scale) {
    ARROW_ASSIGN_OR_RAISE(auto div, DecimalDivMod(lhs, divisor_abs));
    return div.second;
  }
  if (lhs_scale < rhs_scale) {
    const int32_t delta = rhs_scale - lhs_scale;
    ARROW_ASSIGN_OR_RAISE(auto div0, DecimalDivMod(lhs, divisor_abs));
    auto rem = div0.second;
    const arrow::Decimal256 ten(10);
    for (int32_t i = 0; i < delta; ++i) {
      rem *= ten;
      ARROW_ASSIGN_OR_RAISE(auto div, DecimalDivMod(rem, divisor_abs));
      rem = div.second;
    }
    return rem;
  }
  // rhs_scale < lhs_scale: scale up the (absolute) divisor when it fits, otherwise divisor > dividend -> remainder is
  // the dividend (lhs).
  const int32_t delta = lhs_scale - rhs_scale;
  auto scaled_divisor_res = divisor_abs.Rescale(/*original_scale=*/0, /*new_scale=*/delta);
  if (!scaled_divisor_res.ok()) {
    return lhs;
  }
  const auto scaled_divisor = *scaled_divisor_res;
  ARROW_ASSIGN_OR_RAISE(auto div, DecimalDivMod(lhs, scaled_divisor));
  return div.second;
}

template <typename BuilderT, typename LhsReaderT, typename RhsReaderT, typename ComputeFn>
arrow::Status ExecDecimalBinaryLoop(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                                   arrow::compute::ExecResult* out, const DecimalSpec& out_spec,
                                   const std::shared_ptr<arrow::DataType>& out_type, const LhsReaderT& lhs,
                                   const RhsReaderT& rhs, ComputeFn&& compute) {
  if (ctx == nullptr || out == nullptr) {
    return arrow::Status::Invalid("kernel context/result must not be null");
  }
  const int64_t rows = batch.length;
  BuilderT builder(out_type, ctx->memory_pool());
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
    const auto value = *maybe_value;
    if (!FitsInPrecision(value, out_spec.precision)) {
      return arrow::Status::Invalid("decimal math overflow");
    }
    if constexpr (std::is_same_v<BuilderT, arrow::Decimal128Builder>) {
      ARROW_ASSIGN_OR_RAISE(const auto narrow, Decimal256ToDecimal128(value));
      builder.UnsafeAppend(narrow);
    } else {
      builder.UnsafeAppend(value);
    }
  }
  std::shared_ptr<arrow::Array> out_array;
  ARROW_RETURN_NOT_OK(builder.Finish(&out_array));
  out->value = out_array->data();
  return arrow::Status::OK();
}

arrow::Result<arrow::TypeHolder> ResolveDecimalAddSubOutputType(
    arrow::compute::KernelContext*, const std::vector<arrow::TypeHolder>& types) {
  if (types.size() != 2) {
    return arrow::Status::Invalid("decimal add/sub requires 2 args");
  }
  if (types[0].type == nullptr || types[1].type == nullptr) {
    return arrow::Status::Invalid("decimal add/sub input type must not be null");
  }
  const auto lhs_type_id = types[0].type->id();
  const auto rhs_type_id = types[1].type->id();
  if (lhs_type_id != arrow::Type::DECIMAL128 && lhs_type_id != arrow::Type::DECIMAL256 &&
      rhs_type_id != arrow::Type::DECIMAL128 && rhs_type_id != arrow::Type::DECIMAL256) {
    return arrow::Status::Invalid("decimal add/sub requires at least one decimal argument");
  }
  ARROW_ASSIGN_OR_RAISE(const auto lhs_spec, GetDecimalOrIntegerSpec(*types[0].type));
  ARROW_ASSIGN_OR_RAISE(const auto rhs_spec, GetDecimalOrIntegerSpec(*types[1].type));
  const auto out_spec = InferDecimalAddSubResultSpec(lhs_spec, rhs_spec);
  ARROW_ASSIGN_OR_RAISE(auto out_type, MakeArrowDecimalType(out_spec));
  return arrow::TypeHolder(std::move(out_type));
}

arrow::Result<arrow::TypeHolder> ResolveDecimalMulOutputType(arrow::compute::KernelContext*,
                                                             const std::vector<arrow::TypeHolder>& types) {
  if (types.size() != 2) {
    return arrow::Status::Invalid("decimal multiply requires 2 args");
  }
  if (types[0].type == nullptr || types[1].type == nullptr) {
    return arrow::Status::Invalid("decimal multiply input type must not be null");
  }
  const auto lhs_type_id = types[0].type->id();
  const auto rhs_type_id = types[1].type->id();
  if (lhs_type_id != arrow::Type::DECIMAL128 && lhs_type_id != arrow::Type::DECIMAL256 &&
      rhs_type_id != arrow::Type::DECIMAL128 && rhs_type_id != arrow::Type::DECIMAL256) {
    return arrow::Status::Invalid("decimal multiply requires at least one decimal argument");
  }
  ARROW_ASSIGN_OR_RAISE(const auto lhs_spec, GetDecimalOrIntegerSpec(*types[0].type));
  ARROW_ASSIGN_OR_RAISE(const auto rhs_spec, GetDecimalOrIntegerSpec(*types[1].type));
  const auto out_spec = InferDecimalMultiplyResultSpec(lhs_spec, rhs_spec);
  ARROW_ASSIGN_OR_RAISE(auto out_type, MakeArrowDecimalType(out_spec));
  return arrow::TypeHolder(std::move(out_type));
}

arrow::Result<arrow::TypeHolder> ResolveDecimalDivOutputType(arrow::compute::KernelContext*,
                                                             const std::vector<arrow::TypeHolder>& types) {
  if (types.size() != 2) {
    return arrow::Status::Invalid("decimal divide requires 2 args");
  }
  if (types[0].type == nullptr || types[1].type == nullptr) {
    return arrow::Status::Invalid("decimal divide input type must not be null");
  }
  const auto lhs_type_id = types[0].type->id();
  const auto rhs_type_id = types[1].type->id();
  if (lhs_type_id != arrow::Type::DECIMAL128 && lhs_type_id != arrow::Type::DECIMAL256 &&
      rhs_type_id != arrow::Type::DECIMAL128 && rhs_type_id != arrow::Type::DECIMAL256) {
    return arrow::Status::Invalid("decimal divide requires at least one decimal argument");
  }
  ARROW_ASSIGN_OR_RAISE(const auto lhs_spec, GetDecimalOrIntegerSpec(*types[0].type));
  ARROW_ASSIGN_OR_RAISE(const auto rhs_spec, GetDecimalOrIntegerSpec(*types[1].type));
  const auto out_spec = InferDecimalDivideResultSpec(lhs_spec, rhs_spec, kDefaultDivPrecisionIncrement);
  ARROW_ASSIGN_OR_RAISE(auto out_type, MakeArrowDecimalType(out_spec));
  return arrow::TypeHolder(std::move(out_type));
}

arrow::Result<arrow::TypeHolder> ResolveDecimalModuloOutputType(arrow::compute::KernelContext*,
                                                                const std::vector<arrow::TypeHolder>& types) {
  if (types.size() != 2) {
    return arrow::Status::Invalid("decimal modulo requires 2 args");
  }
  if (types[0].type == nullptr || types[1].type == nullptr) {
    return arrow::Status::Invalid("decimal modulo input type must not be null");
  }
  const auto lhs_type_id = types[0].type->id();
  const auto rhs_type_id = types[1].type->id();
  if (lhs_type_id != arrow::Type::DECIMAL128 && lhs_type_id != arrow::Type::DECIMAL256 &&
      rhs_type_id != arrow::Type::DECIMAL128 && rhs_type_id != arrow::Type::DECIMAL256) {
    return arrow::Status::Invalid("decimal modulo requires at least one decimal argument");
  }
  ARROW_ASSIGN_OR_RAISE(const auto lhs_spec, GetDecimalOrIntegerSpec(*types[0].type));
  ARROW_ASSIGN_OR_RAISE(const auto rhs_spec, GetDecimalOrIntegerSpec(*types[1].type));
  const auto out_spec = InferDecimalModuloResultSpec(lhs_spec, rhs_spec);
  ARROW_ASSIGN_OR_RAISE(auto out_type, MakeArrowDecimalType(out_spec));
  return arrow::TypeHolder(std::move(out_type));
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

arrow::Status ExecDecimalAddSubImpl(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                                   arrow::compute::ExecResult* out, bool is_add) {
  if (ctx == nullptr || out == nullptr) {
    return arrow::Status::Invalid("kernel context/result must not be null");
  }
  if (batch.num_values() != 2) {
    return arrow::Status::Invalid("decimal add/sub requires 2 args");
  }
  const auto* lhs_type = batch[0].type();
  const auto* rhs_type = batch[1].type();
  if (lhs_type == nullptr || rhs_type == nullptr) {
    return arrow::Status::Invalid("decimal add/sub input type must not be null");
  }
  ARROW_ASSIGN_OR_RAISE(const auto lhs_spec, GetDecimalOrIntegerSpec(*lhs_type));
  ARROW_ASSIGN_OR_RAISE(const auto rhs_spec, GetDecimalOrIntegerSpec(*rhs_type));
  const auto out_spec = InferDecimalAddSubResultSpec(lhs_spec, rhs_spec);
  ARROW_ASSIGN_OR_RAISE(auto out_type, MakeArrowDecimalType(out_spec));
  const auto in = GetExecInputs(batch);
  ARROW_ASSIGN_OR_RAISE(auto lhs_reader, MakeDec256Reader(*lhs_type, in.lhs_array, in.lhs_scalar));
  ARROW_ASSIGN_OR_RAISE(auto rhs_reader, MakeDec256Reader(*rhs_type, in.rhs_array, in.rhs_scalar));
  if (out_type->id() == arrow::Type::DECIMAL128) {
    return std::visit(
        [&](const auto& lhs, const auto& rhs) -> arrow::Status {
          const auto compute = [&](int64_t i) -> arrow::Result<std::optional<arrow::Decimal256>> {
            ARROW_ASSIGN_OR_RAISE(auto lhs_scaled, RescaleExact(lhs.Value(i), lhs.Scale(), out_spec.scale));
            ARROW_ASSIGN_OR_RAISE(auto rhs_scaled, RescaleExact(rhs.Value(i), rhs.Scale(), out_spec.scale));
            auto res = lhs_scaled + rhs_scaled;
            if (!is_add) {
              // Arrow's BasicDecimal256 doesn't define binary operator-.
              res = lhs_scaled + (-rhs_scaled);
            }
            return std::optional<arrow::Decimal256>(res);
          };
          return ExecDecimalBinaryLoop<arrow::Decimal128Builder>(ctx, batch, out, out_spec, out_type, lhs, rhs,
                                                                compute);
        },
        lhs_reader, rhs_reader);
  }
  return std::visit(
      [&](const auto& lhs, const auto& rhs) -> arrow::Status {
        const auto compute = [&](int64_t i) -> arrow::Result<std::optional<arrow::Decimal256>> {
          ARROW_ASSIGN_OR_RAISE(auto lhs_scaled, RescaleExact(lhs.Value(i), lhs.Scale(), out_spec.scale));
          ARROW_ASSIGN_OR_RAISE(auto rhs_scaled, RescaleExact(rhs.Value(i), rhs.Scale(), out_spec.scale));
          auto res = lhs_scaled + rhs_scaled;
          if (!is_add) {
            // Arrow's BasicDecimal256 doesn't define binary operator-.
            res = lhs_scaled + (-rhs_scaled);
          }
          return std::optional<arrow::Decimal256>(res);
        };
        return ExecDecimalBinaryLoop<arrow::Decimal256Builder>(ctx, batch, out, out_spec, out_type, lhs, rhs, compute);
      },
      lhs_reader, rhs_reader);
}

arrow::Status ExecDecimalAdd(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                            arrow::compute::ExecResult* out) {
  return ExecDecimalAddSubImpl(ctx, batch, out, /*is_add=*/true);
}

arrow::Status ExecDecimalSubtract(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                                 arrow::compute::ExecResult* out) {
  return ExecDecimalAddSubImpl(ctx, batch, out, /*is_add=*/false);
}

arrow::Status ExecDecimalMultiply(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                                 arrow::compute::ExecResult* out) {
  if (ctx == nullptr || out == nullptr) {
    return arrow::Status::Invalid("kernel context/result must not be null");
  }
  if (batch.num_values() != 2) {
    return arrow::Status::Invalid("decimal multiply requires 2 args");
  }
  const auto* lhs_type = batch[0].type();
  const auto* rhs_type = batch[1].type();
  if (lhs_type == nullptr || rhs_type == nullptr) {
    return arrow::Status::Invalid("decimal multiply input type must not be null");
  }
  ARROW_ASSIGN_OR_RAISE(const auto lhs_spec, GetDecimalOrIntegerSpec(*lhs_type));
  ARROW_ASSIGN_OR_RAISE(const auto rhs_spec, GetDecimalOrIntegerSpec(*rhs_type));
  const auto out_spec = InferDecimalMultiplyResultSpec(lhs_spec, rhs_spec);
  ARROW_ASSIGN_OR_RAISE(auto out_type, MakeArrowDecimalType(out_spec));
  const auto in = GetExecInputs(batch);
  ARROW_ASSIGN_OR_RAISE(auto lhs_reader, MakeDec256Reader(*lhs_type, in.lhs_array, in.lhs_scalar));
  ARROW_ASSIGN_OR_RAISE(auto rhs_reader, MakeDec256Reader(*rhs_type, in.rhs_array, in.rhs_scalar));
  const auto compute = [&](const auto& lhs, const auto& rhs, int64_t i)
      -> arrow::Result<std::optional<arrow::Decimal256>> {
    ARROW_ASSIGN_OR_RAISE(const auto res, DecimalMulTrunc(lhs.Value(i), lhs.Scale(), rhs.Value(i), rhs.Scale(),
                                                         out_spec.scale));
    return std::optional<arrow::Decimal256>(res);
  };
  if (out_type->id() == arrow::Type::DECIMAL128) {
    return std::visit(
        [&](const auto& lhs, const auto& rhs) -> arrow::Status {
          const auto compute_row = [&](int64_t i) { return compute(lhs, rhs, i); };
          return ExecDecimalBinaryLoop<arrow::Decimal128Builder>(ctx, batch, out, out_spec, out_type, lhs, rhs,
                                                                compute_row);
        },
        lhs_reader, rhs_reader);
  }
  return std::visit(
      [&](const auto& lhs, const auto& rhs) -> arrow::Status {
        const auto compute_row = [&](int64_t i) { return compute(lhs, rhs, i); };
        return ExecDecimalBinaryLoop<arrow::Decimal256Builder>(ctx, batch, out, out_spec, out_type, lhs, rhs,
                                                              compute_row);
      },
      lhs_reader, rhs_reader);
}

arrow::Status ExecDecimalDivide(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                               arrow::compute::ExecResult* out) {
  if (ctx == nullptr || out == nullptr) {
    return arrow::Status::Invalid("kernel context/result must not be null");
  }
  if (batch.num_values() != 2) {
    return arrow::Status::Invalid("decimal divide requires 2 args");
  }
  const auto* lhs_type = batch[0].type();
  const auto* rhs_type = batch[1].type();
  if (lhs_type == nullptr || rhs_type == nullptr) {
    return arrow::Status::Invalid("decimal divide input type must not be null");
  }
  ARROW_ASSIGN_OR_RAISE(const auto lhs_spec, GetDecimalOrIntegerSpec(*lhs_type));
  ARROW_ASSIGN_OR_RAISE(const auto rhs_spec, GetDecimalOrIntegerSpec(*rhs_type));
  const auto out_spec = InferDecimalDivideResultSpec(lhs_spec, rhs_spec, kDefaultDivPrecisionIncrement);
  ARROW_ASSIGN_OR_RAISE(auto out_type, MakeArrowDecimalType(out_spec));
  const auto in = GetExecInputs(batch);
  ARROW_ASSIGN_OR_RAISE(auto lhs_reader, MakeDec256Reader(*lhs_type, in.lhs_array, in.lhs_scalar));
  ARROW_ASSIGN_OR_RAISE(auto rhs_reader, MakeDec256Reader(*rhs_type, in.rhs_array, in.rhs_scalar));
  const auto compute = [&](const auto& lhs, const auto& rhs, int64_t i)
      -> arrow::Result<std::optional<arrow::Decimal256>> {
    ARROW_ASSIGN_OR_RAISE(const auto res, DecimalDivTrunc(lhs.Value(i), lhs.Scale(), rhs.Value(i), rhs.Scale(),
                                                         out_spec.scale, out_spec.precision));
    return std::optional<arrow::Decimal256>(res);
  };
  if (out_type->id() == arrow::Type::DECIMAL128) {
    return std::visit(
        [&](const auto& lhs, const auto& rhs) -> arrow::Status {
          const auto compute_row = [&](int64_t i) { return compute(lhs, rhs, i); };
          return ExecDecimalBinaryLoop<arrow::Decimal128Builder>(ctx, batch, out, out_spec, out_type, lhs, rhs,
                                                                compute_row);
        },
        lhs_reader, rhs_reader);
  }
  return std::visit(
      [&](const auto& lhs, const auto& rhs) -> arrow::Status {
        const auto compute_row = [&](int64_t i) { return compute(lhs, rhs, i); };
        return ExecDecimalBinaryLoop<arrow::Decimal256Builder>(ctx, batch, out, out_spec, out_type, lhs, rhs,
                                                              compute_row);
      },
      lhs_reader, rhs_reader);
}

arrow::Status ExecDecimalTiDBDivide(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                                   arrow::compute::ExecResult* out) {
  if (ctx == nullptr || out == nullptr) {
    return arrow::Status::Invalid("kernel context/result must not be null");
  }
  if (batch.num_values() != 2) {
    return arrow::Status::Invalid("decimal tidbDivide requires 2 args");
  }
  const auto* lhs_type = batch[0].type();
  const auto* rhs_type = batch[1].type();
  if (lhs_type == nullptr || rhs_type == nullptr) {
    return arrow::Status::Invalid("decimal tidbDivide input type must not be null");
  }
  ARROW_ASSIGN_OR_RAISE(const auto lhs_spec, GetDecimalOrIntegerSpec(*lhs_type));
  ARROW_ASSIGN_OR_RAISE(const auto rhs_spec, GetDecimalOrIntegerSpec(*rhs_type));
  const auto out_spec = InferDecimalDivideResultSpec(lhs_spec, rhs_spec, kDefaultDivPrecisionIncrement);
  ARROW_ASSIGN_OR_RAISE(auto out_type, MakeArrowDecimalType(out_spec));
  const auto in = GetExecInputs(batch);
  ARROW_ASSIGN_OR_RAISE(auto lhs_reader, MakeDec256Reader(*lhs_type, in.lhs_array, in.lhs_scalar));
  ARROW_ASSIGN_OR_RAISE(auto rhs_reader, MakeDec256Reader(*rhs_type, in.rhs_array, in.rhs_scalar));
  if (out_type->id() == arrow::Type::DECIMAL128) {
    return std::visit(
        [&](const auto& lhs, const auto& rhs) -> arrow::Status {
          const auto compute = [&](int64_t i) -> arrow::Result<std::optional<arrow::Decimal256>> {
            return DecimalTiDBDivRound(lhs.Value(i), lhs.Scale(), rhs.Value(i), rhs.Scale(), out_spec.scale,
                                      out_spec.precision);
          };
          return ExecDecimalBinaryLoop<arrow::Decimal128Builder>(ctx, batch, out, out_spec, out_type, lhs, rhs,
                                                                compute);
        },
        lhs_reader, rhs_reader);
  }
  return std::visit(
      [&](const auto& lhs, const auto& rhs) -> arrow::Status {
        const auto compute = [&](int64_t i) -> arrow::Result<std::optional<arrow::Decimal256>> {
          return DecimalTiDBDivRound(lhs.Value(i), lhs.Scale(), rhs.Value(i), rhs.Scale(), out_spec.scale,
                                    out_spec.precision);
        };
        return ExecDecimalBinaryLoop<arrow::Decimal256Builder>(ctx, batch, out, out_spec, out_type, lhs, rhs, compute);
      },
      lhs_reader, rhs_reader);
}

arrow::Status ExecDecimalModulo(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                               arrow::compute::ExecResult* out) {
  if (ctx == nullptr || out == nullptr) {
    return arrow::Status::Invalid("kernel context/result must not be null");
  }
  if (batch.num_values() != 2) {
    return arrow::Status::Invalid("decimal modulo requires 2 args");
  }
  const auto* lhs_type = batch[0].type();
  const auto* rhs_type = batch[1].type();
  if (lhs_type == nullptr || rhs_type == nullptr) {
    return arrow::Status::Invalid("decimal modulo input type must not be null");
  }
  ARROW_ASSIGN_OR_RAISE(const auto lhs_spec, GetDecimalOrIntegerSpec(*lhs_type));
  ARROW_ASSIGN_OR_RAISE(const auto rhs_spec, GetDecimalOrIntegerSpec(*rhs_type));
  const auto out_spec = InferDecimalModuloResultSpec(lhs_spec, rhs_spec);
  ARROW_ASSIGN_OR_RAISE(auto out_type, MakeArrowDecimalType(out_spec));
  const auto in = GetExecInputs(batch);
  ARROW_ASSIGN_OR_RAISE(auto lhs_reader, MakeDec256Reader(*lhs_type, in.lhs_array, in.lhs_scalar));
  ARROW_ASSIGN_OR_RAISE(auto rhs_reader, MakeDec256Reader(*rhs_type, in.rhs_array, in.rhs_scalar));
  if (out_type->id() == arrow::Type::DECIMAL128) {
    return std::visit(
        [&](const auto& lhs, const auto& rhs) -> arrow::Status {
          const auto compute = [&](int64_t i) -> arrow::Result<std::optional<arrow::Decimal256>> {
            if (rhs.Value(i) == 0) {
              return std::nullopt;
            }
            ARROW_ASSIGN_OR_RAISE(const auto res,
                                  DecimalModuloScaled(lhs.Value(i), lhs.Scale(), rhs.Value(i), rhs.Scale(),
                                                     out_spec.scale));
            return std::optional<arrow::Decimal256>(res);
          };
          return ExecDecimalBinaryLoop<arrow::Decimal128Builder>(ctx, batch, out, out_spec, out_type, lhs, rhs,
                                                                compute);
        },
        lhs_reader, rhs_reader);
  }
  return std::visit(
      [&](const auto& lhs, const auto& rhs) -> arrow::Status {
        const auto compute = [&](int64_t i) -> arrow::Result<std::optional<arrow::Decimal256>> {
          if (rhs.Value(i) == 0) {
            return std::nullopt;
          }
          ARROW_ASSIGN_OR_RAISE(const auto res, DecimalModuloScaled(lhs.Value(i), lhs.Scale(), rhs.Value(i), rhs.Scale(),
                                                                   out_spec.scale));
          return std::optional<arrow::Decimal256>(res);
        };
        return ExecDecimalBinaryLoop<arrow::Decimal256Builder>(ctx, batch, out, out_spec, out_type, lhs, rhs, compute);
      },
      lhs_reader, rhs_reader);
}

}  // namespace

arrow::Status RegisterDecimalArithmeticFunctions(arrow::compute::FunctionRegistry* registry,
                                                arrow::compute::FunctionRegistry* fallback_registry) {
  if (registry == nullptr) {
    return arrow::Status::Invalid("function registry must not be null");
  }
  if (fallback_registry == nullptr) {
    return arrow::Status::Invalid("fallback function registry must not be null");
  }
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
  const auto add_decimal_kernels = [&](const std::shared_ptr<arrow::compute::ScalarFunction>& fn,
                                       arrow::compute::OutputType::Resolver out_resolver,
                                       arrow::compute::ArrayKernelExec exec) -> arrow::Status {
    ARROW_RETURN_NOT_OK(fn->AddKernel(make_binary_kernel(arrow::Type::DECIMAL128, arrow::Type::DECIMAL128,
                                                        out_resolver, exec)));
    ARROW_RETURN_NOT_OK(fn->AddKernel(make_binary_kernel(arrow::Type::DECIMAL128, arrow::Type::DECIMAL256,
                                                        out_resolver, exec)));
    ARROW_RETURN_NOT_OK(fn->AddKernel(make_binary_kernel(arrow::Type::DECIMAL256, arrow::Type::DECIMAL128,
                                                        out_resolver, exec)));
    ARROW_RETURN_NOT_OK(fn->AddKernel(make_binary_kernel(arrow::Type::DECIMAL256, arrow::Type::DECIMAL256,
                                                        out_resolver, exec)));
    for (auto int_id : {arrow::Type::INT8, arrow::Type::UINT8, arrow::Type::INT16, arrow::Type::UINT16,
                        arrow::Type::INT32, arrow::Type::UINT32, arrow::Type::INT64, arrow::Type::UINT64}) {
      ARROW_RETURN_NOT_OK(fn->AddKernel(make_binary_kernel(arrow::Type::DECIMAL128, int_id, out_resolver, exec)));
      ARROW_RETURN_NOT_OK(fn->AddKernel(make_binary_kernel(int_id, arrow::Type::DECIMAL128, out_resolver, exec)));
      ARROW_RETURN_NOT_OK(fn->AddKernel(make_binary_kernel(arrow::Type::DECIMAL256, int_id, out_resolver, exec)));
      ARROW_RETURN_NOT_OK(fn->AddKernel(make_binary_kernel(int_id, arrow::Type::DECIMAL256, out_resolver, exec)));
    }
    return arrow::Status::OK();
  };
  auto decimal_add = std::make_shared<arrow::compute::ScalarFunction>(
      "tiforth.decimal_add", arrow::compute::Arity::Binary(), arrow::compute::FunctionDoc::Empty());
  ARROW_RETURN_NOT_OK(add_decimal_kernels(decimal_add, ResolveDecimalAddSubOutputType, ExecDecimalAdd));
  ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(decimal_add), /*allow_overwrite=*/true));
  auto decimal_subtract = std::make_shared<arrow::compute::ScalarFunction>(
      "tiforth.decimal_subtract", arrow::compute::Arity::Binary(), arrow::compute::FunctionDoc::Empty());
  ARROW_RETURN_NOT_OK(add_decimal_kernels(decimal_subtract, ResolveDecimalAddSubOutputType, ExecDecimalSubtract));
  ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(decimal_subtract), /*allow_overwrite=*/true));
  auto decimal_multiply = std::make_shared<arrow::compute::ScalarFunction>(
      "tiforth.decimal_multiply", arrow::compute::Arity::Binary(), arrow::compute::FunctionDoc::Empty());
  ARROW_RETURN_NOT_OK(add_decimal_kernels(decimal_multiply, ResolveDecimalMulOutputType, ExecDecimalMultiply));
  ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(decimal_multiply), /*allow_overwrite=*/true));
  auto decimal_divide = std::make_shared<arrow::compute::ScalarFunction>(
      "tiforth.decimal_divide", arrow::compute::Arity::Binary(), arrow::compute::FunctionDoc::Empty());
  ARROW_RETURN_NOT_OK(add_decimal_kernels(decimal_divide, ResolveDecimalDivOutputType, ExecDecimalDivide));
  ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(decimal_divide), /*allow_overwrite=*/true));
  auto decimal_tidb_divide = std::make_shared<arrow::compute::ScalarFunction>(
      "tiforth.decimal_tidb_divide", arrow::compute::Arity::Binary(), arrow::compute::FunctionDoc::Empty());
  ARROW_RETURN_NOT_OK(add_decimal_kernels(decimal_tidb_divide, ResolveDecimalDivOutputType, ExecDecimalTiDBDivide));
  ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(decimal_tidb_divide), /*allow_overwrite=*/true));
  auto decimal_modulo = std::make_shared<arrow::compute::ScalarFunction>(
      "tiforth.decimal_modulo", arrow::compute::Arity::Binary(), arrow::compute::FunctionDoc::Empty());
  ARROW_RETURN_NOT_OK(add_decimal_kernels(decimal_modulo, ResolveDecimalModuloOutputType, ExecDecimalModulo));
  ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(decimal_modulo), /*allow_overwrite=*/true));
  return arrow::Status::OK();
}

}  // namespace tiforth::functions
