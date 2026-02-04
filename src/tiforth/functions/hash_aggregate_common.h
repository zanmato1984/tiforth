#pragma once

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <arrow/compute/api_aggregate.h>
#include <arrow/compute/function.h>
#include <arrow/compute/kernel.h>
#include <arrow/compute/registry.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>

namespace tiforth::function::detail {

constexpr int32_t kTiFlashDecimalMaxPrec = 65;
constexpr int32_t kTiFlashDecimalMaxScale = 30;
constexpr int32_t kTiFlashDecimalLongLongDigits = 22;
constexpr int32_t kTiFlashDefaultDivPrecisionIncrement = 4;

inline arrow::Result<std::shared_ptr<arrow::DataType>> MakeDecimalOutType(int32_t precision,
                                                                         int32_t scale) {
  if (precision <= 0 || precision > kTiFlashDecimalMaxPrec) {
    return arrow::Status::Invalid("invalid decimal precision: ", precision);
  }
  if (scale < 0 || scale > kTiFlashDecimalMaxScale) {
    return arrow::Status::Invalid("invalid decimal scale: ", scale);
  }

  constexpr int32_t kArrowDecimal128MaxPrecision = 38;
  if (precision <= kArrowDecimal128MaxPrecision) {
    return arrow::decimal128(precision, scale);
  }
  return arrow::decimal256(precision, scale);
}

inline arrow::Result<std::pair<int32_t, int32_t>> GetDecimalPrecisionAndScale(
    const arrow::DataType& type) {
  if (type.id() != arrow::Type::DECIMAL128 && type.id() != arrow::Type::DECIMAL256) {
    return arrow::Status::Invalid("expected decimal type, got: ", type.ToString());
  }
  const auto& dec = static_cast<const arrow::DecimalType&>(type);
  return std::make_pair(dec.precision(), dec.scale());
}

inline std::pair<int32_t, int32_t> InferSumDecimalPrecisionAndScale(int32_t prec, int32_t scale) {
  const int32_t out_prec = std::min(prec + kTiFlashDecimalLongLongDigits, kTiFlashDecimalMaxPrec);
  return {out_prec, scale};
}

inline std::pair<int32_t, int32_t> InferAvgDecimalPrecisionAndScale(int32_t prec, int32_t scale) {
  const int32_t out_prec =
      std::min(prec + kTiFlashDefaultDivPrecisionIncrement, kTiFlashDecimalMaxPrec);
  const int32_t out_scale =
      std::min(scale + kTiFlashDefaultDivPrecisionIncrement, kTiFlashDecimalMaxScale);
  return {out_prec, out_scale};
}

inline int64_t AddWrapSigned(int64_t a, int64_t b) {
  const uint64_t ua = static_cast<uint64_t>(a);
  const uint64_t ub = static_cast<uint64_t>(b);
  return static_cast<int64_t>(ua + ub);
}

inline arrow::Result<arrow::compute::HashAggregateKernel> CopyFallbackHashKernel(
    arrow::compute::FunctionRegistry* fallback_registry, std::string_view func_name,
    const std::shared_ptr<arrow::DataType>& value_type) {
  if (fallback_registry == nullptr) {
    return arrow::Status::Invalid("fallback registry must not be null");
  }
  ARROW_ASSIGN_OR_RAISE(auto function, fallback_registry->GetFunction(std::string(func_name)));
  std::vector<arrow::TypeHolder> in_types;
  in_types.reserve(2);
  in_types.emplace_back(value_type);
  in_types.emplace_back(arrow::uint32());
  ARROW_ASSIGN_OR_RAISE(const arrow::compute::Kernel* kernel, function->DispatchExact(in_types));
  return *static_cast<const arrow::compute::HashAggregateKernel*>(kernel);
}

}  // namespace tiforth::function::detail

