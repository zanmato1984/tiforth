#include "tiforth/operators/filter.h"

#include <cerrno>
#include <cstdlib>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/compute/api_vector.h>
#include <arrow/memory_pool.h>
#include <arrow/status.h>
#include <arrow/type.h>

#include "tiforth/compiled_expr.h"
#include "tiforth/engine.h"
#include "tiforth/detail/arrow_compute.h"

namespace tiforth {

namespace {

bool StringTruthy(std::string_view value) {
  // TiDB/MySQL-style truthiness for WHERE: coerce string to number (double),
  // then test != 0. Invalid parses produce 0.
  std::string tmp(value);
  char* end = nullptr;
  errno = 0;
  const double number = std::strtod(tmp.c_str(), &end);
  if (end == tmp.c_str()) {
    return false;
  }
  return number != 0.0;
}

template <typename ArrayType, typename ValueType = typename ArrayType::value_type>
arrow::Result<std::shared_ptr<arrow::Array>> NumericTruthy(const ArrayType& values,
                                                           arrow::MemoryPool* memory_pool) {
  arrow::BooleanBuilder builder(memory_pool);
  ARROW_RETURN_NOT_OK(builder.Reserve(values.length()));
  for (int64_t row = 0; row < values.length(); ++row) {
    if (values.IsNull(row)) {
      ARROW_RETURN_NOT_OK(builder.AppendNull());
      continue;
    }
    const auto v = static_cast<ValueType>(values.Value(row));
    ARROW_RETURN_NOT_OK(builder.Append(v != static_cast<ValueType>(0)));
  }
  std::shared_ptr<arrow::Array> out;
  ARROW_RETURN_NOT_OK(builder.Finish(&out));
  return out;
}

arrow::Result<std::shared_ptr<arrow::Array>> CoercePredicateToBoolean(
    const std::shared_ptr<arrow::Array>& predicate_array, arrow::compute::ExecContext* exec_context) {
  if (predicate_array == nullptr) {
    return arrow::Status::Invalid("filter predicate result must not be null");
  }

  if (predicate_array->type_id() == arrow::Type::BOOL) {
    return predicate_array;
  }

  auto* memory_pool = exec_context != nullptr ? exec_context->memory_pool() : arrow::default_memory_pool();

  switch (predicate_array->type_id()) {
    case arrow::Type::INT8:
      return NumericTruthy<arrow::Int8Array>(
          static_cast<const arrow::Int8Array&>(*predicate_array), memory_pool);
    case arrow::Type::INT16:
      return NumericTruthy<arrow::Int16Array>(
          static_cast<const arrow::Int16Array&>(*predicate_array), memory_pool);
    case arrow::Type::INT32:
      return NumericTruthy<arrow::Int32Array>(
          static_cast<const arrow::Int32Array&>(*predicate_array), memory_pool);
    case arrow::Type::INT64:
      return NumericTruthy<arrow::Int64Array>(
          static_cast<const arrow::Int64Array&>(*predicate_array), memory_pool);
    case arrow::Type::UINT8:
      return NumericTruthy<arrow::UInt8Array>(
          static_cast<const arrow::UInt8Array&>(*predicate_array), memory_pool);
    case arrow::Type::UINT16:
      return NumericTruthy<arrow::UInt16Array>(
          static_cast<const arrow::UInt16Array&>(*predicate_array), memory_pool);
    case arrow::Type::UINT32:
      return NumericTruthy<arrow::UInt32Array>(
          static_cast<const arrow::UInt32Array&>(*predicate_array), memory_pool);
    case arrow::Type::UINT64:
      return NumericTruthy<arrow::UInt64Array>(
          static_cast<const arrow::UInt64Array&>(*predicate_array), memory_pool);
    case arrow::Type::FLOAT:
      return NumericTruthy<arrow::FloatArray>(
          static_cast<const arrow::FloatArray&>(*predicate_array), memory_pool);
    case arrow::Type::DOUBLE:
      return NumericTruthy<arrow::DoubleArray>(
          static_cast<const arrow::DoubleArray&>(*predicate_array), memory_pool);
    case arrow::Type::STRING: {
      const auto& values = static_cast<const arrow::StringArray&>(*predicate_array);
      arrow::BooleanBuilder builder(memory_pool);
      ARROW_RETURN_NOT_OK(builder.Reserve(values.length()));
      for (int64_t row = 0; row < values.length(); ++row) {
        if (values.IsNull(row)) {
          ARROW_RETURN_NOT_OK(builder.AppendNull());
          continue;
        }
        ARROW_RETURN_NOT_OK(builder.Append(StringTruthy(values.GetView(row))));
      }
      std::shared_ptr<arrow::Array> out;
      ARROW_RETURN_NOT_OK(builder.Finish(&out));
      return out;
    }
    case arrow::Type::LARGE_STRING: {
      const auto& values = static_cast<const arrow::LargeStringArray&>(*predicate_array);
      arrow::BooleanBuilder builder(memory_pool);
      ARROW_RETURN_NOT_OK(builder.Reserve(values.length()));
      for (int64_t row = 0; row < values.length(); ++row) {
        if (values.IsNull(row)) {
          ARROW_RETURN_NOT_OK(builder.AppendNull());
          continue;
        }
        ARROW_RETURN_NOT_OK(builder.Append(StringTruthy(values.GetView(row))));
      }
      std::shared_ptr<arrow::Array> out;
      ARROW_RETURN_NOT_OK(builder.Finish(&out));
      return out;
    }
    case arrow::Type::BINARY: {
      const auto& values = static_cast<const arrow::BinaryArray&>(*predicate_array);
      arrow::BooleanBuilder builder(memory_pool);
      ARROW_RETURN_NOT_OK(builder.Reserve(values.length()));
      for (int64_t row = 0; row < values.length(); ++row) {
        if (values.IsNull(row)) {
          ARROW_RETURN_NOT_OK(builder.AppendNull());
          continue;
        }
        ARROW_RETURN_NOT_OK(builder.Append(StringTruthy(values.GetView(row))));
      }
      std::shared_ptr<arrow::Array> out;
      ARROW_RETURN_NOT_OK(builder.Finish(&out));
      return out;
    }
    case arrow::Type::LARGE_BINARY: {
      const auto& values = static_cast<const arrow::LargeBinaryArray&>(*predicate_array);
      arrow::BooleanBuilder builder(memory_pool);
      ARROW_RETURN_NOT_OK(builder.Reserve(values.length()));
      for (int64_t row = 0; row < values.length(); ++row) {
        if (values.IsNull(row)) {
          ARROW_RETURN_NOT_OK(builder.AppendNull());
          continue;
        }
        ARROW_RETURN_NOT_OK(builder.Append(StringTruthy(values.GetView(row))));
      }
      std::shared_ptr<arrow::Array> out;
      ARROW_RETURN_NOT_OK(builder.Finish(&out));
      return out;
    }
    default:
      return arrow::Status::Invalid(
          "filter predicate must evaluate to boolean or a scalar type convertible to boolean (got type: ",
          predicate_array->type()->ToString(), ")");
  }
}

}  // namespace

struct FilterTransformOp::Compiled {
  CompiledExpr predicate;
};

FilterTransformOp::~FilterTransformOp() = default;

FilterTransformOp::FilterTransformOp(const Engine* engine, ExprPtr predicate,
                                     arrow::MemoryPool* memory_pool)
    : engine_(engine),
      predicate_(std::move(predicate)),
      exec_context_(memory_pool != nullptr
                        ? memory_pool
                        : (engine != nullptr ? engine->memory_pool() : arrow::default_memory_pool()),
                    /*executor=*/nullptr,
                    engine != nullptr ? engine->function_registry() : nullptr) {}

arrow::Result<OperatorStatus> FilterTransformOp::TransformImpl(
    std::shared_ptr<arrow::RecordBatch>* batch) {
  if (*batch == nullptr) {
    return OperatorStatus::kHasOutput;
  }
  if (engine_ == nullptr) {
    return arrow::Status::Invalid("filter engine must not be null");
  }
  if (predicate_ == nullptr) {
    return arrow::Status::Invalid("filter predicate must not be null");
  }

  const auto& input = **batch;
  if (output_schema_ == nullptr) {
    output_schema_ = input.schema();
  } else if (!output_schema_->Equals(*input.schema(), /*check_metadata=*/true)) {
    return arrow::Status::Invalid("filter input schema mismatch");
  }

  ARROW_RETURN_NOT_OK(detail::EnsureArrowComputeInitialized());

  if (compiled_ == nullptr) {
    if (predicate_ == nullptr) {
      return arrow::Status::Invalid("filter predicate must not be null");
    }
    ARROW_ASSIGN_OR_RAISE(auto compiled_pred,
                          CompileExpr(output_schema_, *predicate_, engine_, &exec_context_));
    compiled_ = std::make_unique<Compiled>(Compiled{std::move(compiled_pred)});
  }

  ARROW_ASSIGN_OR_RAISE(auto predicate_array, ExecuteExprAsArray(compiled_->predicate, input, &exec_context_));
  ARROW_ASSIGN_OR_RAISE(predicate_array, CoercePredicateToBoolean(predicate_array, &exec_context_));
  if (predicate_array->length() != input.num_rows()) {
    return arrow::Status::Invalid("filter predicate length mismatch");
  }
  if (predicate_array->type_id() != arrow::Type::BOOL) {
    return arrow::Status::Invalid("filter predicate must evaluate to boolean");
  }

  const arrow::compute::FilterOptions options(arrow::compute::FilterOptions::DROP);

  std::vector<std::shared_ptr<arrow::Array>> out_columns;
  out_columns.reserve(input.num_columns());
  for (int i = 0; i < input.num_columns(); ++i) {
    ARROW_ASSIGN_OR_RAISE(auto filtered,
                          arrow::compute::Filter(arrow::Datum(input.column(i)),
                                                 arrow::Datum(predicate_array), options,
                                                 &exec_context_));
    ARROW_ASSIGN_OR_RAISE(auto out_array,
                          detail::DatumToArray(filtered, exec_context_.memory_pool()));
    out_columns.push_back(std::move(out_array));
  }

  int64_t out_rows = 0;
  if (!out_columns.empty()) {
    out_rows = out_columns.front()->length();
  } else {
    ARROW_ASSIGN_OR_RAISE(auto filtered_pred,
                          arrow::compute::Filter(arrow::Datum(predicate_array),
                                                 arrow::Datum(predicate_array), options,
                                                 &exec_context_));
    ARROW_ASSIGN_OR_RAISE(auto filtered_pred_array,
                          detail::DatumToArray(filtered_pred, exec_context_.memory_pool()));
    out_rows = filtered_pred_array->length();
  }

  *batch = arrow::RecordBatch::Make(output_schema_, out_rows, std::move(out_columns));
  return OperatorStatus::kHasOutput;
}

}  // namespace tiforth
