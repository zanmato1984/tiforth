#include <arrow/compute/registry.h>
#include <arrow/status.h>

namespace tiforth::functions {

arrow::Status RegisterDecimalArithmeticFunctions(arrow::compute::FunctionRegistry* registry,
                                                arrow::compute::FunctionRegistry* fallback_registry);
arrow::Status RegisterNumericArithmeticFunctions(arrow::compute::FunctionRegistry* registry,
                                                arrow::compute::FunctionRegistry* fallback_registry);

arrow::Status RegisterScalarArithmeticFunctions(arrow::compute::FunctionRegistry* registry,
                                               arrow::compute::FunctionRegistry* fallback_registry) {
  if (registry == nullptr) {
    return arrow::Status::Invalid("function registry must not be null");
  }
  if (fallback_registry == nullptr) {
    return arrow::Status::Invalid("fallback function registry must not be null");
  }

  ARROW_RETURN_NOT_OK(RegisterDecimalArithmeticFunctions(registry, fallback_registry));
  ARROW_RETURN_NOT_OK(RegisterNumericArithmeticFunctions(registry, fallback_registry));
  return arrow::Status::OK();
}

}  // namespace tiforth::functions

