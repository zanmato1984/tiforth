#pragma once

#include <arrow/status.h>

namespace arrow::compute {
class FunctionRegistry;
}  // namespace arrow::compute

namespace tiforth::function {

arrow::Status RegisterTiforthFunctions(arrow::compute::FunctionRegistry* registry,
                                       arrow::compute::FunctionRegistry* fallback_registry);

}  // namespace tiforth::function
