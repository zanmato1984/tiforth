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

#include <arrow/compute/registry.h>
#include <arrow/status.h>

namespace tiforth::function {

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

}  // namespace tiforth::function
