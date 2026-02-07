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

arrow::Status RegisterHashCount(arrow::compute::FunctionRegistry* registry);
arrow::Status RegisterHashCountAll(arrow::compute::FunctionRegistry* registry);
arrow::Status RegisterHashSum(arrow::compute::FunctionRegistry* registry,
                             arrow::compute::FunctionRegistry* fallback_registry);
arrow::Status RegisterHashMin(arrow::compute::FunctionRegistry* registry,
                             arrow::compute::FunctionRegistry* fallback_registry);
arrow::Status RegisterHashMax(arrow::compute::FunctionRegistry* registry,
                             arrow::compute::FunctionRegistry* fallback_registry);
arrow::Status RegisterHashMean(arrow::compute::FunctionRegistry* registry,
                              arrow::compute::FunctionRegistry* fallback_registry);

arrow::Status RegisterHashAggregateFunctions(arrow::compute::FunctionRegistry* registry,
                                            arrow::compute::FunctionRegistry* fallback_registry) {
  if (registry == nullptr) {
    return arrow::Status::Invalid("function registry must not be null");
  }
  if (fallback_registry == nullptr) {
    return arrow::Status::Invalid("fallback function registry must not be null");
  }

  ARROW_RETURN_NOT_OK(RegisterHashCount(registry));
  ARROW_RETURN_NOT_OK(RegisterHashCountAll(registry));
  ARROW_RETURN_NOT_OK(RegisterHashSum(registry, fallback_registry));
  ARROW_RETURN_NOT_OK(RegisterHashMin(registry, fallback_registry));
  ARROW_RETURN_NOT_OK(RegisterHashMax(registry, fallback_registry));
  ARROW_RETURN_NOT_OK(RegisterHashMean(registry, fallback_registry));
  return arrow::Status::OK();
}

}  // namespace tiforth::function

