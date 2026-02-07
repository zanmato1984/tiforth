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

#include "tiforth/engine.h"

#include <utility>

#include <arrow/status.h>
#include <arrow/compute/registry.h>

#include "tiforth/detail/arrow_compute.h"
#include "tiforth/functions/register.h"
#include "tiforth/spill.h"

namespace tiforth {

arrow::Result<std::unique_ptr<Engine>> Engine::Create(EngineOptions options) {
  if (options.memory_pool == nullptr) {
    return arrow::Status::Invalid("EngineOptions.memory_pool must not be null");
  }
  if (options.spill_manager == nullptr) {
    options.spill_manager = std::make_shared<DenySpillManager>();
  }

  ARROW_RETURN_NOT_OK(detail::EnsureArrowComputeInitialized());

  if (options.function_registry == nullptr) {
    auto registry = arrow::compute::FunctionRegistry::Make(arrow::compute::GetFunctionRegistry());
    options.function_registry =
        std::shared_ptr<arrow::compute::FunctionRegistry>(std::move(registry));
  }
  if (options.function_registry == nullptr) {
    return arrow::Status::Invalid("EngineOptions.function_registry must not be null");
  }
  ARROW_RETURN_NOT_OK(
      function::RegisterTiforthFunctions(options.function_registry.get(),
                                         arrow::compute::GetFunctionRegistry()));

  return std::unique_ptr<Engine>(new Engine(std::move(options)));
}

Engine::Engine(EngineOptions options)
    : options_(std::move(options)),
      memory_pool_(options_.memory_pool),
      spill_manager_(options_.spill_manager),
      function_registry_(options_.function_registry) {}

Engine::~Engine() = default;

}  // namespace tiforth
