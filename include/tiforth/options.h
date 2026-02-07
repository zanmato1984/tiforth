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

#pragma once

#include <cstddef>

namespace tiforth {

struct Options {
  // Keep the defaults aligned with Arrow (22.0.0):
  // - arrow::acero::ExecPlan::kMaxBatchSize == 1<<15
  // - arrow::util::MiniBatch::kMiniBatchLength == 1<<10
  static constexpr std::size_t kDefaultMaxBatchLength = 1U << 15;
  static constexpr std::size_t kDefaultMiniBatchLength = 1U << 10;

  std::size_t source_max_batch_length = kDefaultMaxBatchLength;
  std::size_t pipe_max_batch_length = kDefaultMaxBatchLength;
  std::size_t mini_batch_length = kDefaultMiniBatchLength;
};

}  // namespace tiforth
