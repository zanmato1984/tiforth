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

#include <memory>
#include <optional>

#include <arrow/result.h>
#include <arrow/status.h>

#include <broken_pipeline/schedule/detail/callback_resumer.h>

#include "tiforth/traits.h"

namespace tiforth {

enum class BlockedKind {
  kWaiting,
  kWaitForNotify,
  kIOIn,
  kIOOut,
};

class BlockedResumer : public bp::schedule::detail::CallbackResumer {
 public:
  ~BlockedResumer() override = default;

  virtual BlockedKind kind() const = 0;

  // Returns the next blocked kind. Returning nullopt means unblocked and ready to resume.
  virtual arrow::Result<std::optional<BlockedKind>> ExecuteIO() = 0;
  virtual arrow::Result<std::optional<BlockedKind>> Await() = 0;
  virtual arrow::Status Notify() = 0;
};

using BlockedResumerPtr = std::shared_ptr<BlockedResumer>;

}  // namespace tiforth
