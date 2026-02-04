#pragma once

#include <memory>
#include <optional>

#include <arrow/result.h>
#include <arrow/status.h>

#include "tiforth/broken_pipeline_traits.h"

namespace tiforth {

enum class BlockedKind {
  kWaiting,
  kWaitForNotify,
  kIOIn,
  kIOOut,
};

class BlockedResumer : public Resumer {
 public:
  ~BlockedResumer() override = default;

  virtual BlockedKind kind() const = 0;

  // Returns the next blocked kind. Returning nullopt means unblocked and ready to resume.
  virtual arrow::Result<std::optional<BlockedKind>> ExecuteIO() = 0;
  virtual arrow::Result<std::optional<BlockedKind>> Await() = 0;
  virtual arrow::Status Notify() = 0;
};

}  // namespace tiforth

