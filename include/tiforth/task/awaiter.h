#pragma once

#include <arrow/result.h>

#include <functional>
#include <memory>

#include "tiforth/task/resumer.h"

namespace tiforth::task {

class Awaiter {
 public:
  virtual ~Awaiter() = default;
};

using AwaiterPtr = std::shared_ptr<Awaiter>;

using SingleAwaiterFactory = std::function<arrow::Result<AwaiterPtr>(ResumerPtr)>;
using AnyAwaiterFactory = std::function<arrow::Result<AwaiterPtr>(Resumers)>;
using AllAwaiterFactory = std::function<arrow::Result<AwaiterPtr>(Resumers)>;

}  // namespace tiforth::task

