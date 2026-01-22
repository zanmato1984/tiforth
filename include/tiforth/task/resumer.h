#pragma once

#include <arrow/result.h>

#include <functional>
#include <memory>
#include <vector>

namespace tiforth::task {

class Resumer {
 public:
  virtual ~Resumer() = default;
  virtual void Resume() = 0;
  virtual bool IsResumed() const = 0;
};

using ResumerPtr = std::shared_ptr<Resumer>;
using Resumers = std::vector<ResumerPtr>;

using ResumerFactory = std::function<arrow::Result<ResumerPtr>()>;

}  // namespace tiforth::task

