#pragma once

#include <memory>

#include "tiforth/query_context.h"
#include "tiforth/task/awaiter.h"
#include "tiforth/task/resumer.h"

namespace tiforth::task {

struct TaskContext {
  const ::tiforth::QueryContext* query_ctx = nullptr;
  ResumerFactory resumer_factory;
  SingleAwaiterFactory single_awaiter_factory;
  AnyAwaiterFactory any_awaiter_factory;
  AllAwaiterFactory all_awaiter_factory;
};

}  // namespace tiforth::task

