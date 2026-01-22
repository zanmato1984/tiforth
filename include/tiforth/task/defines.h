#pragma once

#include <arrow/result.h>

#include <cstddef>

#include "tiforth/task/task_status.h"

namespace tiforth::task {

using TaskId = std::size_t;
using TaskResult = arrow::Result<TaskStatus>;

}  // namespace tiforth::task

