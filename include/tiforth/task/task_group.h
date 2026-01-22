#pragma once

#include <arrow/status.h>

#include <cstddef>
#include <functional>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "tiforth/task/task.h"

namespace tiforth::task {

class TaskGroup {
 public:
  using NotifyFinishFunc = std::function<arrow::Status(const TaskContext&)>;

  TaskGroup() = default;

  TaskGroup(std::string name, Task task, std::size_t num_tasks,
            std::optional<Continuation> continuation, NotifyFinishFunc notify_finish)
      : name_(std::move(name)),
        task_(std::move(task)),
        num_tasks_(num_tasks),
        continuation_(std::move(continuation)),
        notify_finish_(std::move(notify_finish)) {}

  const std::string& name() const { return name_; }

  const Task& GetTask() const { return task_; }
  std::size_t NumTasks() const { return num_tasks_; }
  const std::optional<Continuation>& GetContinuation() const { return continuation_; }

  arrow::Status NotifyFinish(const TaskContext& ctx) const {
    if (!notify_finish_) {
      return arrow::Status::OK();
    }
    return notify_finish_(ctx);
  }

 private:
  std::string name_;
  Task task_;
  std::size_t num_tasks_ = 0;
  std::optional<Continuation> continuation_;
  NotifyFinishFunc notify_finish_;
};

using TaskGroups = std::vector<TaskGroup>;

}  // namespace tiforth::task

