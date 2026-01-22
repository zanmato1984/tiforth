#pragma once

#include <arrow/status.h>

#include <functional>
#include <string>
#include <utility>

#include "tiforth/task/defines.h"
#include "tiforth/task/task_context.h"

namespace tiforth::task {

class Task {
 public:
  using Func = std::function<TaskResult(const TaskContext&, TaskId)>;

  Task() = default;

  explicit Task(Func func) : func_(std::move(func)) {}
  Task(std::string name, Func func) : name_(std::move(name)), func_(std::move(func)) {}

  TaskResult operator()(const TaskContext& ctx, TaskId task_id) const {
    if (!func_) {
      return arrow::Status::Invalid("task function must not be empty");
    }
    return func_(ctx, task_id);
  }

  explicit operator bool() const { return static_cast<bool>(func_); }

  const std::string& name() const { return name_; }

 private:
  std::string name_;
  Func func_;
};

class Continuation {
 public:
  using Func = std::function<TaskResult(const TaskContext&)>;

  Continuation() = default;

  explicit Continuation(Func func) : func_(std::move(func)) {}
  Continuation(std::string name, Func func) : name_(std::move(name)), func_(std::move(func)) {}

  TaskResult operator()(const TaskContext& ctx) const {
    if (!func_) {
      return arrow::Status::Invalid("continuation function must not be empty");
    }
    return func_(ctx);
  }

  explicit operator bool() const { return static_cast<bool>(func_); }

  const std::string& name() const { return name_; }

 private:
  std::string name_;
  Func func_;
};

}  // namespace tiforth::task

