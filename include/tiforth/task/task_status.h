#pragma once

#include <arrow/util/logging.h>

#include <memory>
#include <string>

#include "tiforth/task/awaiter.h"

namespace tiforth::task {

class TaskStatus {
 private:
  enum class Code {
    kContinue,
    kBlocked,
    kYield,
    kFinished,
    kCancelled,
  };

 public:
  TaskStatus() = default;

  bool IsContinue() const { return code_ == Code::kContinue; }
  bool IsBlocked() const { return code_ == Code::kBlocked; }
  bool IsYield() const { return code_ == Code::kYield; }
  bool IsFinished() const { return code_ == Code::kFinished; }
  bool IsCancelled() const { return code_ == Code::kCancelled; }

  AwaiterPtr& GetAwaiter() {
    ARROW_CHECK(IsBlocked());
    return awaiter_;
  }

  const AwaiterPtr& GetAwaiter() const {
    ARROW_CHECK(IsBlocked());
    return awaiter_;
  }

  bool operator==(const TaskStatus& other) const { return code_ == other.code_; }

  std::string ToString() const {
    switch (code_) {
      case Code::kContinue:
        return "CONTINUE";
      case Code::kBlocked:
        return "BLOCKED";
      case Code::kYield:
        return "YIELD";
      case Code::kFinished:
        return "FINISHED";
      case Code::kCancelled:
        return "CANCELLED";
    }
    return "UNKNOWN";
  }

  static TaskStatus Continue() { return TaskStatus(Code::kContinue); }
  static TaskStatus Blocked(AwaiterPtr awaiter) { return TaskStatus(std::move(awaiter)); }
  static TaskStatus Yield() { return TaskStatus(Code::kYield); }
  static TaskStatus Finished() { return TaskStatus(Code::kFinished); }
  static TaskStatus Cancelled() { return TaskStatus(Code::kCancelled); }

 private:
  explicit TaskStatus(Code code) : code_(code) {}

  explicit TaskStatus(AwaiterPtr awaiter)
      : code_(Code::kBlocked), awaiter_(std::move(awaiter)) {}

  Code code_ = Code::kContinue;
  AwaiterPtr awaiter_;
};

}  // namespace tiforth::task

