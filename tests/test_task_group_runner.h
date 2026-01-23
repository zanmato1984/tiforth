#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

#include <arrow/result.h>
#include <arrow/status.h>

#include "tiforth/task/blocked_resumer.h"
#include "tiforth/task/task_context.h"
#include "tiforth/task/task_group.h"
#include "tiforth/task/task_status.h"

namespace tiforth::test {

class SimpleResumer final : public task::Resumer {
 public:
  void Resume() override { resumed_.store(true, std::memory_order_release); }
  bool IsResumed() const override { return resumed_.load(std::memory_order_acquire); }

 private:
  std::atomic_bool resumed_{false};
};

class ResumersAwaiter final : public task::Awaiter {
 public:
  explicit ResumersAwaiter(task::Resumers resumers) : resumers_(std::move(resumers)) {}

  const task::Resumers& resumers() const { return resumers_; }

 private:
  task::Resumers resumers_;
};

inline task::TaskContext MakeTestTaskContext() {
  task::TaskContext task_ctx;
  task_ctx.resumer_factory = []() -> arrow::Result<task::ResumerPtr> {
    return std::make_shared<SimpleResumer>();
  };
  task_ctx.any_awaiter_factory = [](task::Resumers resumers) -> arrow::Result<task::AwaiterPtr> {
    return std::make_shared<ResumersAwaiter>(std::move(resumers));
  };
  return task_ctx;
}

inline arrow::Status DriveBlockedResumer(const task::BlockedResumerPtr& resumer) {
  if (resumer == nullptr) {
    return arrow::Status::Invalid("blocked resumer must not be null");
  }

  switch (resumer->kind()) {
    case task::BlockedKind::kIOIn:
    case task::BlockedKind::kIOOut: {
      ARROW_ASSIGN_OR_RAISE(const auto next, resumer->ExecuteIO());
      if (!next.has_value()) {
        resumer->Resume();
      }
      return arrow::Status::OK();
    }
    case task::BlockedKind::kWaiting: {
      ARROW_ASSIGN_OR_RAISE(const auto next, resumer->Await());
      if (!next.has_value()) {
        resumer->Resume();
      }
      return arrow::Status::OK();
    }
    case task::BlockedKind::kWaitForNotify: {
      ARROW_RETURN_NOT_OK(resumer->Notify());
      resumer->Resume();
      return arrow::Status::OK();
    }
  }
  return arrow::Status::Invalid("unknown blocked resumer kind");
}

inline arrow::Status DriveAwaiter(const task::AwaiterPtr& awaiter) {
  if (awaiter == nullptr) {
    return arrow::Status::Invalid("awaiter must not be null");
  }

  auto resumers_awaiter = std::dynamic_pointer_cast<ResumersAwaiter>(awaiter);
  if (resumers_awaiter == nullptr) {
    return arrow::Status::Invalid("test runner expected ResumersAwaiter");
  }

  for (const auto& resumer : resumers_awaiter->resumers()) {
    if (resumer == nullptr) {
      return arrow::Status::Invalid("awaiter resumer must not be null");
    }
    auto blocked = std::dynamic_pointer_cast<task::BlockedResumer>(resumer);
    if (blocked == nullptr) {
      return arrow::Status::Invalid("test runner can only drive BlockedResumer");
    }
    ARROW_RETURN_NOT_OK(DriveBlockedResumer(std::move(blocked)));
  }

  return arrow::Status::OK();
}

inline arrow::Status RunTaskToCompletion(const task::Task& task, const task::TaskContext& task_ctx,
                                         task::TaskId task_id) {
  while (true) {
    ARROW_ASSIGN_OR_RAISE(auto status, task(task_ctx, task_id));
    if (status.IsFinished()) {
      return arrow::Status::OK();
    }
    if (status.IsContinue()) {
      continue;
    }
    if (status.IsYield()) {
      std::this_thread::yield();
      continue;
    }
    if (status.IsCancelled()) {
      return arrow::Status::Cancelled("task cancelled");
    }
    if (status.IsBlocked()) {
      ARROW_RETURN_NOT_OK(DriveAwaiter(status.GetAwaiter()));
      continue;
    }
    return arrow::Status::Invalid("unknown task status");
  }
}

inline arrow::Status RunTaskGroupToCompletion(const task::TaskGroup& group,
                                              const task::TaskContext& task_ctx) {
  if (!group.GetTask()) {
    return arrow::Status::Invalid("task group must have a task");
  }
  if (group.NumTasks() == 0) {
    return arrow::Status::Invalid("task group num_tasks must be positive");
  }

  std::mutex mu;
  arrow::Status first_error;
  std::atomic_bool has_error{false};

  std::vector<std::thread> threads;
  threads.reserve(group.NumTasks());
  for (task::TaskId task_id = 0; task_id < group.NumTasks(); ++task_id) {
    threads.emplace_back([&, task_id]() {
      auto st = RunTaskToCompletion(group.GetTask(), task_ctx, task_id);
      if (st.ok()) {
        return;
      }
      if (!has_error.exchange(true)) {
        std::lock_guard<std::mutex> lock(mu);
        first_error = std::move(st);
      }
    });
  }

  for (auto& th : threads) {
    th.join();
  }

  if (has_error.load()) {
    if (first_error.ok()) {
      return arrow::Status::Invalid("task group failed without error status");
    }
    return first_error;
  }

  if (group.GetContinuation().has_value()) {
    const auto& cont = *group.GetContinuation();
    while (true) {
      ARROW_ASSIGN_OR_RAISE(auto st, cont(task_ctx));
      if (st.IsFinished()) {
        break;
      }
      if (st.IsContinue()) {
        continue;
      }
      if (st.IsYield()) {
        std::this_thread::yield();
        continue;
      }
      if (st.IsCancelled()) {
        return arrow::Status::Cancelled("continuation cancelled");
      }
      if (st.IsBlocked()) {
        ARROW_RETURN_NOT_OK(DriveAwaiter(st.GetAwaiter()));
        continue;
      }
      return arrow::Status::Invalid("unknown continuation status");
    }
  }

  return arrow::Status::OK();
}

inline arrow::Status RunTaskGroupsToCompletion(const task::TaskGroups& groups,
                                               const task::TaskContext& task_ctx) {
  for (const auto& group : groups) {
    ARROW_RETURN_NOT_OK(RunTaskGroupToCompletion(group, task_ctx));
  }
  return arrow::Status::OK();
}

}  // namespace tiforth::test

