#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

#include <arrow/result.h>
#include <arrow/status.h>

#include "tiforth/tiforth.h"

namespace tiforth_example {

class SimpleResumer final : public tiforth::task::Resumer {
 public:
  void Resume() override { resumed_.store(true, std::memory_order_release); }
  bool IsResumed() const override { return resumed_.load(std::memory_order_acquire); }

 private:
  std::atomic_bool resumed_{false};
};

class ResumersAwaiter final : public tiforth::task::Awaiter {
 public:
  explicit ResumersAwaiter(tiforth::task::Resumers resumers) : resumers_(std::move(resumers)) {}
  const tiforth::task::Resumers& resumers() const { return resumers_; }

 private:
  tiforth::task::Resumers resumers_;
};

inline tiforth::task::TaskContext MakeTaskContext() {
  tiforth::task::TaskContext task_ctx;
  task_ctx.resumer_factory = []() -> arrow::Result<tiforth::task::ResumerPtr> {
    return std::make_shared<SimpleResumer>();
  };
  task_ctx.any_awaiter_factory =
      [](tiforth::task::Resumers resumers) -> arrow::Result<tiforth::task::AwaiterPtr> {
    return std::make_shared<ResumersAwaiter>(std::move(resumers));
  };
  return task_ctx;
}

inline arrow::Status DriveAwaiter(const tiforth::task::AwaiterPtr& awaiter) {
  if (awaiter == nullptr) {
    return arrow::Status::Invalid("awaiter must not be null");
  }
  auto resumers_awaiter = std::dynamic_pointer_cast<ResumersAwaiter>(awaiter);
  if (resumers_awaiter == nullptr) {
    return arrow::Status::Invalid("expected ResumersAwaiter");
  }
  for (const auto& resumer : resumers_awaiter->resumers()) {
    auto blocked = std::dynamic_pointer_cast<tiforth::task::BlockedResumer>(resumer);
    if (blocked == nullptr) {
      return arrow::Status::Invalid("example runner only supports BlockedResumer");
    }
    switch (blocked->kind()) {
      case tiforth::task::BlockedKind::kIOIn:
      case tiforth::task::BlockedKind::kIOOut: {
        ARROW_ASSIGN_OR_RAISE(const auto next, blocked->ExecuteIO());
        if (!next.has_value()) {
          blocked->Resume();
        }
        break;
      }
      case tiforth::task::BlockedKind::kWaiting: {
        ARROW_ASSIGN_OR_RAISE(const auto next, blocked->Await());
        if (!next.has_value()) {
          blocked->Resume();
        }
        break;
      }
      case tiforth::task::BlockedKind::kWaitForNotify:
        ARROW_RETURN_NOT_OK(blocked->Notify());
        blocked->Resume();
        break;
    }
  }
  return arrow::Status::OK();
}

inline arrow::Status RunTaskGroupToCompletion(const tiforth::task::TaskGroup& group,
                                              const tiforth::task::TaskContext& task_ctx) {
  if (!group.GetTask()) {
    return arrow::Status::Invalid("task group must have a task");
  }
  if (group.NumTasks() == 0) {
    return arrow::Status::Invalid("task group num_tasks must be positive");
  }

  std::atomic_bool has_error{false};
  arrow::Status first_error;
  std::mutex mu;

  std::vector<std::thread> threads;
  threads.reserve(group.NumTasks());
  for (tiforth::task::TaskId task_id = 0; task_id < group.NumTasks(); ++task_id) {
    threads.emplace_back([&, task_id]() {
      while (true) {
        auto st_res = group.GetTask()(task_ctx, task_id);
        if (!st_res.ok()) {
          if (!has_error.exchange(true)) {
            std::lock_guard<std::mutex> lock(mu);
            first_error = st_res.status();
          }
          return;
        }
        auto st = st_res.ValueUnsafe();
        if (st.IsFinished()) {
          return;
        }
        if (st.IsContinue()) {
          continue;
        }
        if (st.IsYield()) {
          std::this_thread::yield();
          continue;
        }
        if (st.IsCancelled()) {
          if (!has_error.exchange(true)) {
            std::lock_guard<std::mutex> lock(mu);
            first_error = arrow::Status::Cancelled("task cancelled");
          }
          return;
        }
        if (st.IsBlocked()) {
          auto drive_st = DriveAwaiter(st.GetAwaiter());
          if (!drive_st.ok() && !has_error.exchange(true)) {
            std::lock_guard<std::mutex> lock(mu);
            first_error = std::move(drive_st);
          }
          continue;
        }
        if (!has_error.exchange(true)) {
          std::lock_guard<std::mutex> lock(mu);
          first_error = arrow::Status::Invalid("unknown task status");
        }
        return;
      }
    });
  }

  for (auto& th : threads) {
    th.join();
  }
  if (has_error.load()) {
    return first_error.ok() ? arrow::Status::Invalid("task group failed") : first_error;
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

inline arrow::Status RunTaskGroupsToCompletion(const tiforth::task::TaskGroups& groups,
                                               const tiforth::task::TaskContext& task_ctx) {
  for (const auto& group : groups) {
    ARROW_RETURN_NOT_OK(RunTaskGroupToCompletion(group, task_ctx));
  }
  return arrow::Status::OK();
}

}  // namespace tiforth_example

