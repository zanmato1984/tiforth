#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

#include <arrow/result.h>
#include <arrow/status.h>

#include "blocked_resumer.h"
#include "tiforth/tiforth.h"

namespace tiforth_example {

class SimpleResumer final : public tiforth::Resumer {
 public:
  void Resume() override { resumed_.store(true, std::memory_order_release); }
  bool IsResumed() const override { return resumed_.load(std::memory_order_acquire); }

 private:
  std::atomic_bool resumed_{false};
};

class ResumersAwaiter final : public tiforth::Awaiter {
 public:
  explicit ResumersAwaiter(tiforth::Resumers resumers) : resumers_(std::move(resumers)) {}
  const tiforth::Resumers& resumers() const { return resumers_; }

 private:
  tiforth::Resumers resumers_;
};

inline tiforth::TaskContext MakeTaskContext() {
  tiforth::TaskContext task_ctx;
  task_ctx.resumer_factory = []() -> arrow::Result<tiforth::ResumerPtr> {
    return std::make_shared<SimpleResumer>();
  };
  task_ctx.awaiter_factory =
      [](tiforth::Resumers resumers) -> arrow::Result<tiforth::AwaiterPtr> {
    return std::make_shared<ResumersAwaiter>(std::move(resumers));
  };
  return task_ctx;
}

inline arrow::Status ValidatePipelineForExample(const tiforth::Pipeline& pipeline) {
  if (pipeline.Sink() == nullptr) {
    return arrow::Status::Invalid("pipeline sink must not be null");
  }
  for (const auto& ch : pipeline.Channels()) {
    if (ch.source_op == nullptr) {
      return arrow::Status::Invalid("pipeline channel source must not be null");
    }
    for (const auto* pipe : ch.pipe_ops) {
      if (pipe == nullptr) {
        return arrow::Status::Invalid("pipeline pipe op must not be null");
      }
    }
  }
  return arrow::Status::OK();
}

inline arrow::Result<tiforth::TaskGroups> CompileToTaskGroups(const tiforth::Pipeline& pipeline,
                                                             std::size_t dop) {
  if (dop == 0) {
    return arrow::Status::Invalid("dop must be positive");
  }
  ARROW_RETURN_NOT_OK(ValidatePipelineForExample(pipeline));
  auto exec = bp::Compile(pipeline, dop);

  tiforth::TaskGroups groups;
  groups.reserve(exec.Pipelinexes().size() + 8);

  if (exec.Sink().backend.has_value()) {
    groups.push_back(*exec.Sink().backend);
  }

  for (const auto& seg : exec.Pipelinexes()) {
    std::vector<tiforth::SourceOp*> sources;
    sources.reserve(seg.Channels().size());
    for (const auto& ch : seg.Channels()) {
      if (ch.source_op == nullptr) {
        return arrow::Status::Invalid("pipeline channel source must not be null");
      }
      bool seen = false;
      for (auto* existing : sources) {
        if (existing == ch.source_op) {
          seen = true;
          break;
        }
      }
      if (!seen) {
        sources.push_back(ch.source_op);
      }
    }

    for (auto* src : sources) {
      if (auto backend = src->Backend(); backend.has_value()) {
        groups.push_back(std::move(*backend));
      }
    }

    for (auto* src : sources) {
      auto frontends = src->Frontend();
      for (auto& g : frontends) {
        groups.push_back(std::move(g));
      }
    }

    auto stage = seg.PipeExec().TaskGroup();
    groups.emplace_back(seg.Name(), stage.Task(), stage.NumTasks(), stage.Continuation());
  }

  for (const auto& g : exec.Sink().frontend) {
    groups.push_back(g);
  }

  return groups;
}

inline arrow::Status DriveAwaiter(const tiforth::AwaiterPtr& awaiter) {
  if (awaiter == nullptr) {
    return arrow::Status::Invalid("awaiter must not be null");
  }
  auto resumers_awaiter = std::dynamic_pointer_cast<ResumersAwaiter>(awaiter);
  if (resumers_awaiter == nullptr) {
    return arrow::Status::Invalid("expected ResumersAwaiter");
  }
  for (const auto& resumer : resumers_awaiter->resumers()) {
    auto blocked = std::dynamic_pointer_cast<tiforth::BlockedResumer>(resumer);
    if (blocked == nullptr) {
      return arrow::Status::Invalid("example runner only supports BlockedResumer");
    }
    switch (blocked->kind()) {
      case tiforth::BlockedKind::kIOIn:
      case tiforth::BlockedKind::kIOOut: {
        ARROW_ASSIGN_OR_RAISE(const auto next, blocked->ExecuteIO());
        if (!next.has_value()) {
          blocked->Resume();
        }
        break;
      }
      case tiforth::BlockedKind::kWaiting: {
        ARROW_ASSIGN_OR_RAISE(const auto next, blocked->Await());
        if (!next.has_value()) {
          blocked->Resume();
        }
        break;
      }
      case tiforth::BlockedKind::kWaitForNotify:
        ARROW_RETURN_NOT_OK(blocked->Notify());
        blocked->Resume();
        break;
    }
  }
  return arrow::Status::OK();
}

inline arrow::Status RunTaskGroupToCompletion(const tiforth::TaskGroup& group,
                                              const tiforth::TaskContext& task_ctx) {
  if (group.NumTasks() == 0) {
    return arrow::Status::Invalid("task group num_tasks must be positive");
  }

  std::atomic_bool has_error{false};
  arrow::Status first_error;
  std::mutex mu;

  std::vector<std::thread> threads;
  threads.reserve(group.NumTasks());
  for (tiforth::TaskId task_id = 0; task_id < group.NumTasks(); ++task_id) {
    threads.emplace_back([&, task_id]() {
      while (true) {
        auto st_res = group.Task()(task_ctx, task_id);
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

  if (group.Continuation().has_value()) {
    const auto& cont = *group.Continuation();
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

inline arrow::Status RunTaskGroupsToCompletion(const tiforth::TaskGroups& groups,
                                               const tiforth::TaskContext& task_ctx) {
  for (const auto& group : groups) {
    ARROW_RETURN_NOT_OK(RunTaskGroupToCompletion(group, task_ctx));
  }
  return arrow::Status::OK();
}

}  // namespace tiforth_example
