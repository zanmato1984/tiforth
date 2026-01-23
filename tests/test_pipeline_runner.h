#pragma once

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/util/logging.h>

#include "tiforth/stash/operators.h"
#include "tiforth/stash/pipeline_exec.h"

namespace tiforth::test {

namespace detail {

inline arrow::Status RunPipelineExecToCompletion(PipelineExec* exec) {
  if (exec == nullptr) {
    return arrow::Status::Invalid("pipeline exec must not be null");
  }

  while (true) {
    ARROW_ASSIGN_OR_RAISE(auto status, exec->Execute());
    switch (status) {
      case OperatorStatus::kNeedInput:
        std::this_thread::yield();
        continue;
      case OperatorStatus::kFinished:
        return arrow::Status::OK();
      case OperatorStatus::kCancelled:
        return arrow::Status::Cancelled("pipeline task cancelled");
      case OperatorStatus::kIOIn:
      case OperatorStatus::kIOOut: {
        while (status == OperatorStatus::kIOIn || status == OperatorStatus::kIOOut) {
          ARROW_ASSIGN_OR_RAISE(status, exec->ExecuteIO());
        }
        continue;
      }
      case OperatorStatus::kWaiting: {
        while (status == OperatorStatus::kWaiting) {
          ARROW_ASSIGN_OR_RAISE(status, exec->Await());
        }
        continue;
      }
      case OperatorStatus::kWaitForNotify:
        ARROW_RETURN_NOT_OK(exec->Notify());
        continue;
      case OperatorStatus::kHasOutput:
        return arrow::Status::Invalid("unexpected OperatorStatus::kHasOutput from Execute");
    }
    return arrow::Status::Invalid("unknown operator status");
  }
}

}  // namespace detail

struct PipelineStageSpec {
  std::string name;
  int num_tasks = 1;
  std::function<arrow::Result<std::unique_ptr<PipelineExec>>(int task_id)> task_factory;
};

class PipelineRunner {
 public:
  arrow::Result<std::size_t> AddStage(PipelineStageSpec spec) {
    if (spec.num_tasks <= 0) {
      return arrow::Status::Invalid("stage num_tasks must be positive");
    }
    if (!spec.task_factory) {
      return arrow::Status::Invalid("stage task_factory must not be empty");
    }
    stages_.push_back(std::move(spec));
    return stages_.size() - 1;
  }

  arrow::Status AddDependency(std::size_t upstream_stage_id, std::size_t downstream_stage_id) {
    if (upstream_stage_id >= stages_.size() || downstream_stage_id >= stages_.size()) {
      return arrow::Status::Invalid("stage id out of range");
    }
    if (upstream_stage_id == downstream_stage_id) {
      return arrow::Status::Invalid("dependency must not be a self-edge");
    }
    dependencies_.push_back({upstream_stage_id, downstream_stage_id});
    return arrow::Status::OK();
  }

  arrow::Status Run() {
    if (stages_.empty()) {
      return arrow::Status::Invalid("runner has no stages");
    }

    ARROW_ASSIGN_OR_RAISE(auto order, TopoOrder());
    for (const auto stage_id : order) {
      ARROW_RETURN_NOT_OK(RunStage(stage_id));
    }
    return arrow::Status::OK();
  }

 private:
  arrow::Result<std::vector<std::size_t>> TopoOrder() const {
    const std::size_t num_stages = stages_.size();
    std::vector<std::vector<std::size_t>> adj(num_stages);
    std::vector<std::size_t> indegree(num_stages, 0);

    for (const auto& dep : dependencies_) {
      const auto upstream = dep.first;
      const auto downstream = dep.second;
      if (upstream >= num_stages || downstream >= num_stages) {
        return arrow::Status::Invalid("dependency stage id out of range");
      }
      adj[upstream].push_back(downstream);
      ++indegree[downstream];
    }

    std::priority_queue<std::size_t, std::vector<std::size_t>, std::greater<>> ready;
    for (std::size_t i = 0; i < num_stages; ++i) {
      if (indegree[i] == 0) {
        ready.push(i);
      }
    }

    std::vector<std::size_t> order;
    order.reserve(num_stages);
    while (!ready.empty()) {
      const std::size_t stage = ready.top();
      ready.pop();
      order.push_back(stage);
      for (const auto downstream : adj[stage]) {
        if (--indegree[downstream] == 0) {
          ready.push(downstream);
        }
      }
    }

    if (order.size() != num_stages) {
      return arrow::Status::Invalid("runner stage dependency graph has a cycle");
    }
    return order;
  }

  arrow::Status RunStage(std::size_t stage_id) {
    if (stage_id >= stages_.size()) {
      return arrow::Status::Invalid("stage id out of range");
    }
    const auto& stage = stages_[stage_id];

    std::vector<std::unique_ptr<PipelineExec>> tasks;
    tasks.reserve(static_cast<std::size_t>(stage.num_tasks));
    for (int i = 0; i < stage.num_tasks; ++i) {
      ARROW_ASSIGN_OR_RAISE(auto exec, stage.task_factory(i));
      if (exec == nullptr) {
        return arrow::Status::Invalid("stage task factory returned null exec");
      }
      tasks.push_back(std::move(exec));
    }

    std::mutex mu;
    arrow::Status first_error;
    std::atomic<bool> has_error{false};

    std::vector<std::thread> threads;
    threads.reserve(tasks.size());
    for (std::size_t i = 0; i < tasks.size(); ++i) {
      threads.emplace_back([&, i]() {
        auto st = detail::RunPipelineExecToCompletion(tasks[i].get());
        if (st.ok()) {
          return;
        }

        const bool already_failed = has_error.exchange(true);
        if (!already_failed) {
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
        return arrow::Status::Invalid("pipeline stage failed without an error status");
      }
      const std::string prefix = stage.name.empty() ? "pipeline stage failed: "
                                                    : ("pipeline stage '" + stage.name + "' failed: ");
      return first_error.WithMessage(prefix + first_error.message());
    }
    return arrow::Status::OK();
  }

  std::vector<PipelineStageSpec> stages_;
  std::vector<std::pair<std::size_t, std::size_t>> dependencies_;
};

}  // namespace tiforth::test
