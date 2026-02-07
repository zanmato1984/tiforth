// Copyright 2026 TiForth Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstddef>
#include <memory>
#include <utility>
#include <vector>

#include <arrow/result.h>
#include <arrow/status.h>

#include <broken_pipeline/schedule/naive_parallel_scheduler.h>

#include "tiforth/traits.h"

namespace tiforth::test {

inline bp::schedule::NaiveParallelScheduler& TestScheduler() {
  static bp::schedule::NaiveParallelScheduler scheduler;
  return scheduler;
}

inline TaskContext MakeTestTaskContext(const void* context = nullptr) {
  return TestScheduler().MakeTaskContext(context);
}

inline arrow::Status ValidatePipelineForTests(const Pipeline& pipeline) {
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

inline arrow::Result<TaskGroups> CompileToTaskGroups(const Pipeline& pipeline, std::size_t dop) {
  if (dop == 0) {
    return arrow::Status::Invalid("dop must be positive");
  }
  ARROW_RETURN_NOT_OK(ValidatePipelineForTests(pipeline));
  auto exec = bp::Compile(pipeline, dop);

  TaskGroups groups;
  groups.reserve(exec.Pipelinexes().size() + 8);

  if (exec.Sink().backend.has_value()) {
    groups.push_back(*exec.Sink().backend);
  }

  for (const auto& seg : exec.Pipelinexes()) {
    std::vector<SourceOp*> sources;
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

inline arrow::Status RunTaskGroupToCompletion(const TaskGroup& group, const TaskContext& task_ctx) {
  if (group.NumTasks() == 0) {
    return arrow::Status::Invalid("task group num_tasks must be positive");
  }

  auto handle = TestScheduler().ScheduleTaskGroup(group, task_ctx);
  ARROW_ASSIGN_OR_RAISE(auto status, TestScheduler().WaitTaskGroup(handle));

  if (status.IsFinished()) {
    return arrow::Status::OK();
  }
  if (status.IsCancelled()) {
    return arrow::Status::Cancelled("task group cancelled");
  }
  if (status.IsContinue() || status.IsYield() || status.IsBlocked()) {
    return arrow::Status::Invalid("task group did not finish");
  }
  return arrow::Status::Invalid("unknown task group status");
}

inline arrow::Status RunTaskGroupsToCompletion(const TaskGroups& groups,
                                               const TaskContext& task_ctx) {
  for (const auto& group : groups) {
    ARROW_RETURN_NOT_OK(RunTaskGroupToCompletion(group, task_ctx));
  }
  return arrow::Status::OK();
}

}  // namespace tiforth::test
