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
#include <stdexcept>
#include <utility>
#include <vector>

#include <arrow/result.h>
#include <arrow/status.h>

#include <gtest/gtest.h>

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/IOThreadPoolExecutor.h>

#include <broken_pipeline/schedule/async_dual_pool_scheduler.h>
#include <broken_pipeline/schedule/naive_parallel_scheduler.h>
#include <broken_pipeline/schedule/parallel_coro_scheduler.h>
#include <broken_pipeline/schedule/sequential_coro_scheduler.h>

#include "tiforth/traits.h"

namespace tiforth::test {

constexpr std::size_t kTestCpuThreadPoolSize = 4;
constexpr std::size_t kTestIoThreadPoolSize = 2;

// Scheduler holders mirror broken_pipeline/tests/schedule/scheduler_test.cpp.
struct AsyncDualPoolSchedulerHolder {
  folly::CPUThreadPoolExecutor cpu_executor{kTestCpuThreadPoolSize};
  folly::IOThreadPoolExecutor io_executor{kTestIoThreadPoolSize};
  bp::schedule::AsyncDualPoolScheduler scheduler{&cpu_executor, &io_executor};
  static constexpr const char* kName = "AsyncDualPoolScheduler";
};

struct NaiveParallelSchedulerHolder {
  bp::schedule::NaiveParallelScheduler scheduler;
  static constexpr const char* kName = "NaiveParallelScheduler";
};

struct ParallelCoroSchedulerHolder {
  bp::schedule::ParallelCoroScheduler scheduler;
  static constexpr const char* kName = "ParallelCoroScheduler";
};

struct SequentialCoroSchedulerHolder {
  bp::schedule::SequentialCoroScheduler scheduler;
  static constexpr const char* kName = "SequentialCoroScheduler";
};

using SchedulerTypes =
    ::testing::Types<AsyncDualPoolSchedulerHolder, NaiveParallelSchedulerHolder,
                     ParallelCoroSchedulerHolder, SequentialCoroSchedulerHolder>;

using CallbackSchedulerTypes =
    ::testing::Types<AsyncDualPoolSchedulerHolder, NaiveParallelSchedulerHolder>;

class SchedulerRef {
 public:
  virtual ~SchedulerRef() = default;
  virtual const char* name() const = 0;
  virtual TaskContext MakeTaskContext(const void* context) = 0;
  virtual arrow::Result<TaskStatus> ScheduleAndWait(const TaskGroup& group,
                                                    TaskContext task_ctx) = 0;
};

template <typename SchedulerHolder>
class SchedulerBinding final : public SchedulerRef {
 public:
  explicit SchedulerBinding(SchedulerHolder* holder) : holder_(holder) {}

  const char* name() const override { return SchedulerHolder::kName; }

  TaskContext MakeTaskContext(const void* context) override {
    return holder_->scheduler.MakeTaskContext(context);
  }

  arrow::Result<TaskStatus> ScheduleAndWait(const TaskGroup& group,
                                            TaskContext task_ctx) override {
    auto handle = holder_->scheduler.ScheduleTaskGroup(group, std::move(task_ctx));
    return holder_->scheduler.WaitTaskGroup(handle);
  }

 private:
  SchedulerHolder* holder_;
};

inline SchedulerRef* g_current_scheduler = nullptr;

inline void SetCurrentScheduler(SchedulerRef* scheduler) { g_current_scheduler = scheduler; }

inline SchedulerRef& RequireScheduler() {
  if (g_current_scheduler == nullptr) {
    throw std::runtime_error(
        "test scheduler is not set (use TIFORTH_SCHEDULER_TEST_SUITE)");
  }
  return *g_current_scheduler;
}

template <typename SchedulerHolder>
class SchedulerTest : public ::testing::Test {
 protected:
  SchedulerHolder holder_;
  SchedulerBinding<SchedulerHolder> binding_{&holder_};

  void SetUp() override { SetCurrentScheduler(&binding_); }
  void TearDown() override { SetCurrentScheduler(nullptr); }

  const char* SchedulerName() const { return binding_.name(); }
};

#ifndef TIFORTH_SCHEDULER_TEST_SUITE
#define TIFORTH_SCHEDULER_TEST_SUITE(SuiteName)                            \
  template <typename SchedulerHolder>                                      \
  class SuiteName : public ::tiforth::test::SchedulerTest<SchedulerHolder> \
  {};                                                                      \
  TYPED_TEST_SUITE(SuiteName, ::tiforth::test::SchedulerTypes)
#endif

#ifndef TIFORTH_CALLBACK_SCHEDULER_TEST_SUITE
#define TIFORTH_CALLBACK_SCHEDULER_TEST_SUITE(SuiteName)                   \
  template <typename SchedulerHolder>                                      \
  class SuiteName : public ::tiforth::test::SchedulerTest<SchedulerHolder> \
  {};                                                                      \
  TYPED_TEST_SUITE(SuiteName, ::tiforth::test::CallbackSchedulerTypes)
#endif

#ifndef TIFORTH_SCHEDULER_TEST
#define TIFORTH_SCHEDULER_TEST(SuiteName, TestName) TYPED_TEST(SuiteName, TestName)
#endif

inline TaskContext MakeTestTaskContext(const void* context = nullptr) {
  return RequireScheduler().MakeTaskContext(context);
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

inline arrow::Status RunTaskGroupToCompletion(const TaskGroup& group,
                                              const TaskContext& task_ctx) {
  if (group.NumTasks() == 0) {
    return arrow::Status::Invalid("task group num_tasks must be positive");
  }

  auto& scheduler = RequireScheduler();
  auto result = scheduler.ScheduleAndWait(group, task_ctx);
  if (!result.ok()) {
    return result.status().WithMessage("scheduler ", scheduler.name(), ": ",
                                       result.status().message());
  }
  const auto status = *result;

  if (status.IsFinished()) {
    return arrow::Status::OK();
  }
  if (status.IsCancelled()) {
    return arrow::Status::Cancelled("task group cancelled");
  }
  if (status.IsContinue() || status.IsYield() || status.IsBlocked()) {
    return arrow::Status::Invalid("scheduler ", scheduler.name(),
                                  ": task group did not finish");
  }
  return arrow::Status::Invalid("scheduler ", scheduler.name(),
                                ": unknown task group status");
}

inline arrow::Status RunTaskGroupsToCompletion(const TaskGroups& groups,
                                               const TaskContext& task_ctx) {
  for (const auto& group : groups) {
    ARROW_RETURN_NOT_OK(RunTaskGroupToCompletion(group, task_ctx));
  }
  return arrow::Status::OK();
}

}  // namespace tiforth::test
