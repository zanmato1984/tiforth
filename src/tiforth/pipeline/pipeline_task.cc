#include "tiforth/pipeline/pipeline_task.h"

#include <algorithm>
#include <utility>

#include <arrow/status.h>

namespace tiforth::pipeline {

PipelineTask::Channel::Channel(const PipelineContext& pipeline_ctx, const PipelineTask& task,
                               std::size_t channel_id, std::size_t dop)
    : pipeline_ctx_(pipeline_ctx),
      task_(task),
      channel_id_(channel_id),
      dop_(dop),
      source_(task_.pipeline_.Channels()[channel_id].source_op->Source(pipeline_ctx)),
      pipes_(task_.pipeline_.Channels()[channel_id].pipe_ops.size()),
      sink_(task_.pipeline_.Channels()[channel_id].sink_op->Sink(pipeline_ctx)),
      thread_locals_(dop) {
  const auto& pipe_ops = task_.pipeline_.Channels()[channel_id].pipe_ops;
  std::transform(pipe_ops.begin(), pipe_ops.end(), pipes_.begin(), [&](auto* pipe_op) {
    return std::make_pair(pipe_op->Pipe(pipeline_ctx), pipe_op->Drain(pipeline_ctx));
  });

  std::vector<std::size_t> drains;
  drains.reserve(pipes_.size());
  for (std::size_t i = 0; i < pipes_.size(); ++i) {
    if (pipes_[i].second) {
      drains.push_back(i);
    }
  }
  for (std::size_t i = 0; i < dop; ++i) {
    thread_locals_[i].drains = drains;
  }
}

OpResult PipelineTask::Channel::operator()(const PipelineContext& pipeline_ctx,
                                           const task::TaskContext& task_ctx,
                                           ThreadId thread_id) {
  if (cancelled_.load(std::memory_order_relaxed)) {
    return OpOutput::Cancelled();
  }

  if (thread_locals_[thread_id].sinking) {
    thread_locals_[thread_id].sinking = false;
    return Sink(pipeline_ctx, task_ctx, thread_id, std::nullopt);
  }

  if (!thread_locals_[thread_id].pipe_stack.empty()) {
    const auto pipe_id = thread_locals_[thread_id].pipe_stack.top();
    thread_locals_[thread_id].pipe_stack.pop();
    return Pipe(pipeline_ctx, task_ctx, thread_id, pipe_id, std::nullopt);
  }

  if (!thread_locals_[thread_id].source_done) {
    auto result = source_(pipeline_ctx, task_ctx, thread_id);
    if (!result.ok()) {
      cancelled_.store(true, std::memory_order_relaxed);
      return result.status();
    }

    if (result->IsBlocked()) {
      return result;
    }

    if (result->IsFinished()) {
      thread_locals_[thread_id].source_done = true;
      if (result->GetBatch().has_value()) {
        return Pipe(pipeline_ctx, task_ctx, thread_id, /*pipe_id=*/0,
                    std::move(result->GetBatch()));
      }
    } else {
      ARROW_CHECK(result->IsSourcePipeHasMore());
      ARROW_CHECK(result->GetBatch().has_value());
      return Pipe(pipeline_ctx, task_ctx, thread_id, /*pipe_id=*/0, std::move(result->GetBatch()));
    }
  }

  if (thread_locals_[thread_id].draining >= thread_locals_[thread_id].drains.size()) {
    return OpOutput::Finished();
  }

  for (; thread_locals_[thread_id].draining < thread_locals_[thread_id].drains.size();
       ++thread_locals_[thread_id].draining) {
    const auto drain_id = thread_locals_[thread_id].drains[thread_locals_[thread_id].draining];
    ARROW_CHECK(drain_id < pipes_.size());
    if (!pipes_[drain_id].second) {
      continue;
    }

    auto result = pipes_[drain_id].second(pipeline_ctx, task_ctx, thread_id);
    if (!result.ok()) {
      cancelled_.store(true, std::memory_order_relaxed);
      return result.status();
    }

    if (thread_locals_[thread_id].yield) {
      ARROW_CHECK(result->IsPipeYieldBack());
      thread_locals_[thread_id].yield = false;
      return OpOutput::PipeYieldBack();
    }

    if (result->IsPipeYield()) {
      ARROW_CHECK(!thread_locals_[thread_id].yield);
      thread_locals_[thread_id].yield = true;
      return OpOutput::PipeYield();
    }

    if (result->IsBlocked()) {
      return result;
    }

    ARROW_CHECK(result->IsSourcePipeHasMore() || result->IsFinished());
    if (result->GetBatch().has_value()) {
      if (result->IsFinished()) {
        ++thread_locals_[thread_id].draining;
      }
      return Pipe(pipeline_ctx, task_ctx, thread_id, drain_id + 1, std::move(result->GetBatch()));
    }
  }

  return OpOutput::Finished();
}

OpResult PipelineTask::Channel::Pipe(const PipelineContext& pipeline_ctx,
                                     const task::TaskContext& task_ctx, ThreadId thread_id,
                                     std::size_t pipe_id, std::optional<Batch> input) {
  for (std::size_t i = pipe_id; i < pipes_.size(); ++i) {
    auto result = pipes_[i].first(pipeline_ctx, task_ctx, thread_id, std::move(input));
    if (!result.ok()) {
      cancelled_.store(true, std::memory_order_relaxed);
      return result.status();
    }

    if (thread_locals_[thread_id].yield) {
      ARROW_CHECK(result->IsPipeYieldBack());
      thread_locals_[thread_id].pipe_stack.push(i);
      thread_locals_[thread_id].yield = false;
      return OpOutput::PipeYieldBack();
    }

    if (result->IsPipeYield()) {
      ARROW_CHECK(!thread_locals_[thread_id].yield);
      thread_locals_[thread_id].pipe_stack.push(i);
      thread_locals_[thread_id].yield = true;
      return OpOutput::PipeYield();
    }

    if (result->IsBlocked()) {
      thread_locals_[thread_id].pipe_stack.push(i);
      return result;
    }

    ARROW_CHECK(result->IsPipeSinkNeedsMore() || result->IsPipeEven() ||
                result->IsSourcePipeHasMore());
    if (result->IsPipeEven() || result->IsSourcePipeHasMore()) {
      if (result->IsSourcePipeHasMore()) {
        thread_locals_[thread_id].pipe_stack.push(i);
      }
      ARROW_CHECK(result->GetBatch().has_value());
      input = std::move(result->GetBatch());
    } else {
      return OpOutput::PipeSinkNeedsMore();
    }
  }

  return Sink(pipeline_ctx, task_ctx, thread_id, std::move(input));
}

OpResult PipelineTask::Channel::Sink(const PipelineContext& pipeline_ctx,
                                     const task::TaskContext& task_ctx, ThreadId thread_id,
                                     std::optional<Batch> input) {
  auto result = sink_(pipeline_ctx, task_ctx, thread_id, std::move(input));
  if (!result.ok()) {
    cancelled_.store(true, std::memory_order_relaxed);
    return result.status();
  }
  ARROW_CHECK(result->IsPipeSinkNeedsMore() || result->IsBlocked());
  if (result->IsBlocked()) {
    thread_locals_[thread_id].sinking = true;
  }
  return result;
}

PipelineTask::PipelineTask(const PipelineContext& pipeline_ctx, const PhysicalPipeline& pipeline,
                           std::size_t dop)
    : pipeline_(pipeline) {
  for (std::size_t i = 0; i < pipeline_.Channels().size(); ++i) {
    channels_.emplace_back(pipeline_ctx, *this, i, dop);
  }
  for (std::size_t i = 0; i < dop; ++i) {
    thread_locals_.emplace_back(channels_.size());
  }
}

task::TaskResult PipelineTask::operator()(const PipelineContext& pipeline_ctx,
                                          const task::TaskContext& task_ctx, ThreadId thread_id) {
  bool all_finished = true;
  bool all_unfinished_blocked = true;
  task::Resumers resumers;

  OpOutput last_op_output = OpOutput::PipeSinkNeedsMore();

  for (std::size_t i = 0; i < channels_.size(); ++i) {
    if (thread_locals_[thread_id].finished[i]) {
      continue;
    }

    if (auto& resumer = thread_locals_[thread_id].resumers[i]; resumer != nullptr) {
      if (resumer->IsResumed()) {
        resumer = nullptr;
      } else {
        resumers.push_back(resumer);
        all_finished = false;
        continue;
      }
    }

    auto& channel = channels_[i];
    ARROW_ASSIGN_OR_RAISE(last_op_output, channel(pipeline_ctx, task_ctx, thread_id));

    if (last_op_output.IsFinished()) {
      ARROW_CHECK(!last_op_output.GetBatch().has_value());
      thread_locals_[thread_id].finished[i] = true;
    } else {
      all_finished = false;
    }

    if (last_op_output.IsBlocked()) {
      thread_locals_[thread_id].resumers[i] = last_op_output.GetResumer();
      resumers.push_back(std::move(last_op_output.GetResumer()));
    } else {
      all_unfinished_blocked = false;
    }

    if (!last_op_output.IsFinished() && !last_op_output.IsBlocked()) {
      break;
    }
  }

  if (all_finished) {
    return task::TaskStatus::Finished();
  }

  if (all_unfinished_blocked && !resumers.empty()) {
    if (!task_ctx.any_awaiter_factory) {
      return arrow::Status::Invalid("any_awaiter_factory must not be empty");
    }
    ARROW_ASSIGN_OR_RAISE(auto awaiter, task_ctx.any_awaiter_factory(std::move(resumers)));
    return task::TaskStatus::Blocked(std::move(awaiter));
  }

  if (last_op_output.IsPipeYield()) {
    return task::TaskStatus::Yield();
  }
  if (last_op_output.IsCancelled()) {
    return task::TaskStatus::Cancelled();
  }
  return task::TaskStatus::Continue();
}

}  // namespace tiforth::pipeline

