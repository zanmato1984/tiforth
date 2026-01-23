#pragma once

#include <arrow/result.h>
#include <arrow/util/logging.h>

#include <atomic>
#include <optional>
#include <stack>
#include <utility>
#include <vector>

#include "tiforth/pipeline/physical_pipeline.h"
#include "tiforth/task/defines.h"
#include "tiforth/task/task_context.h"
#include "tiforth/task/task_status.h"

namespace tiforth::pipeline {

class PipelineTask {
 public:
  class Channel {
   public:
    Channel(const PipelineContext&, const PipelineTask&, std::size_t channel_id, std::size_t dop);

    Channel(Channel&& other)
        : Channel(other.pipeline_ctx_, other.task_, other.channel_id_, other.dop_) {}

    OpResult operator()(const PipelineContext&, const task::TaskContext&, ThreadId);

   private:
    OpResult Pipe(const PipelineContext&, const task::TaskContext&, ThreadId, std::size_t pipe_id,
                  std::optional<Batch>);
    OpResult Sink(const PipelineContext&, const task::TaskContext&, ThreadId, std::optional<Batch>);

    const PipelineContext& pipeline_ctx_;
    const PipelineTask& task_;
    const std::size_t channel_id_;
    const std::size_t dop_;

    PipelineSource source_;
    std::vector<std::pair<PipelinePipe, PipelineDrain>> pipes_;
    PipelineSink sink_;

    struct ThreadLocal {
      bool sinking = false;
      std::stack<std::size_t> pipe_stack;
      bool source_done = false;
      std::vector<std::size_t> drains;
      std::size_t draining = 0;
      bool yield = false;
    };

    std::vector<ThreadLocal> thread_locals_;
    std::atomic_bool cancelled_{false};
  };

  PipelineTask(const PipelineContext&, const PhysicalPipeline&, std::size_t dop);

  task::TaskResult operator()(const PipelineContext&, const task::TaskContext&, ThreadId);

  const PhysicalPipeline& Pipeline() const { return pipeline_; }
  const std::vector<Channel>& Channels() const { return channels_; }

 private:
  const PhysicalPipeline& pipeline_;
  std::vector<Channel> channels_;

  struct ThreadLocal {
    explicit ThreadLocal(std::size_t num_channels)
        : finished(num_channels, false), resumers(num_channels, nullptr) {}

    std::vector<bool> finished;
    task::Resumers resumers;
  };

  std::vector<ThreadLocal> thread_locals_;
};

}  // namespace tiforth::pipeline

