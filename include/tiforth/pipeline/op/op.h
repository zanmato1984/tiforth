#pragma once

#include <functional>
#include <memory>
#include <optional>

#include "tiforth/pipeline/op/op_output.h"
#include "tiforth/pipeline/pipeline_context.h"
#include "tiforth/task/task_context.h"
#include "tiforth/task/task_group.h"

namespace tiforth::pipeline {

class SourceOp;
class PipeOp;
class SinkOp;

using PipelineSource =
    std::function<OpResult(const PipelineContext&, const task::TaskContext&, ThreadId)>;
using PipelinePipe = std::function<OpResult(const PipelineContext&, const task::TaskContext&,
                                           ThreadId, std::optional<Batch>)>;
using PipelineDrain =
    std::function<OpResult(const PipelineContext&, const task::TaskContext&, ThreadId)>;
using PipelineSink = std::function<OpResult(const PipelineContext&, const task::TaskContext&,
                                           ThreadId, std::optional<Batch>)>;

class SourceOp {
 public:
  virtual ~SourceOp() = default;
  virtual PipelineSource Source(const PipelineContext&) = 0;
  virtual task::TaskGroups Frontend(const PipelineContext&) { return {}; }
  virtual std::optional<task::TaskGroup> Backend(const PipelineContext&) { return std::nullopt; }
};

class PipeOp {
 public:
  virtual ~PipeOp() = default;
  virtual PipelinePipe Pipe(const PipelineContext&) = 0;
  virtual PipelineDrain Drain(const PipelineContext&) = 0;
  virtual std::unique_ptr<SourceOp> ImplicitSource(const PipelineContext&) { return nullptr; }
};

class SinkOp {
 public:
  virtual ~SinkOp() = default;
  virtual PipelineSink Sink(const PipelineContext&) = 0;
  virtual task::TaskGroups Frontend(const PipelineContext&) { return {}; }
  virtual std::optional<task::TaskGroup> Backend(const PipelineContext&) { return std::nullopt; }
  virtual std::unique_ptr<SourceOp> ImplicitSource(const PipelineContext&) { return nullptr; }
};

}  // namespace tiforth::pipeline

