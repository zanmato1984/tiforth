#pragma once

#include <string>
#include <utility>
#include <vector>

#include "tiforth/pipeline/op/op.h"

namespace tiforth::pipeline {

class LogicalPipeline {
 public:
  struct Channel {
    SourceOp* source_op = nullptr;
    std::vector<PipeOp*> pipe_ops;
  };

  LogicalPipeline(std::string name, std::vector<Channel> channels, SinkOp* sink_op)
      : name_(std::move(name)), channels_(std::move(channels)), sink_op_(sink_op) {}

  const std::string& name() const { return name_; }

  const std::vector<Channel>& Channels() const { return channels_; }

  SinkOp* SinkOp() const { return sink_op_; }

 private:
  std::string name_;
  std::vector<Channel> channels_;
  class SinkOp* sink_op_ = nullptr;
};

}  // namespace tiforth::pipeline

