#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "tiforth/pipeline/logical_pipeline.h"

namespace tiforth::pipeline {

class PhysicalPipeline {
 public:
  struct Channel {
    SourceOp* source_op = nullptr;
    std::vector<PipeOp*> pipe_ops;
    SinkOp* sink_op = nullptr;
  };

  PhysicalPipeline(std::string name, std::vector<Channel> channels,
                   std::vector<std::unique_ptr<SourceOp>> implicit_sources)
      : name_(std::move(name)),
        channels_(std::move(channels)),
        implicit_sources_(std::move(implicit_sources)) {}

  const std::string& name() const { return name_; }

  const std::vector<Channel>& Channels() const { return channels_; }

  const std::vector<std::unique_ptr<SourceOp>>& ImplicitSources() const {
    return implicit_sources_;
  }

 private:
  std::string name_;
  std::vector<Channel> channels_;
  std::vector<std::unique_ptr<SourceOp>> implicit_sources_;
};

using PhysicalPipelines = std::vector<PhysicalPipeline>;

PhysicalPipelines CompilePipeline(const PipelineContext&, const LogicalPipeline&);

}  // namespace tiforth::pipeline

