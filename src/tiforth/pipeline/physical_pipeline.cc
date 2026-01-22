#include "tiforth/pipeline/physical_pipeline.h"

#include <algorithm>
#include <map>
#include <unordered_map>
#include <utility>

namespace tiforth::pipeline {

namespace {

class PipelineCompiler {
 public:
  explicit PipelineCompiler(const LogicalPipeline& logical_pipeline)
      : logical_pipeline_(logical_pipeline) {}

  PhysicalPipelines Compile(const PipelineContext& ctx) && {
    ExtractTopology(ctx);
    SortTopology();
    return BuildPhysicalPipelines();
  }

 private:
  void ExtractTopology(const PipelineContext& pipeline_context) {
    std::unordered_map<PipeOp*, SourceOp*> pipe_source_map;
    for (const auto& channel : logical_pipeline_.Channels()) {
      std::size_t next_stage_id = 0;
      topology_.emplace(channel.source_op,
                        std::pair<std::size_t, LogicalPipeline::Channel>{next_stage_id++,
                                                                         channel});
      sources_keep_order_.push_back(channel.source_op);

      for (std::size_t i = 0; i < channel.pipe_ops.size(); ++i) {
        auto* pipe = channel.pipe_ops[i];
        if (pipe_source_map.count(pipe) == 0) {
          auto implicit_source_up = pipe->ImplicitSource(pipeline_context);
          if (implicit_source_up != nullptr) {
            auto* implicit_source = implicit_source_up.get();
            pipe_source_map.emplace(pipe, implicit_source);

            LogicalPipeline::Channel new_channel{
                implicit_source, std::vector<PipeOp*>(channel.pipe_ops.begin() + i + 1,
                                                      channel.pipe_ops.end())};
            topology_.emplace(
                implicit_source,
                std::pair<std::size_t, LogicalPipeline::Channel>{next_stage_id++,
                                                                std::move(new_channel)});
            sources_keep_order_.push_back(implicit_source);
            implicit_sources_keepalive_.emplace(implicit_source, std::move(implicit_source_up));
          }
        } else {
          auto* implicit_source = pipe_source_map[pipe];
          auto& stage_id = topology_[implicit_source].first;
          if (stage_id < next_stage_id) {
            stage_id = next_stage_id++;
          }
        }
      }
    }
  }

  void SortTopology() {
    for (auto* source : sources_keep_order_) {
      auto& physical_info = topology_[source];
      if (auto it = implicit_sources_keepalive_.find(source);
          it != implicit_sources_keepalive_.end()) {
        physical_pipelines_[physical_info.first].first.push_back(std::move(it->second));
      }
      physical_pipelines_[physical_info.first].second.push_back(std::move(physical_info.second));
    }
  }

  PhysicalPipelines BuildPhysicalPipelines() {
    PhysicalPipelines physical_pipelines;
    for (auto& [stage_id, physical_info] : physical_pipelines_) {
      auto implicit_sources = std::move(physical_info.first);
      auto logical_channels = std::move(physical_info.second);

      std::vector<PhysicalPipeline::Channel> physical_channels(logical_channels.size());
      std::transform(
          logical_channels.begin(), logical_channels.end(), physical_channels.begin(),
          [&](auto& channel) -> PhysicalPipeline::Channel {
            return {channel.source_op, std::move(channel.pipe_ops), logical_pipeline_.SinkOp()};
          });

      auto name = "PhysicalPipeline" + std::to_string(stage_id) + "(" + logical_pipeline_.name() +
                  ")";
      physical_pipelines.emplace_back(std::move(name), std::move(physical_channels),
                                      std::move(implicit_sources));
    }
    return physical_pipelines;
  }

  const LogicalPipeline& logical_pipeline_;

  std::unordered_map<SourceOp*, std::pair<std::size_t, LogicalPipeline::Channel>> topology_;
  std::vector<SourceOp*> sources_keep_order_;
  std::unordered_map<SourceOp*, std::unique_ptr<SourceOp>> implicit_sources_keepalive_;
  std::map<std::size_t,
           std::pair<std::vector<std::unique_ptr<SourceOp>>, std::vector<LogicalPipeline::Channel>>>
      physical_pipelines_;
};

}  // namespace

PhysicalPipelines CompilePipeline(const PipelineContext& ctx, const LogicalPipeline& logical_pipeline) {
  return PipelineCompiler(logical_pipeline).Compile(ctx);
}

}  // namespace tiforth::pipeline

