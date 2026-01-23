#include "tiforth/stash/plan.h"

#include <queue>
#include <utility>

#include <arrow/status.h>
#include <arrow/util/logging.h>

namespace tiforth {

arrow::Result<std::unique_ptr<PlanBuilder>> PlanBuilder::Create(const Engine* engine) {
  if (engine == nullptr) {
    return arrow::Status::Invalid("engine must not be null");
  }
  return std::unique_ptr<PlanBuilder>(new PlanBuilder(engine));
}

PlanBuilder::PlanBuilder(const Engine* engine) : engine_(engine) {}

PlanBuilder::~PlanBuilder() = default;

arrow::Result<std::size_t> PlanBuilder::AddStage() {
  stages_.push_back(PlanStage{});
  return stages_.size() - 1;
}

arrow::Status PlanBuilder::SetStageSourceTaskInput(std::size_t stage_id) {
  if (stage_id >= stages_.size()) {
    return arrow::Status::Invalid("stage_id out of range");
  }
  auto& stage = stages_[stage_id];
  stage.source_kind = PlanStageSourceKind::kTaskInput;
  stage.source_factory = {};
  return arrow::Status::OK();
}

arrow::Status PlanBuilder::SetStageSource(std::size_t stage_id, PlanSourceFactory factory) {
  if (stage_id >= stages_.size()) {
    return arrow::Status::Invalid("stage_id out of range");
  }
  if (!factory) {
    return arrow::Status::Invalid("source factory must not be empty");
  }
  auto& stage = stages_[stage_id];
  stage.source_kind = PlanStageSourceKind::kCustom;
  stage.source_factory = std::move(factory);
  return arrow::Status::OK();
}

arrow::Status PlanBuilder::AppendTransform(std::size_t stage_id, PlanTransformFactory factory) {
  if (stage_id >= stages_.size()) {
    return arrow::Status::Invalid("stage_id out of range");
  }
  if (!factory) {
    return arrow::Status::Invalid("transform factory must not be empty");
  }
  stages_[stage_id].transform_factories.push_back(std::move(factory));
  return arrow::Status::OK();
}

arrow::Status PlanBuilder::SetStageSinkTaskOutput(std::size_t stage_id) {
  if (stage_id >= stages_.size()) {
    return arrow::Status::Invalid("stage_id out of range");
  }
  auto& stage = stages_[stage_id];
  stage.sink_kind = PlanStageSinkKind::kTaskOutput;
  stage.sink_factory = {};
  return arrow::Status::OK();
}

arrow::Status PlanBuilder::SetStageSink(std::size_t stage_id, PlanSinkFactory factory) {
  if (stage_id >= stages_.size()) {
    return arrow::Status::Invalid("stage_id out of range");
  }
  if (!factory) {
    return arrow::Status::Invalid("sink factory must not be empty");
  }
  auto& stage = stages_[stage_id];
  stage.sink_kind = PlanStageSinkKind::kCustom;
  stage.sink_factory = std::move(factory);
  return arrow::Status::OK();
}

arrow::Status PlanBuilder::AddDependency(std::size_t upstream_stage_id, std::size_t downstream_stage_id) {
  if (upstream_stage_id >= stages_.size() || downstream_stage_id >= stages_.size()) {
    return arrow::Status::Invalid("dependency stage id out of range");
  }
  if (upstream_stage_id == downstream_stage_id) {
    return arrow::Status::Invalid("dependency must not be a self-edge");
  }
  dependencies_.push_back({upstream_stage_id, downstream_stage_id});
  return arrow::Status::OK();
}

arrow::Result<std::unique_ptr<Plan>> PlanBuilder::Finalize() {
  if (engine_ == nullptr) {
    return arrow::Status::Invalid("engine must not be null");
  }
  if (stages_.empty()) {
    return arrow::Status::Invalid("plan must have at least one stage");
  }

  std::size_t task_input_sources = 0;
  std::size_t task_output_sinks = 0;

  for (const auto& stage : stages_) {
    if (stage.source_kind == PlanStageSourceKind::kCustom && !stage.source_factory) {
      return arrow::Status::Invalid("custom stage source factory must not be empty");
    }
    if (stage.sink_kind == PlanStageSinkKind::kCustom && !stage.sink_factory) {
      return arrow::Status::Invalid("custom stage sink factory must not be empty");
    }
    for (const auto& transform_factory : stage.transform_factories) {
      if (!transform_factory) {
        return arrow::Status::Invalid("transform factory must not be empty");
      }
    }
    if (stage.source_kind == PlanStageSourceKind::kTaskInput) {
      ++task_input_sources;
    }
    if (stage.sink_kind == PlanStageSinkKind::kTaskOutput) {
      ++task_output_sinks;
    }
  }

  if (task_input_sources > 1) {
    return arrow::Status::Invalid("plan must not have more than one task-input source stage");
  }
  if (task_output_sinks > 1) {
    return arrow::Status::Invalid("plan must not have more than one task-output sink stage");
  }

  const std::size_t num_stages = stages_.size();
  std::vector<std::vector<std::size_t>> adj(num_stages);
  std::vector<std::size_t> indegree(num_stages, 0);
  for (const auto& dep : dependencies_) {
    const auto upstream = dep.first;
    const auto downstream = dep.second;
    ARROW_DCHECK(upstream < num_stages);
    ARROW_DCHECK(downstream < num_stages);
    adj[upstream].push_back(downstream);
    ++indegree[downstream];
  }

  std::priority_queue<std::size_t, std::vector<std::size_t>, std::greater<>> ready;
  for (std::size_t i = 0; i < num_stages; ++i) {
    if (indegree[i] == 0) {
      ready.push(i);
    }
  }

  std::vector<std::size_t> stage_order;
  stage_order.reserve(num_stages);
  while (!ready.empty()) {
    const std::size_t stage = ready.top();
    ready.pop();
    stage_order.push_back(stage);
    for (const auto downstream : adj[stage]) {
      ARROW_DCHECK(downstream < num_stages);
      if (--indegree[downstream] == 0) {
        ready.push(downstream);
      }
    }
  }
  if (stage_order.size() != num_stages) {
    return arrow::Status::Invalid("plan has a cycle in stage dependencies");
  }

  return std::unique_ptr<Plan>(new Plan(engine_, std::move(breaker_state_factories_), std::move(stages_),
                                        std::move(stage_order)));
}

Plan::Plan(const Engine* engine, std::vector<BreakerStateFactory> breaker_state_factories,
           std::vector<PlanStage> stages, std::vector<std::size_t> stage_order)
    : engine_(engine),
      breaker_state_factories_(std::move(breaker_state_factories)),
      stages_(std::move(stages)),
      stage_order_(std::move(stage_order)) {}

arrow::Result<std::unique_ptr<Task>> Plan::CreateTask() const {
  if (engine_ == nullptr) {
    return arrow::Status::Invalid("engine must not be null");
  }
  auto task = std::unique_ptr<Task>(new Task());
  ARROW_RETURN_NOT_OK(task->InitPlan(*this));
  return task;
}

}  // namespace tiforth
