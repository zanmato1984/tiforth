#pragma once

#include <cstddef>
#include <functional>
#include <memory>
#include <utility>
#include <vector>

#include <arrow/result.h>
#include <arrow/status.h>

#include "tiforth/engine.h"
#include "tiforth/stash/operators.h"
#include "tiforth/task.h"

namespace tiforth {

struct BreakerStateId {
  std::size_t value = 0;
};

class PlanTaskContext {
 public:
  PlanTaskContext() = default;

  PlanTaskContext(const PlanTaskContext&) = delete;
  PlanTaskContext& operator=(const PlanTaskContext&) = delete;

  template <typename T>
  arrow::Result<std::shared_ptr<T>> GetBreakerState(BreakerStateId id) const {
    if (id.value >= breaker_states_.size()) {
      return arrow::Status::Invalid("breaker state id out of range");
    }
    const auto& state = breaker_states_[id.value];
    if (state == nullptr) {
      return arrow::Status::Invalid("breaker state is null");
    }
    return std::static_pointer_cast<T>(state);
  }

 private:
  std::vector<std::shared_ptr<void>> breaker_states_;

  friend class Plan;
  friend class Task;
};

using BreakerStateFactory = std::function<arrow::Result<std::shared_ptr<void>>()>;
using PlanSourceFactory = std::function<arrow::Result<SourceOpPtr>(PlanTaskContext*)>;
using PlanTransformFactory = std::function<arrow::Result<TransformOpPtr>(PlanTaskContext*)>;
using PlanSinkFactory = std::function<arrow::Result<SinkOpPtr>(PlanTaskContext*)>;

enum class PlanStageSourceKind {
  kTaskInput,
  kCustom,
};

enum class PlanStageSinkKind {
  kTaskOutput,
  kCustom,
};

struct PlanStage {
  PlanStageSourceKind source_kind = PlanStageSourceKind::kTaskInput;
  PlanSourceFactory source_factory;

  std::vector<PlanTransformFactory> transform_factories;

  PlanStageSinkKind sink_kind = PlanStageSinkKind::kTaskOutput;
  PlanSinkFactory sink_factory;
};

class Plan;

class PlanBuilder {
 public:
  static arrow::Result<std::unique_ptr<PlanBuilder>> Create(const Engine* engine);

  PlanBuilder(const PlanBuilder&) = delete;
  PlanBuilder& operator=(const PlanBuilder&) = delete;

  ~PlanBuilder();

  template <typename T>
  arrow::Result<BreakerStateId> AddBreakerState(
      std::function<arrow::Result<std::shared_ptr<T>>()> factory) {
    if (!factory) {
      return arrow::Status::Invalid("breaker state factory must not be empty");
    }
    BreakerStateId id{breaker_state_factories_.size()};
    breaker_state_factories_.push_back([factory]() -> arrow::Result<std::shared_ptr<void>> {
      ARROW_ASSIGN_OR_RAISE(auto state, factory());
      if (state == nullptr) {
        return arrow::Status::Invalid("breaker state factory returned null");
      }
      return std::static_pointer_cast<void>(std::move(state));
    });
    return id;
  }

  arrow::Result<std::size_t> AddStage();

  arrow::Status SetStageSourceTaskInput(std::size_t stage_id);
  arrow::Status SetStageSource(std::size_t stage_id, PlanSourceFactory factory);

  arrow::Status AppendTransform(std::size_t stage_id, PlanTransformFactory factory);

  arrow::Status SetStageSinkTaskOutput(std::size_t stage_id);
  arrow::Status SetStageSink(std::size_t stage_id, PlanSinkFactory factory);

  arrow::Status AddDependency(std::size_t upstream_stage_id, std::size_t downstream_stage_id);

  arrow::Result<std::unique_ptr<Plan>> Finalize();

 private:
  explicit PlanBuilder(const Engine* engine);

  const Engine* engine_;
  std::vector<BreakerStateFactory> breaker_state_factories_;
  std::vector<PlanStage> stages_;
  std::vector<std::pair<std::size_t, std::size_t>> dependencies_;
};

class Plan {
 public:
  Plan(const Plan&) = delete;
  Plan& operator=(const Plan&) = delete;

  arrow::Result<std::unique_ptr<Task>> CreateTask() const;

 private:
  friend class PlanBuilder;
  friend class Task;

  Plan(const Engine* engine, std::vector<BreakerStateFactory> breaker_state_factories,
       std::vector<PlanStage> stages, std::vector<std::size_t> stage_order);

  const Engine* engine_;
  std::vector<BreakerStateFactory> breaker_state_factories_;
  std::vector<PlanStage> stages_;
  std::vector<std::size_t> stage_order_;
};

}  // namespace tiforth
