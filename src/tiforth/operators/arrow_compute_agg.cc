#include "tiforth/operators/arrow_compute_agg.h"

#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <arrow/array/util.h>
#include <arrow/acero/exec_plan.h>
#include <arrow/acero/options.h>
#include <arrow/compute/api_aggregate.h>
#include <arrow/compute/expression.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>
#include <arrow/util/async_generator.h>
#include <arrow/util/logging.h>

#include "tiforth/compiled_expr.h"
#include "tiforth/engine.h"

namespace tiforth {

struct ArrowComputeAggTransformOp::ExecState {
  ExecState() : input_producer(input_gen.producer()) {}

  arrow::PushGenerator<std::optional<arrow::compute::ExecBatch>> input_gen;
  arrow::PushGenerator<std::optional<arrow::compute::ExecBatch>>::Producer input_producer;
  std::unique_ptr<arrow::RecordBatchReader> output_reader;
  std::shared_ptr<arrow::Schema> output_schema;
  bool input_closed = false;
  bool output_started = false;
  bool output_exhausted = false;
};

namespace {

arrow::Result<std::string> ToHashAggFunctionName(std::string_view func) {
  if (func == "count_all" || func == "hash_count_all") {
    return std::string("hash_count_all");
  }
  if (func == "count" || func == "hash_count") {
    return std::string("hash_count");
  }
  if (func == "sum" || func == "hash_sum") {
    return std::string("hash_sum");
  }
  if (func == "mean" || func == "avg" || func == "hash_mean") {
    return std::string("hash_mean");
  }
  if (func == "min" || func == "hash_min") {
    return std::string("hash_min");
  }
  if (func == "max" || func == "hash_max") {
    return std::string("hash_max");
  }
  return arrow::Status::NotImplemented("arrow compute grouped agg function is not supported: ",
                                       func);
}

arrow::Result<std::shared_ptr<arrow::Schema>> RenameKeyFields(
    const std::shared_ptr<arrow::Schema>& schema, const std::vector<AggKey>& keys) {
  if (schema == nullptr) {
    return arrow::Status::Invalid("schema must not be null");
  }
  if (static_cast<std::size_t>(schema->num_fields()) < keys.size()) {
    return arrow::Status::Invalid("schema is missing key fields");
  }

  auto fields = schema->fields();
  for (std::size_t i = 0; i < keys.size(); ++i) {
    if (fields[i] == nullptr) {
      return arrow::Status::Invalid("schema field must not be null");
    }
    fields[i] = fields[i]->WithName(keys[i].name);
  }
  return arrow::schema(std::move(fields), schema->metadata());
}

}  // namespace

ArrowComputeAggTransformOp::ArrowComputeAggTransformOp(const Engine* engine, std::vector<AggKey> keys,
                                                       std::vector<AggFunc> aggs,
                                                       arrow::MemoryPool* memory_pool)
    : engine_(engine),
      keys_(std::move(keys)),
      aggs_(std::move(aggs)),
      exec_context_(memory_pool != nullptr
                        ? memory_pool
                        : (engine != nullptr ? engine->memory_pool() : arrow::default_memory_pool()),
                    /*executor=*/nullptr,
                    engine != nullptr ? engine->function_registry() : nullptr) {}

ArrowComputeAggTransformOp::~ArrowComputeAggTransformOp() = default;

arrow::Result<std::shared_ptr<arrow::RecordBatch>> ArrowComputeAggTransformOp::NextOutputBatch() {
  if (engine_ == nullptr) {
    return arrow::Status::Invalid("engine must not be null");
  }
  if (input_schema_ == nullptr) {
    return arrow::Status::Invalid("input schema must not be null");
  }
  if (exec_state_ == nullptr) {
    return arrow::Status::Invalid("exec state must not be null");
  }
  if (exec_state_->output_reader == nullptr) {
    return arrow::Status::Invalid("output reader must not be null");
  }
  if (exec_state_->output_schema == nullptr) {
    return arrow::Status::Invalid("output schema must not be null");
  }
  if (exec_state_->output_exhausted) {
    return std::shared_ptr<arrow::RecordBatch>();
  }

  if (keys_.empty()) {
    return arrow::Status::NotImplemented("group-by without keys is not implemented");
  }

  std::shared_ptr<arrow::RecordBatch> batch;
  ARROW_RETURN_NOT_OK(exec_state_->output_reader->ReadNext(&batch));
  if (batch == nullptr) {
    exec_state_->output_exhausted = true;
    if (exec_state_->output_started) {
      return std::shared_ptr<arrow::RecordBatch>();
    }

    exec_state_->output_started = true;
    std::vector<std::shared_ptr<arrow::Array>> empty_columns;
    empty_columns.reserve(static_cast<std::size_t>(exec_state_->output_schema->num_fields()));
    for (const auto& field : exec_state_->output_schema->fields()) {
      if (field == nullptr) {
        return arrow::Status::Invalid("aggregate output field must not be null");
      }
      ARROW_ASSIGN_OR_RAISE(auto array,
                            arrow::MakeEmptyArray(field->type(), exec_context_.memory_pool()));
      empty_columns.push_back(std::move(array));
    }
    return arrow::RecordBatch::Make(exec_state_->output_schema, /*num_rows=*/0,
                                    std::move(empty_columns));
  }

  exec_state_->output_started = true;
  return arrow::RecordBatch::Make(exec_state_->output_schema, batch->num_rows(), batch->columns());
}

arrow::Result<OperatorStatus> ArrowComputeAggTransformOp::TransformImpl(
    std::shared_ptr<arrow::RecordBatch>* batch) {
  if (engine_ == nullptr) {
    return arrow::Status::Invalid("engine must not be null");
  }

  if (*batch == nullptr) {
    if (!finalized_) {
      if (input_schema_ == nullptr) {
        finalized_ = true;
        eos_forwarded_ = true;
        batch->reset();
        return OperatorStatus::kHasOutput;
      }

      if (exec_state_ == nullptr) {
        return arrow::Status::Invalid("arrow compute agg is missing execution state");
      }
      if (!exec_state_->input_closed) {
        if (!exec_state_->input_producer.Close()) {
          return arrow::Status::Cancelled("arrow compute agg input producer closed early");
        }
        exec_state_->input_closed = true;
      }
      finalized_ = true;
    }

    if (exec_state_ != nullptr) {
      ARROW_ASSIGN_OR_RAISE(*batch, NextOutputBatch());
      if (*batch != nullptr) {
        return OperatorStatus::kHasOutput;
      }
      exec_state_.reset();
    }
    if (!eos_forwarded_) {
      eos_forwarded_ = true;
      batch->reset();
      return OperatorStatus::kHasOutput;
    }
    batch->reset();
    return OperatorStatus::kHasOutput;
  }

  if (finalized_) {
    return arrow::Status::Invalid("arrow compute agg received input after finalization");
  }

  const auto& input = **batch;
  if (input_schema_ == nullptr) {
    input_schema_ = input.schema();
    if (input_schema_ == nullptr) {
      return arrow::Status::Invalid("input schema must not be null");
    }
    exec_state_ = std::make_unique<ExecState>();

    if (keys_.empty()) {
      return arrow::Status::NotImplemented("group-by without keys is not implemented");
    }

    std::vector<arrow::compute::Expression> project_exprs;
    std::vector<std::string> project_names;
    project_exprs.reserve(keys_.size() + aggs_.size());
    project_names.reserve(keys_.size() + aggs_.size());

    for (const auto& key : keys_) {
      if (key.expr == nullptr) {
        return arrow::Status::Invalid("group-by key expr must not be null");
      }
      ARROW_ASSIGN_OR_RAISE(auto compiled,
                            CompileExpr(input_schema_, *key.expr, engine_, &exec_context_));
      project_exprs.push_back(std::move(compiled.bound));
      project_names.push_back(key.name);
    }

    std::vector<arrow::FieldRef> group_keys;
    group_keys.reserve(keys_.size());
    for (int32_t i = 0; i < static_cast<int32_t>(keys_.size()); ++i) {
      group_keys.emplace_back(i);
    }

    std::vector<arrow::compute::Aggregate> aggregates;
    aggregates.reserve(aggs_.size());
    int32_t arg_columns = 0;
    for (const auto& agg : aggs_) {
      ARROW_ASSIGN_OR_RAISE(auto func_name, ToHashAggFunctionName(agg.func));
      if (func_name == "hash_count_all") {
        if (agg.arg != nullptr) {
          return arrow::Status::Invalid("count_all must not have an argument expression");
        }
        aggregates.emplace_back(std::move(func_name), agg.name);
        continue;
      }

      if (agg.arg == nullptr) {
        return arrow::Status::Invalid("aggregate argument expr must not be null");
      }
      const int32_t target_index = static_cast<int32_t>(keys_.size()) + arg_columns;
      const std::string arg_name = "__tiforth_agg_arg" + std::to_string(arg_columns);
      ARROW_ASSIGN_OR_RAISE(auto compiled,
                            CompileExpr(input_schema_, *agg.arg, engine_, &exec_context_));
      project_exprs.push_back(std::move(compiled.bound));
      project_names.push_back(arg_name);
      aggregates.emplace_back(std::move(func_name), arrow::FieldRef(target_index), agg.name);
      ++arg_columns;
    }

    arrow::acero::Declaration plan = arrow::acero::Declaration::Sequence(
        {{"source", arrow::acero::SourceNodeOptions(input_schema_, exec_state_->input_gen)},
         {"project", arrow::acero::ProjectNodeOptions(std::move(project_exprs),
                                                      std::move(project_names))},
         {"aggregate", arrow::acero::AggregateNodeOptions(std::move(aggregates),
                                                          std::move(group_keys))}});

    arrow::acero::QueryOptions options;
    options.use_threads = true;
    options.memory_pool = exec_context_.memory_pool();
    options.function_registry = engine_->function_registry();
    ARROW_ASSIGN_OR_RAISE(exec_state_->output_reader,
                          arrow::acero::DeclarationToReader(std::move(plan), std::move(options)));
    const auto output_schema = exec_state_->output_reader->schema();
    if (output_schema == nullptr) {
      return arrow::Status::Invalid("aggregate output schema must not be null");
    }
    ARROW_ASSIGN_OR_RAISE(exec_state_->output_schema, RenameKeyFields(output_schema, keys_));
  } else if (!input_schema_->Equals(*input.schema(), /*check_metadata=*/true)) {
    return arrow::Status::Invalid("arrow compute agg input schema mismatch");
  }

  arrow::compute::ExecBatch exec_batch(input);
  if (!exec_state_->input_producer.Push(
          std::optional<arrow::compute::ExecBatch>(std::move(exec_batch)))) {
    return arrow::Status::Cancelled("arrow compute agg input producer closed early");
  }
  batch->reset();
  return OperatorStatus::kNeedInput;
}

}  // namespace tiforth
