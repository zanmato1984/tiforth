#include "tiforth/operators/arrow_compute_agg.h"

#include <cstdint>
#include <string_view>
#include <utility>
#include <vector>

#include <arrow/acero/exec_plan.h>
#include <arrow/acero/options.h>
#include <arrow/array/concatenate.h>
#include <arrow/array/util.h>
#include <arrow/compute/api_aggregate.h>
#include <arrow/compute/expression.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <arrow/util/logging.h>

#include "tiforth/engine.h"

namespace tiforth {

namespace {

arrow::Result<arrow::FieldRef> ToArrowFieldRef(const Expr& expr) {
  const auto* field_ref = std::get_if<FieldRef>(&expr.node);
  if (field_ref == nullptr) {
    return arrow::Status::NotImplemented("only field_ref expressions are supported");
  }
  if (field_ref->index >= 0) {
    return arrow::FieldRef(field_ref->index);
  }
  if (!field_ref->name.empty()) {
    return arrow::FieldRef(field_ref->name);
  }
  return arrow::Status::Invalid("field_ref must have a name or index");
}

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

arrow::Result<std::shared_ptr<arrow::RecordBatch>> CombineBatches(
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches, arrow::MemoryPool* pool) {
  if (batches.empty()) {
    return arrow::Status::Invalid("no batches to combine");
  }
  if (batches.size() == 1) {
    return batches.front();
  }

  const auto schema = batches.front()->schema();
  if (schema == nullptr) {
    return arrow::Status::Invalid("batch schema must not be null");
  }

  const int num_columns = schema->num_fields();
  int64_t total_rows = 0;
  for (const auto& batch : batches) {
    if (batch == nullptr) {
      return arrow::Status::Invalid("batch must not be null");
    }
    if (!schema->Equals(*batch->schema(), /*check_metadata=*/true)) {
      return arrow::Status::Invalid("batch schema mismatch");
    }
    if (batch->num_columns() != num_columns) {
      return arrow::Status::Invalid("batch column count mismatch");
    }
    total_rows += batch->num_rows();
  }

  std::vector<std::shared_ptr<arrow::Array>> columns;
  columns.reserve(static_cast<std::size_t>(num_columns));
  for (int col = 0; col < num_columns; ++col) {
    std::vector<std::shared_ptr<arrow::Array>> chunks;
    chunks.reserve(batches.size());
    for (const auto& batch : batches) {
      chunks.push_back(batch->column(col));
    }
    ARROW_ASSIGN_OR_RAISE(auto combined, arrow::Concatenate(chunks, pool));
    columns.push_back(std::move(combined));
  }

  return arrow::RecordBatch::Make(schema, total_rows, std::move(columns));
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

arrow::Result<std::shared_ptr<arrow::RecordBatch>> ArrowComputeAggTransformOp::FinalizeOutput() {
  if (engine_ == nullptr) {
    return arrow::Status::Invalid("engine must not be null");
  }
  if (input_schema_ == nullptr) {
    return arrow::Status::Invalid("input schema must not be null");
  }

  // MS14: keep implementation intentionally narrow. Extend to computed keys/args via an
  // explicit project node if needed.
  if (keys_.empty()) {
    return arrow::Status::NotImplemented("group-by without keys is not implemented");
  }

  ARROW_ASSIGN_OR_RAISE(auto table, arrow::Table::FromRecordBatches(input_schema_, buffered_));

  std::vector<arrow::FieldRef> group_keys;
  group_keys.reserve(keys_.size());
  for (const auto& key : keys_) {
    if (key.expr == nullptr) {
      return arrow::Status::Invalid("group-by key expr must not be null");
    }
    ARROW_ASSIGN_OR_RAISE(auto ref, ToArrowFieldRef(*key.expr));
    group_keys.push_back(std::move(ref));
  }

  std::vector<arrow::compute::Aggregate> aggregates;
  aggregates.reserve(aggs_.size());
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
    ARROW_ASSIGN_OR_RAISE(auto target_ref, ToArrowFieldRef(*agg.arg));
    aggregates.emplace_back(std::move(func_name), std::move(target_ref), agg.name);
  }

  arrow::acero::Declaration plan = arrow::acero::Declaration::Sequence(
      {{"table_source", arrow::acero::TableSourceNodeOptions(std::move(table))},
       {"aggregate", arrow::acero::AggregateNodeOptions(std::move(aggregates),
                                                        std::move(group_keys))}});

  ARROW_ASSIGN_OR_RAISE(
      auto out,
      arrow::acero::DeclarationToExecBatches(std::move(plan), /*use_threads=*/false,
                                             exec_context_.memory_pool(),
                                             engine_->function_registry()));

  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  batches.reserve(out.batches.size());
  for (const auto& exec_batch : out.batches) {
    ARROW_ASSIGN_OR_RAISE(auto batch,
                          exec_batch.ToRecordBatch(out.schema, exec_context_.memory_pool()));
    batches.push_back(std::move(batch));
  }

  std::shared_ptr<arrow::RecordBatch> combined;
  if (!batches.empty()) {
    ARROW_ASSIGN_OR_RAISE(combined, CombineBatches(batches, exec_context_.memory_pool()));
  } else {
    if (out.schema == nullptr) {
      return arrow::Status::Invalid("aggregate output schema must not be null");
    }
    std::vector<std::shared_ptr<arrow::Array>> empty_columns;
    empty_columns.reserve(static_cast<std::size_t>(out.schema->num_fields()));
    for (const auto& field : out.schema->fields()) {
      if (field == nullptr) {
        return arrow::Status::Invalid("aggregate output field must not be null");
      }
      ARROW_ASSIGN_OR_RAISE(auto array,
                            arrow::MakeEmptyArray(field->type(), exec_context_.memory_pool()));
      empty_columns.push_back(std::move(array));
    }
    combined =
        arrow::RecordBatch::Make(out.schema, /*num_rows=*/0, std::move(empty_columns));
  }

  if (combined == nullptr) {
    return arrow::Status::Invalid("aggregate output batch must not be null");
  }

  // Aggregate node outputs keys first. Rename key columns to match AggKey names for
  // consistency with TiForth native hash agg APIs.
  auto schema = combined->schema();
  if (schema == nullptr) {
    return arrow::Status::Invalid("aggregate output schema must not be null");
  }
  if (static_cast<std::size_t>(schema->num_fields()) < keys_.size()) {
    return arrow::Status::Invalid("aggregate output schema is missing key fields");
  }
  auto fields = schema->fields();
  for (std::size_t i = 0; i < keys_.size(); ++i) {
    if (fields[i] == nullptr) {
      return arrow::Status::Invalid("aggregate output field must not be null");
    }
    fields[i] = fields[i]->WithName(keys_[i].name);
  }

  auto renamed_schema = arrow::schema(std::move(fields), schema->metadata());
  return arrow::RecordBatch::Make(std::move(renamed_schema), combined->num_rows(),
                                  combined->columns());
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

      ARROW_ASSIGN_OR_RAISE(*batch, FinalizeOutput());
      finalized_ = true;
      return OperatorStatus::kHasOutput;
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
  } else if (!input_schema_->Equals(*input.schema(), /*check_metadata=*/true)) {
    return arrow::Status::Invalid("arrow compute agg input schema mismatch");
  }

  buffered_.push_back(std::move(*batch));
  batch->reset();
  return OperatorStatus::kNeedInput;
}

}  // namespace tiforth
