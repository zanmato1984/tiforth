#include "tiforth/operators/arrow_compute_agg.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <arrow/array/util.h>
#include <arrow/acero/exec_plan.h>
#include <arrow/acero/options.h>
#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/compute/api.h>
#include <arrow/compute/api_aggregate.h>
#include <arrow/compute/expression.h>
#include <arrow/datum.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/util/async_generator.h>
#include <arrow/util/logging.h>

#include "tiforth/compiled_expr.h"
#include "tiforth/detail/arrow_compute.h"
#include "tiforth/engine.h"

namespace tiforth {

struct ArrowComputeAggTransformOp::ExecState {
  ExecState() : input_producer(input_gen.producer()) {}

  arrow::PushGenerator<std::optional<arrow::compute::ExecBatch>> input_gen;
  arrow::PushGenerator<std::optional<arrow::compute::ExecBatch>>::Producer input_producer;
  std::unique_ptr<arrow::RecordBatchReader> output_reader;
  std::shared_ptr<arrow::Schema> output_schema;
  std::vector<std::shared_ptr<arrow::Array>> stable_dict_key_values;
  bool input_closed = false;
  bool output_started = false;
  bool output_exhausted = false;
};

struct ArrowComputeAggTransformOp::DictState {
  std::vector<CompiledExpr> keys;
  std::vector<std::optional<CompiledExpr>> agg_args;
  std::vector<std::optional<int>> key_field_indices;
  std::vector<std::optional<int>> agg_arg_field_indices;

  std::shared_ptr<arrow::Schema> projected_schema;
  std::shared_ptr<arrow::Schema> encoded_schema;
  std::vector<std::shared_ptr<arrow::DataType>> stable_key_value_types;
  std::vector<std::unique_ptr<arrow::DictionaryUnifier>> stable_key_unifiers;
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

arrow::Result<std::shared_ptr<arrow::Schema>> MoveKeyFieldsToEnd(
    const std::shared_ptr<arrow::Schema>& schema, std::size_t num_keys) {
  if (schema == nullptr) {
    return arrow::Status::Invalid("schema must not be null");
  }
  if (static_cast<std::size_t>(schema->num_fields()) < num_keys) {
    return arrow::Status::Invalid("schema is missing key fields");
  }

  auto fields = schema->fields();
  std::vector<std::shared_ptr<arrow::Field>> reordered;
  reordered.reserve(fields.size());
  for (std::size_t i = num_keys; i < fields.size(); ++i) {
    reordered.push_back(std::move(fields[i]));
  }
  for (std::size_t i = 0; i < num_keys; ++i) {
    reordered.push_back(std::move(fields[i]));
  }
  return arrow::schema(std::move(reordered), schema->metadata());
}

std::optional<int> FieldRefIndexInSchema(const std::shared_ptr<arrow::Schema>& schema,
                                         const ExprPtr& expr) {
  if (schema == nullptr || expr == nullptr) {
    return std::nullopt;
  }
  if (!std::holds_alternative<FieldRef>(expr->node)) {
    return std::nullopt;
  }
  const auto& ref = std::get<FieldRef>(expr->node);
  if (ref.index >= 0) {
    return ref.index;
  }
  if (!ref.name.empty()) {
    const int index = schema->GetFieldIndex(ref.name);
    if (index >= 0) {
      return index;
    }
  }
  return std::nullopt;
}

bool IsBinaryLikeKeyType(const std::shared_ptr<arrow::DataType>& type) {
  if (type == nullptr) {
    return false;
  }
  switch (type->id()) {
    case arrow::Type::BINARY:
    case arrow::Type::STRING:
      return true;
    default:
      return false;
  }
}

arrow::Result<std::shared_ptr<arrow::Array>> EncodeBinaryKeyAsStableInt32(
    const std::shared_ptr<arrow::Array>& key_array, arrow::DictionaryUnifier* unifier,
    arrow::compute::ExecContext* exec_context) {
  if (key_array == nullptr) {
    return arrow::Status::Invalid("key array must not be null");
  }
  if (!IsBinaryLikeKeyType(key_array->type())) {
    return arrow::Status::Invalid("expected binary/string key array");
  }
  if (unifier == nullptr) {
    return arrow::Status::Invalid("dictionary unifier must not be null");
  }
  if (exec_context == nullptr) {
    return arrow::Status::Invalid("exec_context must not be null");
  }

  ARROW_ASSIGN_OR_RAISE(
      auto encoded_datum,
      arrow::compute::CallFunction("dictionary_encode", {arrow::Datum(key_array)}, exec_context));
  if (!encoded_datum.is_array()) {
    return arrow::Status::Invalid("dictionary_encode returned non-array datum");
  }
  const auto encoded_array = encoded_datum.make_array();
  if (encoded_array == nullptr) {
    return arrow::Status::Invalid("dictionary_encode returned null array");
  }

  const auto dict_array = std::dynamic_pointer_cast<arrow::DictionaryArray>(encoded_array);
  if (dict_array == nullptr) {
    return arrow::Status::Invalid("dictionary_encode returned non-dictionary array");
  }
  const auto dict_values = dict_array->dictionary();
  if (dict_values == nullptr) {
    return arrow::Status::Invalid("dictionary_encode returned null dictionary values");
  }

  std::shared_ptr<arrow::Buffer> transpose;
  ARROW_RETURN_NOT_OK(unifier->Unify(*dict_values, &transpose));
  if (transpose == nullptr && dict_values->length() > 0) {
    return arrow::Status::Invalid("dictionary unifier returned null transpose map");
  }
  const auto* transpose_map =
      transpose != nullptr ? reinterpret_cast<const int32_t*>(transpose->data()) : nullptr;

  const auto indices = dict_array->indices();
  if (indices == nullptr) {
    return arrow::Status::Invalid("dictionary indices must not be null");
  }
  if (indices->length() != key_array->length()) {
    return arrow::Status::Invalid("dictionary indices length mismatch");
  }

  arrow::Int32Builder out(exec_context->memory_pool());
  ARROW_RETURN_NOT_OK(out.Reserve(indices->length()));

  switch (indices->type_id()) {
    case arrow::Type::INT8: {
      const auto& typed = static_cast<const arrow::Int8Array&>(*indices);
      for (int64_t i = 0; i < typed.length(); ++i) {
        if (typed.IsNull(i)) {
          ARROW_RETURN_NOT_OK(out.AppendNull());
          continue;
        }
        const int32_t local = static_cast<int32_t>(typed.Value(i));
        ARROW_RETURN_NOT_OK(out.Append(transpose_map[local]));
      }
      break;
    }
    case arrow::Type::INT16: {
      const auto& typed = static_cast<const arrow::Int16Array&>(*indices);
      for (int64_t i = 0; i < typed.length(); ++i) {
        if (typed.IsNull(i)) {
          ARROW_RETURN_NOT_OK(out.AppendNull());
          continue;
        }
        const int32_t local = static_cast<int32_t>(typed.Value(i));
        ARROW_RETURN_NOT_OK(out.Append(transpose_map[local]));
      }
      break;
    }
    case arrow::Type::INT32: {
      const auto& typed = static_cast<const arrow::Int32Array&>(*indices);
      for (int64_t i = 0; i < typed.length(); ++i) {
        if (typed.IsNull(i)) {
          ARROW_RETURN_NOT_OK(out.AppendNull());
          continue;
        }
        const int32_t local = typed.Value(i);
        ARROW_RETURN_NOT_OK(out.Append(transpose_map[local]));
      }
      break;
    }
    case arrow::Type::INT64: {
      const auto& typed = static_cast<const arrow::Int64Array&>(*indices);
      for (int64_t i = 0; i < typed.length(); ++i) {
        if (typed.IsNull(i)) {
          ARROW_RETURN_NOT_OK(out.AppendNull());
          continue;
        }
        const int32_t local = static_cast<int32_t>(typed.Value(i));
        ARROW_RETURN_NOT_OK(out.Append(transpose_map[local]));
      }
      break;
    }
    default:
      return arrow::Status::NotImplemented(
          "dictionary indices type not supported: ", indices->type()->ToString());
  }

  std::shared_ptr<arrow::Array> out_array;
  ARROW_RETURN_NOT_OK(out.Finish(&out_array));
  return out_array;
}

arrow::Result<std::shared_ptr<arrow::Schema>> RestoreStableKeyFieldTypesAtEnd(
    const std::shared_ptr<arrow::Schema>& schema, std::size_t num_keys,
    const std::vector<std::shared_ptr<arrow::DataType>>& stable_value_types) {
  if (schema == nullptr) {
    return arrow::Status::Invalid("schema must not be null");
  }
  if (schema->num_fields() < 0) {
    return arrow::Status::Invalid("schema must have non-negative field count");
  }
  const std::size_t fields_count = static_cast<std::size_t>(schema->num_fields());
  if (fields_count < num_keys) {
    return arrow::Status::Invalid("schema is missing key fields");
  }
  if (stable_value_types.size() != num_keys) {
    return arrow::Status::Invalid("stable key types mismatch");
  }

  auto fields = schema->fields();
  for (std::size_t key_i = 0; key_i < num_keys; ++key_i) {
    const auto& value_type = stable_value_types[key_i];
    if (value_type == nullptr) {
      continue;
    }
    const std::size_t i = fields_count - num_keys + key_i;
    const auto& field = fields[i];
    if (field == nullptr) {
      return arrow::Status::Invalid("schema field must not be null");
    }
    fields[i] = field->WithType(value_type);
  }
  return arrow::schema(std::move(fields), schema->metadata());
}

}  // namespace

ArrowComputeAggTransformOp::ArrowComputeAggTransformOp(const Engine* engine, std::vector<AggKey> keys,
                                                       std::vector<AggFunc> aggs,
                                                       ArrowComputeAggOptions options,
                                                       arrow::MemoryPool* memory_pool)
    : engine_(engine),
      keys_(std::move(keys)),
      aggs_(std::move(aggs)),
      options_(options),
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
  const int32_t num_keys = static_cast<int32_t>(keys_.size());
  auto columns = batch->columns();
  std::vector<std::shared_ptr<arrow::Array>> reordered;
  reordered.reserve(columns.size());
  for (int32_t i = num_keys; i < static_cast<int32_t>(columns.size()); ++i) {
    reordered.push_back(columns[i]);
  }
  for (int32_t i = 0; i < num_keys; ++i) {
    reordered.push_back(columns[i]);
  }

  if (options_.stable_dictionary_encode_binary_keys) {
    const int32_t start = static_cast<int32_t>(reordered.size()) - num_keys;
    if (exec_state_->stable_dict_key_values.size() != static_cast<std::size_t>(num_keys)) {
      return arrow::Status::Invalid("stable dictionary output state is not initialized");
    }
    for (int32_t key_i = 0; key_i < num_keys; ++key_i) {
      const auto& dict_values =
          exec_state_->stable_dict_key_values[static_cast<std::size_t>(key_i)];
      if (dict_values == nullptr) {
        continue;
      }
      const int32_t i = start + key_i;
      if (i < 0 || i >= static_cast<int32_t>(reordered.size())) {
        return arrow::Status::Invalid("stable dictionary key index out of range");
      }
      if (reordered[i] == nullptr) {
        return arrow::Status::Invalid("output key column must not be null");
      }
      if (reordered[i]->type_id() != arrow::Type::INT32) {
        return arrow::Status::Invalid("stable dictionary output key expected int32, got: ",
                                      reordered[i]->type()->ToString());
      }
      ARROW_ASSIGN_OR_RAISE(
          auto decoded,
          arrow::compute::CallFunction("take",
                                       {arrow::Datum(dict_values), arrow::Datum(reordered[i])},
                                       &exec_context_));
      if (!decoded.is_array()) {
        return arrow::Status::Invalid("take returned non-array datum");
      }
      reordered[i] = decoded.make_array();
    }
  }
  return arrow::RecordBatch::Make(exec_state_->output_schema, batch->num_rows(),
                                  std::move(reordered));
}

pipeline::PipelinePipe ArrowComputeAggTransformOp::Pipe(const pipeline::PipelineContext&) {
  return [this](const pipeline::PipelineContext&, const task::TaskContext&, pipeline::ThreadId,
                std::optional<pipeline::Batch> input) -> pipeline::OpResult {
    if (!input.has_value()) {
      return pipeline::OpOutput::PipeSinkNeedsMore();
    }
    auto batch = std::move(*input);
    if (batch == nullptr) {
      return arrow::Status::Invalid("arrow compute agg input batch must not be null");
    }
    ARROW_RETURN_NOT_OK(ConsumeBatch(std::move(batch)));
    return pipeline::OpOutput::PipeSinkNeedsMore();
  };
}

pipeline::PipelineDrain ArrowComputeAggTransformOp::Drain(const pipeline::PipelineContext&) {
  return [this](const pipeline::PipelineContext&, const task::TaskContext&,
                pipeline::ThreadId) -> pipeline::OpResult {
    ARROW_RETURN_NOT_OK(FinalizeIfNeeded());
    if (exec_state_ == nullptr) {
      return pipeline::OpOutput::Finished();
    }
    ARROW_ASSIGN_OR_RAISE(auto batch, NextOutputBatch());
    if (batch != nullptr) {
      return pipeline::OpOutput::SourcePipeHasMore(std::move(batch));
    }
    exec_state_.reset();
    return pipeline::OpOutput::Finished();
  };
}

arrow::Status ArrowComputeAggTransformOp::ConsumeBatch(std::shared_ptr<arrow::RecordBatch> batch) {
  if (engine_ == nullptr) {
    return arrow::Status::Invalid("engine must not be null");
  }
  if (batch == nullptr) {
    return arrow::Status::Invalid("input batch must not be null");
  }
  if (finalized_) {
    return arrow::Status::Invalid("arrow compute agg received input after finalization");
  }

  if (options_.stable_dictionary_encode_binary_keys) {
    return ConsumeBatchStableDictionary(std::move(batch));
  }

  const auto& input = *batch;
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
    ARROW_ASSIGN_OR_RAISE(auto renamed_schema, RenameKeyFields(output_schema, keys_));
    ARROW_ASSIGN_OR_RAISE(exec_state_->output_schema,
                          MoveKeyFieldsToEnd(std::move(renamed_schema), keys_.size()));
  } else if (!input_schema_->Equals(*input.schema(), /*check_metadata=*/true)) {
    return arrow::Status::Invalid("arrow compute agg input schema mismatch");
  }

  if (exec_state_ == nullptr) {
    return arrow::Status::Invalid("arrow compute agg missing exec state");
  }

  arrow::compute::ExecBatch exec_batch(input);
  if (!exec_state_->input_producer.Push(
          std::optional<arrow::compute::ExecBatch>(std::move(exec_batch)))) {
    return arrow::Status::Cancelled("arrow compute agg input producer closed early");
  }
  return arrow::Status::OK();
}

arrow::Status ArrowComputeAggTransformOp::FinalizeIfNeeded() {
  if (engine_ == nullptr) {
    return arrow::Status::Invalid("engine must not be null");
  }
  if (finalized_) {
    return arrow::Status::OK();
  }
  if (input_schema_ == nullptr) {
    finalized_ = true;
    return arrow::Status::OK();
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

  if (options_.stable_dictionary_encode_binary_keys) {
    if (keys_.empty()) {
      return arrow::Status::NotImplemented("group-by without keys is not implemented");
    }
    if (dict_state_ == nullptr || dict_state_->encoded_schema == nullptr) {
      return arrow::Status::Invalid("stable dictionary agg is missing encoded schema state");
    }

    const int32_t num_keys = static_cast<int32_t>(keys_.size());
    if (dict_state_->stable_key_unifiers.size() != static_cast<std::size_t>(num_keys) ||
        dict_state_->stable_key_value_types.size() != static_cast<std::size_t>(num_keys)) {
      return arrow::Status::Invalid("stable dictionary agg missing key unifier state");
    }
    exec_state_->stable_dict_key_values.resize(static_cast<std::size_t>(num_keys));
    for (int32_t i = 0; i < num_keys; ++i) {
      const auto& value_type = dict_state_->stable_key_value_types[static_cast<std::size_t>(i)];
      if (value_type == nullptr) {
        continue;
      }
      auto& unifier = dict_state_->stable_key_unifiers[static_cast<std::size_t>(i)];
      if (unifier == nullptr) {
        return arrow::Status::Invalid("stable dictionary key unifier must not be null");
      }
      std::shared_ptr<arrow::Array> dict_values;
      ARROW_RETURN_NOT_OK(unifier->GetResultWithIndexType(arrow::int32(), &dict_values));
      if (dict_values == nullptr) {
        return arrow::Status::Invalid("stable dictionary values must not be null");
      }
      exec_state_->stable_dict_key_values[static_cast<std::size_t>(i)] = std::move(dict_values);
    }
    dict_state_.reset();
  }

  finalized_ = true;
  return arrow::Status::OK();
}

arrow::Status ArrowComputeAggTransformOp::ConsumeBatchStableDictionary(
    std::shared_ptr<arrow::RecordBatch> batch) {
  if (engine_ == nullptr) {
    return arrow::Status::Invalid("engine must not be null");
  }
  if (batch == nullptr) {
    return arrow::Status::Invalid("input batch must not be null");
  }
  if (!options_.stable_dictionary_encode_binary_keys) {
    return arrow::Status::Invalid("stable dictionary mode is not enabled");
  }

  if (finalized_) {
    return arrow::Status::Invalid("arrow compute agg received input after finalization");
  }

  const auto& input = *batch;
  if (input_schema_ == nullptr) {
    input_schema_ = input.schema();
    if (input_schema_ == nullptr) {
      return arrow::Status::Invalid("input schema must not be null");
    }
    if (keys_.empty()) {
      return arrow::Status::NotImplemented("group-by without keys is not implemented");
    }

    dict_state_ = std::make_unique<DictState>();
    dict_state_->keys.reserve(keys_.size());
    dict_state_->key_field_indices.reserve(keys_.size());
    for (const auto& key : keys_) {
      if (key.name.empty()) {
        return arrow::Status::Invalid("group-by key name must not be empty");
      }
      if (key.expr == nullptr) {
        return arrow::Status::Invalid("group-by key expr must not be null");
      }
      ARROW_ASSIGN_OR_RAISE(auto compiled,
                            CompileExpr(input_schema_, *key.expr, engine_, &exec_context_));
      dict_state_->keys.push_back(std::move(compiled));
      dict_state_->key_field_indices.push_back(FieldRefIndexInSchema(input_schema_, key.expr));
    }

    dict_state_->agg_args.resize(aggs_.size());
    dict_state_->agg_arg_field_indices.resize(aggs_.size());
    for (std::size_t i = 0; i < aggs_.size(); ++i) {
      const auto& agg = aggs_[i];
      if (agg.name.empty()) {
        return arrow::Status::Invalid("aggregate output name must not be empty");
      }
      ARROW_ASSIGN_OR_RAISE(auto func_name, ToHashAggFunctionName(agg.func));
      if (func_name == "hash_count_all") {
        if (agg.arg != nullptr) {
          return arrow::Status::Invalid("count_all must not have an argument expression");
        }
        dict_state_->agg_args[i] = std::nullopt;
        dict_state_->agg_arg_field_indices[i] = std::nullopt;
        continue;
      }
      if (agg.arg == nullptr) {
        return arrow::Status::Invalid("aggregate argument expr must not be null");
      }
      ARROW_ASSIGN_OR_RAISE(auto compiled,
                            CompileExpr(input_schema_, *agg.arg, engine_, &exec_context_));
      dict_state_->agg_args[i] = std::move(compiled);
      dict_state_->agg_arg_field_indices[i] = FieldRefIndexInSchema(input_schema_, agg.arg);
    }
  } else if (!input_schema_->Equals(*input.schema(), /*check_metadata=*/true)) {
    return arrow::Status::Invalid("arrow compute agg input schema mismatch");
  }

  if (dict_state_ == nullptr) {
    return arrow::Status::Invalid("stable dictionary agg missing dict state");
  }

  if (exec_state_ != nullptr && dict_state_->encoded_schema == nullptr) {
    return arrow::Status::Invalid("stable dictionary agg missing encoded schema");
  }

  std::vector<std::shared_ptr<arrow::Array>> key_arrays;
  key_arrays.reserve(keys_.size());
  for (const auto& compiled_key : dict_state_->keys) {
    ARROW_ASSIGN_OR_RAISE(auto arr, ExecuteExprAsArray(compiled_key, input, &exec_context_));
    key_arrays.push_back(std::move(arr));
  }

  std::vector<std::shared_ptr<arrow::Array>> arg_arrays;
  arg_arrays.reserve(aggs_.size());
  for (std::size_t i = 0; i < aggs_.size(); ++i) {
    if (!dict_state_->agg_args[i].has_value()) {
      continue;
    }
    ARROW_ASSIGN_OR_RAISE(auto arr,
                          ExecuteExprAsArray(*dict_state_->agg_args[i], input, &exec_context_));
    arg_arrays.push_back(std::move(arr));
  }

  std::vector<std::shared_ptr<arrow::Array>> columns;
  columns.reserve(key_arrays.size() + arg_arrays.size());
  for (auto& a : key_arrays) {
    columns.push_back(std::move(a));
  }
  for (auto& a : arg_arrays) {
    columns.push_back(std::move(a));
  }

  if (dict_state_->projected_schema == nullptr) {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    fields.reserve(columns.size());
    for (std::size_t i = 0; i < keys_.size(); ++i) {
      std::shared_ptr<arrow::Field> field;
      if (dict_state_->key_field_indices[i].has_value()) {
        const auto in_field = input_schema_->field(*dict_state_->key_field_indices[i]);
        if (in_field != nullptr) {
          field = in_field->WithName(keys_[i].name);
        }
      }
      if (field == nullptr) {
        field = arrow::field(keys_[i].name, columns[i]->type());
      }
      fields.push_back(std::move(field));
    }

    std::size_t arg_col = 0;
    for (std::size_t i = 0; i < aggs_.size(); ++i) {
      if (!dict_state_->agg_args[i].has_value()) {
        continue;
      }
      const std::string arg_name = "__tiforth_agg_arg" + std::to_string(arg_col);
      std::shared_ptr<arrow::Field> field;
      if (dict_state_->agg_arg_field_indices[i].has_value()) {
        const auto in_field = input_schema_->field(*dict_state_->agg_arg_field_indices[i]);
        if (in_field != nullptr) {
          field = in_field->WithName(arg_name);
        }
      }
      const std::size_t col_index = keys_.size() + arg_col;
      if (field == nullptr) {
        field = arrow::field(arg_name, columns[col_index]->type());
      }
      fields.push_back(std::move(field));
      ++arg_col;
    }

    dict_state_->projected_schema = arrow::schema(std::move(fields), input_schema_->metadata());
  }

  if (dict_state_->projected_schema == nullptr) {
    return arrow::Status::Invalid("stable dictionary agg missing projected schema");
  }
  if (dict_state_->projected_schema->num_fields() != static_cast<int>(columns.size())) {
    return arrow::Status::Invalid("projected schema column count mismatch");
  }

  if (dict_state_->encoded_schema == nullptr) {
    ARROW_RETURN_NOT_OK(detail::EnsureArrowComputeInitialized());

    const int32_t num_keys = static_cast<int32_t>(keys_.size());
    dict_state_->stable_key_value_types.resize(static_cast<std::size_t>(num_keys));
    dict_state_->stable_key_unifiers.resize(static_cast<std::size_t>(num_keys));

    auto encoded_fields = dict_state_->projected_schema->fields();
    for (int32_t i = 0; i < num_keys; ++i) {
      const auto& field = encoded_fields[static_cast<std::size_t>(i)];
      if (field == nullptr) {
        return arrow::Status::Invalid("projected key field must not be null");
      }
      const auto& type = field->type();
      if (!IsBinaryLikeKeyType(type)) {
        continue;
      }
      dict_state_->stable_key_value_types[static_cast<std::size_t>(i)] = type;
      ARROW_ASSIGN_OR_RAISE(
          dict_state_->stable_key_unifiers[static_cast<std::size_t>(i)],
          arrow::DictionaryUnifier::Make(type, exec_context_.memory_pool()));
      encoded_fields[static_cast<std::size_t>(i)] = field->WithType(arrow::int32());
    }

    dict_state_->encoded_schema =
        arrow::schema(std::move(encoded_fields), dict_state_->projected_schema->metadata());

    exec_state_ = std::make_unique<ExecState>();
    exec_state_->stable_dict_key_values.resize(static_cast<std::size_t>(num_keys));

    std::vector<arrow::FieldRef> group_keys;
    group_keys.reserve(keys_.size());
    for (int32_t i = 0; i < num_keys; ++i) {
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
      const int32_t target_index = num_keys + arg_columns;
      aggregates.emplace_back(std::move(func_name), arrow::FieldRef(target_index), agg.name);
      ++arg_columns;
    }

    arrow::acero::Declaration plan = arrow::acero::Declaration::Sequence(
        {{"source", arrow::acero::SourceNodeOptions(dict_state_->encoded_schema, exec_state_->input_gen)},
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
    ARROW_ASSIGN_OR_RAISE(auto renamed_schema, RenameKeyFields(output_schema, keys_));
    ARROW_ASSIGN_OR_RAISE(auto moved_schema,
                          MoveKeyFieldsToEnd(std::move(renamed_schema), keys_.size()));
    ARROW_ASSIGN_OR_RAISE(exec_state_->output_schema,
                          RestoreStableKeyFieldTypesAtEnd(std::move(moved_schema), keys_.size(),
                                                         dict_state_->stable_key_value_types));
  }

  if (dict_state_->encoded_schema == nullptr) {
    return arrow::Status::Invalid("stable dictionary agg missing encoded schema");
  }
  if (exec_state_ == nullptr) {
    return arrow::Status::Invalid("stable dictionary agg missing exec state");
  }
  if (dict_state_->stable_key_unifiers.size() != keys_.size() ||
      dict_state_->stable_key_value_types.size() != keys_.size()) {
    return arrow::Status::Invalid("stable dictionary agg missing key state");
  }

  std::vector<std::shared_ptr<arrow::Array>> encoded_columns;
  encoded_columns.reserve(columns.size());
  for (std::size_t i = 0; i < keys_.size(); ++i) {
    const auto& type = dict_state_->stable_key_value_types[i];
    if (type == nullptr) {
      encoded_columns.push_back(std::move(columns[i]));
      continue;
    }
    auto& unifier = dict_state_->stable_key_unifiers[i];
    if (unifier == nullptr) {
      return arrow::Status::Invalid("stable dictionary key unifier must not be null");
    }
    ARROW_ASSIGN_OR_RAISE(encoded_columns.emplace_back(),
                          EncodeBinaryKeyAsStableInt32(columns[i], unifier.get(), &exec_context_));
  }
  for (std::size_t i = keys_.size(); i < columns.size(); ++i) {
    encoded_columns.push_back(std::move(columns[i]));
  }

  auto encoded_batch =
      arrow::RecordBatch::Make(dict_state_->encoded_schema, input.num_rows(),
                               std::move(encoded_columns));
  arrow::compute::ExecBatch exec_batch(*encoded_batch);
  if (!exec_state_->input_producer.Push(
          std::optional<arrow::compute::ExecBatch>(std::move(exec_batch)))) {
    return arrow::Status::Cancelled("arrow compute agg input producer closed early");
  }
  return arrow::Status::OK();
}

}  // namespace tiforth
