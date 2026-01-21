#include "tiforth/operators/hash_agg.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <arrow/array/util.h>
#include <arrow/compute/function.h>
#include <arrow/compute/registry.h>
#include <arrow/compute/row/grouper.h>
#include <arrow/memory_pool.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/util/logging.h>

#include "tiforth/compiled_expr.h"
#include "tiforth/detail/arrow_compute.h"
#include "tiforth/detail/collation_single_key_grouper.h"
#include "tiforth/detail/small_string_single_key_grouper.h"
#include "tiforth/engine.h"
#include "tiforth/expr.h"
#include "tiforth/type_metadata.h"

namespace tiforth {

struct HashAggTransformOp::Compiled {
  std::vector<CompiledExpr> keys;
  std::vector<std::optional<CompiledExpr>> agg_args;
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
  return arrow::Status::NotImplemented("grouped hash aggregation is not supported: ", func);
}

std::optional<int> FieldRefIndex(const ExprPtr& expr,
                                 const std::shared_ptr<arrow::Schema>& schema) {
  if (expr == nullptr) {
    return std::nullopt;
  }
  if (std::holds_alternative<FieldRef>(expr->node)) {
    const auto& ref = std::get<FieldRef>(expr->node);
    if (ref.index >= 0) {
      return ref.index;
    }
    if (schema != nullptr && !ref.name.empty()) {
      const int idx = schema->GetFieldIndex(ref.name);
      if (idx >= 0) {
        return idx;
      }
    }
  }
  return std::nullopt;
}

std::vector<arrow::TypeHolder> ExtendWithGroupIdType(
    const std::vector<arrow::TypeHolder>& in_types) {
  std::vector<arrow::TypeHolder> out;
  out.reserve(in_types.size() + 1);
  out = in_types;
  out.emplace_back(arrow::uint32());
  return out;
}

arrow::Result<const arrow::compute::HashAggregateKernel*> GetKernel(
    arrow::compute::ExecContext* ctx, const arrow::compute::Aggregate& aggregate,
    const std::vector<arrow::TypeHolder>& in_types) {
  if (ctx == nullptr) {
    return arrow::Status::Invalid("exec context must not be null");
  }

  const auto aggr_in_types = ExtendWithGroupIdType(in_types);
  ARROW_ASSIGN_OR_RAISE(auto function, ctx->func_registry()->GetFunction(aggregate.function));
  if (function->kind() != arrow::compute::Function::HASH_AGGREGATE) {
    if (function->kind() == arrow::compute::Function::SCALAR_AGGREGATE) {
      return arrow::Status::Invalid(
          "expected hash aggregate function for group-by (function=", aggregate.function, ")");
    }
    return arrow::Status::Invalid("function is not an aggregate function: ", aggregate.function);
  }

  ARROW_ASSIGN_OR_RAISE(const arrow::compute::Kernel* kernel,
                        function->DispatchExact(aggr_in_types));
  return static_cast<const arrow::compute::HashAggregateKernel*>(kernel);
}

arrow::Result<std::unique_ptr<arrow::compute::KernelState>> InitKernel(
    const arrow::compute::HashAggregateKernel* kernel, arrow::compute::ExecContext* ctx,
    const arrow::compute::Aggregate& aggregate,
    const std::vector<arrow::TypeHolder>& in_types) {
  if (kernel == nullptr) {
    return arrow::Status::Invalid("hash aggregate kernel must not be null");
  }
  if (ctx == nullptr) {
    return arrow::Status::Invalid("exec context must not be null");
  }

  const auto aggr_in_types = ExtendWithGroupIdType(in_types);

  arrow::compute::KernelContext kernel_ctx{ctx};
  const auto* options = aggregate.options.get();
  if (options == nullptr) {
    auto maybe_function = ctx->func_registry()->GetFunction(aggregate.function);
    if (maybe_function.ok()) {
      options = maybe_function.ValueOrDie()->default_options();
    }
  }

  ARROW_ASSIGN_OR_RAISE(
      auto state,
      kernel->init(&kernel_ctx, arrow::compute::KernelInitArgs{kernel, aggr_in_types, options}));
  return state;
}

arrow::Result<std::vector<const arrow::compute::HashAggregateKernel*>> GetKernels(
    arrow::compute::ExecContext* ctx, const std::vector<arrow::compute::Aggregate>& aggregates,
    const std::vector<std::vector<arrow::TypeHolder>>& in_types) {
  if (aggregates.size() != in_types.size()) {
    return arrow::Status::Invalid("aggregate arity mismatch: aggregates=", aggregates.size(),
                                  " args=", in_types.size());
  }

  std::vector<const arrow::compute::HashAggregateKernel*> kernels(in_types.size());
  for (std::size_t i = 0; i < aggregates.size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(kernels[i], GetKernel(ctx, aggregates[i], in_types[i]));
  }
  return kernels;
}

arrow::Result<std::vector<std::unique_ptr<arrow::compute::KernelState>>> InitKernels(
    const std::vector<const arrow::compute::HashAggregateKernel*>& kernels,
    arrow::compute::ExecContext* ctx, const std::vector<arrow::compute::Aggregate>& aggregates,
    const std::vector<std::vector<arrow::TypeHolder>>& in_types) {
  if (kernels.size() != aggregates.size()) {
    return arrow::Status::Invalid("kernel count mismatch: kernels=", kernels.size(),
                                  " aggregates=", aggregates.size());
  }

  std::vector<std::unique_ptr<arrow::compute::KernelState>> states(kernels.size());
  for (std::size_t i = 0; i < aggregates.size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(states[i], InitKernel(kernels[i], ctx, aggregates[i], in_types[i]));
  }
  return states;
}

arrow::Result<std::unique_ptr<arrow::compute::Grouper>> MakeDefaultGrouper(
    const std::vector<arrow::TypeHolder>& key_types,
    const std::shared_ptr<arrow::Schema>& input_schema, const std::vector<AggKey>& keys,
    arrow::compute::ExecContext* exec_context) {
  if (exec_context == nullptr) {
    return arrow::Status::Invalid("exec context must not be null");
  }
  if (key_types.size() == 1) {
    const auto id = key_types[0].id();
    if (id == arrow::Type::BINARY || id == arrow::Type::STRING) {
      int32_t collation_id = 63;
      if (input_schema != nullptr && keys.size() == 1) {
        const auto maybe_index = FieldRefIndex(keys[0].expr, input_schema);
        if (maybe_index.has_value()) {
          const int idx = *maybe_index;
          if (idx < 0 || idx >= input_schema->num_fields()) {
            return arrow::Status::Invalid("group-by key field index out of range");
          }
          const auto& field = input_schema->field(idx);
          if (field == nullptr) {
            return arrow::Status::Invalid("input field must not be null");
          }
          ARROW_ASSIGN_OR_RAISE(const auto logical_type, GetLogicalType(*field));
          if (logical_type.id == LogicalTypeId::kString && logical_type.collation_id >= 0 &&
              logical_type.collation_id != 63) {
            collation_id = logical_type.collation_id;
          }
        }
      }
      auto key_type = key_types[0].GetSharedPtr();
      if (key_type == nullptr) {
        return arrow::Status::Invalid("key type must not be null");
      }
      if (collation_id != 63) {
        const auto collation = CollationFromId(collation_id);
        if (collation.kind == CollationKind::kUnsupported) {
          return arrow::Status::NotImplemented("unsupported collation id: ", collation_id);
        }
        return std::make_unique<detail::CollationSingleKeyGrouper>(std::move(key_type),
                                                                   collation_id, exec_context);
      }
      return std::make_unique<detail::SmallStringSingleKeyGrouper>(std::move(key_type),
                                                                   exec_context);
    }
  }
  return arrow::compute::Grouper::Make(key_types, exec_context);
}

}  // namespace

HashAggContext::HashAggContext(const Engine* engine, std::vector<AggKey> keys,
                               std::vector<AggFunc> aggs, GrouperFactory grouper_factory,
                               arrow::MemoryPool* memory_pool)
    : engine_(engine),
      keys_(std::move(keys)),
      aggs_(std::move(aggs)),
      grouper_factory_(std::move(grouper_factory)),
      exec_context_(memory_pool != nullptr
                        ? memory_pool
                        : (engine != nullptr ? engine->memory_pool() : arrow::default_memory_pool()),
                    /*executor=*/nullptr,
                    engine != nullptr ? engine->function_registry() : nullptr) {}

HashAggContext::~HashAggContext() = default;

HashAggTransformOp::HashAggTransformOp(std::shared_ptr<HashAggContext> context)
    : context_(std::move(context)),
      engine_(context_ != nullptr ? context_->engine() : nullptr),
      exec_context_(context_ != nullptr ? context_->memory_pool()
                                        : (engine_ != nullptr ? engine_->memory_pool()
                                                              : arrow::default_memory_pool()),
                    /*executor=*/nullptr,
                    engine_ != nullptr ? engine_->function_registry() : nullptr) {
  ARROW_DCHECK(context_ != nullptr);
}

HashAggTransformOp::~HashAggTransformOp() = default;

arrow::Status HashAggTransformOp::InitIfNeededAndConsume(const arrow::RecordBatch& batch) {
  if (engine_ == nullptr) {
    return arrow::Status::Invalid("hash agg engine must not be null");
  }
  if (context_ == nullptr) {
    return arrow::Status::Invalid("hash agg context must not be null");
  }
  if (context_->keys().empty()) {
    return arrow::Status::NotImplemented("group-by without keys is not implemented");
  }

  if (input_schema_ == nullptr) {
    input_schema_ = batch.schema();
    if (input_schema_ == nullptr) {
      return arrow::Status::Invalid("input schema must not be null");
    }
  } else if (!input_schema_->Equals(*batch.schema(), /*check_metadata=*/true)) {
    return arrow::Status::Invalid("hash agg input schema mismatch");
  }

  ARROW_RETURN_NOT_OK(detail::EnsureArrowComputeInitialized());

  if (compiled_ == nullptr) {
    compiled_ = std::make_unique<Compiled>();
    compiled_->keys.reserve(context_->keys().size());
    for (const auto& key : context_->keys()) {
      if (key.name.empty()) {
        return arrow::Status::Invalid("group-by key name must not be empty");
      }
      if (key.expr == nullptr) {
        return arrow::Status::Invalid("group-by key expr must not be null");
      }
      ARROW_ASSIGN_OR_RAISE(auto compiled_key,
                            CompileExpr(input_schema_, *key.expr, engine_, &exec_context_));
      compiled_->keys.push_back(std::move(compiled_key));
    }

    compiled_->agg_args.resize(context_->aggs().size());
    for (std::size_t i = 0; i < context_->aggs().size(); ++i) {
      const auto& agg = context_->aggs()[i];
      if (agg.name.empty()) {
        return arrow::Status::Invalid("aggregate output name must not be empty");
      }
      ARROW_ASSIGN_OR_RAISE(auto func_name, ToHashAggFunctionName(agg.func));
      if (func_name == "hash_count_all") {
        if (agg.arg != nullptr) {
          return arrow::Status::Invalid("count_all must not have an argument expression");
        }
        continue;
      }
      if (agg.arg == nullptr) {
        return arrow::Status::Invalid("aggregate argument expr must not be null");
      }
      ARROW_ASSIGN_OR_RAISE(auto compiled_arg,
                            CompileExpr(input_schema_, *agg.arg, engine_, &exec_context_));
      compiled_->agg_args[i] = std::move(compiled_arg);
    }
  }

  return ConsumeBatch(batch);
}

arrow::Status HashAggTransformOp::ConsumeBatch(const arrow::RecordBatch& batch) {
  if (compiled_ == nullptr) {
    return arrow::Status::Invalid("hash agg is missing compiled expressions");
  }
  if (static_cast<std::size_t>(batch.num_columns()) != static_cast<std::size_t>(batch.schema()->num_fields())) {
    return arrow::Status::Invalid("input batch schema mismatch");
  }
  if (context_ == nullptr) {
    return arrow::Status::Invalid("hash agg context must not be null");
  }

  const int64_t length = batch.num_rows();
  if (length < 0) {
    return arrow::Status::Invalid("negative batch length");
  }

  std::vector<std::shared_ptr<arrow::Array>> key_arrays;
  key_arrays.reserve(compiled_->keys.size());
  for (const auto& compiled_key : compiled_->keys) {
    ARROW_ASSIGN_OR_RAISE(auto array, ExecuteExprAsArray(compiled_key, batch, &exec_context_));
    if (array == nullptr) {
      return arrow::Status::Invalid("key array must not be null");
    }
    if (array->length() != length) {
      return arrow::Status::Invalid("key array length mismatch");
    }
    key_arrays.push_back(std::move(array));
  }

  std::vector<std::shared_ptr<arrow::Array>> agg_arg_arrays;
  agg_arg_arrays.resize(context_->aggs().size());
  for (std::size_t i = 0; i < context_->aggs().size(); ++i) {
    if (!compiled_->agg_args[i].has_value()) {
      continue;
    }
    ARROW_ASSIGN_OR_RAISE(auto array,
                          ExecuteExprAsArray(*compiled_->agg_args[i], batch, &exec_context_));
    if (array == nullptr) {
      return arrow::Status::Invalid("aggregate argument array must not be null");
    }
    if (array->length() != length) {
      return arrow::Status::Invalid("aggregate argument array length mismatch");
    }
    agg_arg_arrays[i] = std::move(array);
  }

  if (grouper_ == nullptr) {
    key_types_.clear();
    key_types_.reserve(key_arrays.size());
    for (const auto& arr : key_arrays) {
      if (arr == nullptr || arr->type() == nullptr) {
        return arrow::Status::Invalid("key array/type must not be null");
      }
      key_types_.emplace_back(arr->type().get());
    }

    if (context_->grouper_factory()) {
      ARROW_ASSIGN_OR_RAISE(grouper_, context_->grouper_factory()(key_types_, &exec_context_));
    } else {
      ARROW_ASSIGN_OR_RAISE(grouper_,
                            MakeDefaultGrouper(key_types_, input_schema_, context_->keys(),
                                               &exec_context_));
    }
    if (grouper_ == nullptr) {
      return arrow::Status::Invalid("grouper must not be null");
    }

    aggregates_.clear();
    aggregates_.reserve(context_->aggs().size());
    agg_in_types_.clear();
    agg_in_types_.reserve(context_->aggs().size());

    for (std::size_t i = 0; i < context_->aggs().size(); ++i) {
      ARROW_ASSIGN_OR_RAISE(auto func_name, ToHashAggFunctionName(context_->aggs()[i].func));
      arrow::compute::Aggregate agg;
      agg.function = std::move(func_name);
      agg.name = context_->aggs()[i].name;
      aggregates_.push_back(std::move(agg));

      std::vector<arrow::TypeHolder> in_types;
      if (aggregates_.back().function != "hash_count_all") {
        if (agg_arg_arrays[i] == nullptr || agg_arg_arrays[i]->type() == nullptr) {
          return arrow::Status::Invalid("aggregate argument type must not be null");
        }
        in_types.emplace_back(agg_arg_arrays[i]->type().get());
      }
      agg_in_types_.push_back(std::move(in_types));
    }

    ARROW_ASSIGN_OR_RAISE(agg_kernels_, GetKernels(&exec_context_, aggregates_, agg_in_types_));
    ARROW_ASSIGN_OR_RAISE(agg_states_,
                          InitKernels(agg_kernels_, &exec_context_, aggregates_, agg_in_types_));
  }

  std::vector<arrow::Datum> key_values;
  key_values.reserve(key_arrays.size());
  for (const auto& arr : key_arrays) {
    key_values.emplace_back(arr);
  }
  const arrow::compute::ExecBatch key_batch(std::move(key_values), length);
  const arrow::compute::ExecSpan key_span(key_batch);
  ARROW_ASSIGN_OR_RAISE(auto id_batch, grouper_->Consume(key_span));
  if (!id_batch.is_array()) {
    return arrow::Status::Invalid("expected grouper Consume to return an array datum");
  }

  for (std::size_t i = 0; i < agg_kernels_.size(); ++i) {
    arrow::compute::KernelContext kernel_ctx{&exec_context_};
    kernel_ctx.SetState(agg_states_[i].get());

    std::vector<arrow::Datum> values;
    values.reserve(2);
    if (aggregates_[i].function != "hash_count_all") {
      if (agg_arg_arrays[i] == nullptr) {
        return arrow::Status::Invalid("missing aggregate argument array");
      }
      values.emplace_back(agg_arg_arrays[i]);
    }
    values.emplace_back(id_batch);

    const arrow::compute::ExecBatch agg_batch(std::move(values), length);
    const arrow::compute::ExecSpan agg_span(agg_batch);
    ARROW_RETURN_NOT_OK(agg_kernels_[i]->resize(&kernel_ctx, grouper_->num_groups()));
    ARROW_RETURN_NOT_OK(agg_kernels_[i]->consume(&kernel_ctx, agg_span));
  }

  return arrow::Status::OK();
}

arrow::Result<OperatorStatus> HashAggTransformOp::TransformImpl(
    std::shared_ptr<arrow::RecordBatch>* batch) {
  if (batch == nullptr) {
    return arrow::Status::Invalid("batch must not be null");
  }
  if (engine_ == nullptr) {
    return arrow::Status::Invalid("hash agg engine must not be null");
  }
  if (context_ == nullptr) {
    return arrow::Status::Invalid("hash agg context must not be null");
  }

  if (*batch == nullptr) {
    if (!sealed_ && input_schema_ != nullptr) {
      HashAggContext::PartialState partial;
      partial.input_schema = input_schema_;
      partial.key_types = std::move(key_types_);
      partial.grouper = std::move(grouper_);
      partial.agg_in_types = std::move(agg_in_types_);
      partial.agg_states = std::move(agg_states_);
      ARROW_RETURN_NOT_OK(context_->MergePartial(std::move(partial)));
      agg_kernels_.clear();
      aggregates_.clear();
      compiled_.reset();
      sealed_ = true;
    }
    batch->reset();
    return OperatorStatus::kHasOutput;
  }

  if (sealed_) {
    return arrow::Status::Invalid("hash agg received input after seal");
  }

  ARROW_RETURN_NOT_OK(InitIfNeededAndConsume(**batch));
  batch->reset();
  return OperatorStatus::kNeedInput;
}

arrow::Status HashAggContext::InitIfNeeded(const PartialState& partial) {
  if (engine_ == nullptr) {
    return arrow::Status::Invalid("hash agg engine must not be null");
  }
  if (keys_.empty()) {
    return arrow::Status::NotImplemented("group-by without keys is not implemented");
  }
  if (partial.input_schema == nullptr) {
    return arrow::Status::Invalid("input schema must not be null");
  }
  if (partial.key_types.size() != keys_.size()) {
    return arrow::Status::Invalid("group-by key arity mismatch: keys=", keys_.size(),
                                  " key_types=", partial.key_types.size());
  }
  if (partial.agg_in_types.size() != aggs_.size() || partial.agg_states.size() != aggs_.size()) {
    return arrow::Status::Invalid("aggregate arity mismatch");
  }

  if (input_schema_ == nullptr) {
    input_schema_ = partial.input_schema;
  } else if (!input_schema_->Equals(*partial.input_schema, /*check_metadata=*/true)) {
    return arrow::Status::Invalid("hash agg input schema mismatch");
  }

  if (grouper_ != nullptr) {
    return arrow::Status::OK();
  }

  ARROW_RETURN_NOT_OK(detail::EnsureArrowComputeInitialized());

  if (grouper_factory_) {
    ARROW_ASSIGN_OR_RAISE(grouper_, grouper_factory_(partial.key_types, &exec_context_));
  } else {
    ARROW_ASSIGN_OR_RAISE(grouper_,
                          MakeDefaultGrouper(partial.key_types, input_schema_, keys_,
                                             &exec_context_));
  }
  if (grouper_ == nullptr) {
    return arrow::Status::Invalid("grouper must not be null");
  }

  aggregates_.clear();
  aggregates_.reserve(aggs_.size());
  for (std::size_t i = 0; i < aggs_.size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(auto func_name, ToHashAggFunctionName(aggs_[i].func));
    arrow::compute::Aggregate agg;
    agg.function = std::move(func_name);
    agg.name = aggs_[i].name;
    aggregates_.push_back(std::move(agg));
  }

  agg_in_types_ = partial.agg_in_types;
  ARROW_ASSIGN_OR_RAISE(agg_kernels_, GetKernels(&exec_context_, aggregates_, agg_in_types_));
  ARROW_ASSIGN_OR_RAISE(agg_states_,
                        InitKernels(agg_kernels_, &exec_context_, aggregates_, agg_in_types_));

  std::vector<std::shared_ptr<arrow::Field>> out_fields;
  out_fields.reserve(aggs_.size() + keys_.size());

  for (std::size_t i = 0; i < aggs_.size(); ++i) {
    arrow::compute::KernelContext kernel_ctx{&exec_context_};
    kernel_ctx.SetState(agg_states_[i].get());
    const auto aggr_in_types = ExtendWithGroupIdType(agg_in_types_[i]);
    ARROW_ASSIGN_OR_RAISE(auto type_holder,
                          agg_kernels_[i]->signature->out_type().Resolve(&kernel_ctx,
                                                                         aggr_in_types));
    bool nullable = true;
    if (aggregates_[i].function == "hash_count_all" || aggregates_[i].function == "hash_count") {
      nullable = false;
    } else if (input_schema_ != nullptr) {
      const auto maybe_index = FieldRefIndex(aggs_[i].arg, input_schema_);
      if (maybe_index.has_value()) {
        const int idx = *maybe_index;
        if (idx < 0 || idx >= input_schema_->num_fields()) {
          return arrow::Status::Invalid("aggregate argument field index out of range");
        }
        const auto& in_field = input_schema_->field(idx);
        if (in_field == nullptr) {
          return arrow::Status::Invalid("input field must not be null");
        }
        nullable = in_field->nullable();
      }
    }
    out_fields.push_back(arrow::field(aggs_[i].name, type_holder.GetSharedPtr(), nullable));
  }

  for (std::size_t i = 0; i < keys_.size(); ++i) {
    std::shared_ptr<arrow::Field> field;
    if (input_schema_ != nullptr) {
      const auto maybe_index = FieldRefIndex(keys_[i].expr, input_schema_);
      if (maybe_index.has_value()) {
        const int idx = *maybe_index;
        if (idx < 0 || idx >= input_schema_->num_fields()) {
          return arrow::Status::Invalid("group-by key field index out of range");
        }
        const auto& in_field = input_schema_->field(idx);
        if (in_field == nullptr) {
          return arrow::Status::Invalid("input field must not be null");
        }
        field = in_field->WithName(keys_[i].name);
      }
    }
    if (field == nullptr) {
      auto key_type = partial.key_types[i].GetSharedPtr();
      if (key_type == nullptr) {
        return arrow::Status::Invalid("key type must not be null");
      }
      field = arrow::field(keys_[i].name, std::move(key_type));
    }
    out_fields.push_back(std::move(field));
  }

  output_schema_ = arrow::schema(std::move(out_fields));
  return arrow::Status::OK();
}

arrow::Status HashAggContext::MergePartial(PartialState partial) {
  if (finalized_) {
    return arrow::Status::Invalid("hash agg context is finalized");
  }
  if (partial.input_schema == nullptr) {
    return arrow::Status::Invalid("partial input schema must not be null");
  }
  if (partial.grouper == nullptr) {
    return arrow::Status::Invalid("partial grouper must not be null");
  }
  if (partial.agg_states.size() != aggs_.size()) {
    return arrow::Status::Invalid("partial aggregate state size mismatch");
  }

  ARROW_RETURN_NOT_OK(InitIfNeeded(partial));
  if (grouper_ == nullptr) {
    return arrow::Status::Invalid("grouper must not be null");
  }

  const int64_t partial_groups = static_cast<int64_t>(partial.grouper->num_groups());
  if (partial_groups < 0) {
    return arrow::Status::Invalid("negative group count");
  }
  if (partial_groups > static_cast<int64_t>(std::numeric_limits<int32_t>::max())) {
    return arrow::Status::NotImplemented("too many groups for merge");
  }

  ARROW_ASSIGN_OR_RAISE(auto uniques, partial.grouper->GetUniques());
  if (uniques.length != partial_groups) {
    return arrow::Status::Invalid("unique key batch length mismatch");
  }
  if (static_cast<std::size_t>(uniques.values.size()) != keys_.size()) {
    return arrow::Status::Invalid("unique key batch column count mismatch");
  }

  const arrow::compute::ExecSpan unique_span(uniques);
  ARROW_ASSIGN_OR_RAISE(auto id_batch, grouper_->Consume(unique_span));
  if (!id_batch.is_array()) {
    return arrow::Status::Invalid("expected grouper Consume to return an array datum");
  }
  const auto& mapping = id_batch.array();
  if (mapping == nullptr) {
    return arrow::Status::Invalid("group id mapping array must not be null");
  }
  if (mapping->length != partial_groups) {
    return arrow::Status::Invalid("group id mapping length mismatch");
  }

  for (std::size_t i = 0; i < aggs_.size(); ++i) {
    if (agg_kernels_[i] == nullptr || agg_states_[i] == nullptr) {
      return arrow::Status::Invalid("aggregate kernel/state must not be null");
    }
    if (partial.agg_states[i] == nullptr) {
      return arrow::Status::Invalid("partial aggregate state must not be null");
    }

    arrow::compute::KernelContext kernel_ctx{&exec_context_};
    kernel_ctx.SetState(agg_states_[i].get());
    ARROW_RETURN_NOT_OK(agg_kernels_[i]->resize(&kernel_ctx, grouper_->num_groups()));

    auto other = std::move(partial.agg_states[i]);
    ARROW_RETURN_NOT_OK(agg_kernels_[i]->merge(&kernel_ctx, std::move(*other), *mapping));
  }

  return arrow::Status::OK();
}

arrow::Status HashAggContext::Finalize() {
  if (finalized_) {
    return arrow::Status::OK();
  }

  if (input_schema_ == nullptr) {
    finalized_ = true;
    return arrow::Status::OK();
  }
  if (output_schema_ == nullptr) {
    return arrow::Status::Invalid("hash agg output schema must not be null");
  }
  if (grouper_ == nullptr) {
    return arrow::Status::Invalid("hash agg grouper must not be null");
  }
  if (agg_kernels_.size() != aggs_.size() || agg_states_.size() != aggs_.size()) {
    return arrow::Status::Invalid("hash agg kernel/state size mismatch");
  }

  const int64_t num_groups = static_cast<int64_t>(grouper_->num_groups());
  if (num_groups < 0) {
    return arrow::Status::Invalid("negative group count");
  }
  if (num_groups > static_cast<int64_t>(std::numeric_limits<int32_t>::max())) {
    return arrow::Status::NotImplemented("too many groups for RecordBatch output");
  }

  ARROW_ASSIGN_OR_RAISE(auto uniques, grouper_->GetUniques());
  if (uniques.length != num_groups) {
    return arrow::Status::Invalid("unique key batch length mismatch");
  }
  if (static_cast<std::size_t>(uniques.values.size()) != keys_.size()) {
    return arrow::Status::Invalid("unique key batch column count mismatch");
  }

  std::vector<std::shared_ptr<arrow::Array>> out_columns;
  out_columns.reserve(aggs_.size() + keys_.size());

  for (std::size_t i = 0; i < aggs_.size(); ++i) {
    arrow::compute::KernelContext kernel_ctx{&exec_context_};
    kernel_ctx.SetState(agg_states_[i].get());
    arrow::Datum out;
    ARROW_RETURN_NOT_OK(agg_kernels_[i]->finalize(&kernel_ctx, &out));
    ARROW_ASSIGN_OR_RAISE(auto out_array,
                          detail::DatumToArray(out, num_groups, exec_context_.memory_pool()));
    out_columns.push_back(std::move(out_array));
    agg_states_[i].reset();
  }

  for (std::size_t i = 0; i < keys_.size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(auto out_array,
                          detail::DatumToArray(uniques.values[i], num_groups,
                                               exec_context_.memory_pool()));
    out_columns.push_back(std::move(out_array));
  }

  output_batch_ = arrow::RecordBatch::Make(output_schema_, num_groups, std::move(out_columns));

  grouper_.reset();
  agg_kernels_.clear();
  agg_states_.clear();
  aggregates_.clear();
  agg_in_types_.clear();

  output_offset_ = 0;
  output_started_ = false;
  finalized_ = true;
  return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> HashAggContext::NextOutputBatch(
    int64_t max_rows) {
  if (max_rows <= 0) {
    return arrow::Status::Invalid("max_rows must be positive");
  }
  if (output_batch_ == nullptr) {
    return std::shared_ptr<arrow::RecordBatch>();
  }
  if (output_schema_ == nullptr) {
    return arrow::Status::Invalid("hash agg output schema must not be null");
  }

  if (output_batch_->num_rows() == 0) {
    if (output_started_) {
      return std::shared_ptr<arrow::RecordBatch>();
    }
    output_started_ = true;
    return output_batch_;
  }

  if (output_offset_ >= output_batch_->num_rows()) {
    return std::shared_ptr<arrow::RecordBatch>();
  }

  const int64_t remaining = output_batch_->num_rows() - output_offset_;
  const int64_t length = std::min(remaining, max_rows);
  auto out = output_batch_->Slice(output_offset_, length);
  output_offset_ += length;
  output_started_ = true;
  return out;
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> HashAggContext::ReadNextOutputBatch(
    int64_t max_rows) {
  ARROW_RETURN_NOT_OK(Finalize());
  ARROW_ASSIGN_OR_RAISE(auto out, NextOutputBatch(max_rows));
  if (out == nullptr) {
    output_batch_.reset();
  }
  return out;
}

HashAggMergeSinkOp::HashAggMergeSinkOp(std::shared_ptr<HashAggContext> context)
    : context_(std::move(context)) {}

arrow::Result<OperatorStatus> HashAggMergeSinkOp::WriteImpl(
    std::shared_ptr<arrow::RecordBatch> batch) {
  if (context_ == nullptr) {
    return arrow::Status::Invalid("hash agg context must not be null");
  }
  if (batch != nullptr) {
    return arrow::Status::Invalid("hash agg merge sink expects EOS only");
  }
  ARROW_RETURN_NOT_OK(context_->Finalize());
  return OperatorStatus::kFinished;
}

HashAggResultSourceOp::HashAggResultSourceOp(std::shared_ptr<HashAggContext> context,
                                             int64_t max_output_rows)
    : context_(std::move(context)), max_output_rows_(max_output_rows) {}

arrow::Result<OperatorStatus> HashAggResultSourceOp::ReadImpl(
    std::shared_ptr<arrow::RecordBatch>* batch) {
  if (batch == nullptr) {
    return arrow::Status::Invalid("batch output must not be null");
  }
  if (context_ == nullptr) {
    return arrow::Status::Invalid("hash agg context must not be null");
  }
  ARROW_ASSIGN_OR_RAISE(*batch, context_->ReadNextOutputBatch(max_output_rows_));
  if (*batch == nullptr) {
    return OperatorStatus::kFinished;
  }
  return OperatorStatus::kHasOutput;
}

}  // namespace tiforth
