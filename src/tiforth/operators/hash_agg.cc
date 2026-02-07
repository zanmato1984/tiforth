// Copyright 2026 TiForth Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
#include <arrow/compute/api_aggregate.h>
#include <arrow/compute/exec.h>
#include <arrow/compute/function.h>
#include <arrow/compute/kernel.h>
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

namespace tiforth::op {

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
    return arrow::Status::Invalid("aggregate function count mismatch");
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
  if (kernels.size() != aggregates.size() || aggregates.size() != in_types.size()) {
    return arrow::Status::Invalid("aggregate kernel/state count mismatch");
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
  if (key_types.size() == 1 && keys.size() == 1) {
    if (const auto maybe_idx = FieldRefIndex(keys[0].expr, input_schema); maybe_idx.has_value()) {
      const int idx = *maybe_idx;
      int32_t collation_id = 63;
      if (input_schema != nullptr && idx >= 0 && idx < input_schema->num_fields()) {
        if (const auto& field = input_schema->field(idx); field != nullptr) {
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
      if (key_type->id() == arrow::Type::STRING || key_type->id() == arrow::Type::BINARY) {
        return std::make_unique<detail::SmallStringSingleKeyGrouper>(std::move(key_type),
                                                                     exec_context);
      }
    }
  }
  return arrow::compute::Grouper::Make(key_types, exec_context);
}

}  // namespace

struct HashAggState::Impl {
  struct Compiled {
    std::vector<CompiledExpr> keys;
    std::vector<std::optional<CompiledExpr>> agg_args;
  };

  struct ThreadLocal {
    std::unique_ptr<arrow::compute::Grouper> grouper;
    std::vector<std::unique_ptr<arrow::compute::KernelState>> agg_states;
  };

  Impl(const Engine* engine, std::vector<AggKey> keys, std::vector<AggFunc> aggs,
       GrouperFactory grouper_factory, arrow::MemoryPool* memory_pool, std::size_t dop);

  const Engine* engine() const { return engine_; }
  const std::vector<AggKey>& keys() const { return keys_; }
  const std::vector<AggFunc>& aggs() const { return aggs_; }
  const GrouperFactory& grouper_factory() const { return grouper_factory_; }
  arrow::MemoryPool* memory_pool() const { return exec_context_.memory_pool(); }

  arrow::Status Consume(ThreadId thread_id, const arrow::RecordBatch& batch);
  arrow::Status MergeAndFinalize();
  arrow::Result<std::shared_ptr<arrow::RecordBatch>> OutputBatch();

 private:
  arrow::Status InitIfNeeded(const arrow::RecordBatch& batch);
  arrow::Status ConsumeBatch(ThreadId thread_id, const arrow::RecordBatch& batch);
  arrow::Status Merge();
  arrow::Status Finalize();

  const Engine* engine_ = nullptr;
  std::vector<AggKey> keys_;
  std::vector<AggFunc> aggs_;
  GrouperFactory grouper_factory_;

  std::shared_ptr<arrow::Schema> input_schema_;
  arrow::compute::ExecContext exec_context_;
  std::size_t dop_ = 1;

  std::unique_ptr<Compiled> compiled_;

  std::vector<arrow::TypeHolder> key_types_;
  std::vector<arrow::compute::Aggregate> aggregates_;
  std::vector<std::vector<arrow::TypeHolder>> agg_in_types_;
  std::vector<const arrow::compute::HashAggregateKernel*> agg_kernels_;

  std::vector<ThreadLocal> thread_locals_;

  std::shared_ptr<arrow::Schema> output_schema_;
  std::shared_ptr<arrow::RecordBatch> output_batch_;
  bool finalized_ = false;
};

HashAggState::Impl::Impl(const Engine* engine, std::vector<AggKey> keys, std::vector<AggFunc> aggs,
                         GrouperFactory grouper_factory, arrow::MemoryPool* memory_pool,
                         std::size_t dop)
    : engine_(engine),
      keys_(std::move(keys)),
      aggs_(std::move(aggs)),
      grouper_factory_(std::move(grouper_factory)),
      exec_context_(memory_pool != nullptr
                        ? memory_pool
                        : (engine != nullptr ? engine->memory_pool() : arrow::default_memory_pool()),
                    /*executor=*/nullptr,
                    engine != nullptr ? engine->function_registry() : nullptr),
      dop_(dop == 0 ? 1 : dop),
      thread_locals_(dop_ == 0 ? 1 : dop_) {}

HashAggState::HashAggState(const Engine* engine, std::vector<AggKey> keys, std::vector<AggFunc> aggs,
                           GrouperFactory grouper_factory, arrow::MemoryPool* memory_pool,
                           std::size_t dop)
    : impl_(std::make_unique<Impl>(engine, std::move(keys), std::move(aggs),
                                   std::move(grouper_factory), memory_pool, dop)) {}

HashAggState::~HashAggState() = default;

arrow::Status HashAggState::Impl::InitIfNeeded(const arrow::RecordBatch& batch) {
  if (engine_ == nullptr) {
    return arrow::Status::Invalid("hash agg engine must not be null");
  }
  if (keys_.empty()) {
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

  if (compiled_ != nullptr) {
    return arrow::Status::OK();
  }

  auto compiled = std::make_unique<Compiled>();
  compiled->keys.reserve(keys_.size());
  for (const auto& key : keys_) {
    if (key.name.empty()) {
      return arrow::Status::Invalid("group-by key name must not be empty");
    }
    if (key.expr == nullptr) {
      return arrow::Status::Invalid("group-by key expr must not be null");
    }
    ARROW_ASSIGN_OR_RAISE(auto compiled_key,
                          CompileExpr(input_schema_, *key.expr, engine_, &exec_context_));
    compiled->keys.push_back(std::move(compiled_key));
  }

  compiled->agg_args.resize(aggs_.size());
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
      continue;
    }
    if (agg.arg == nullptr) {
      return arrow::Status::Invalid("aggregate argument expr must not be null");
    }
    ARROW_ASSIGN_OR_RAISE(auto compiled_arg,
                          CompileExpr(input_schema_, *agg.arg, engine_, &exec_context_));
    compiled->agg_args[i] = std::move(compiled_arg);
  }

  compiled_ = std::move(compiled);
  return arrow::Status::OK();
}

arrow::Status HashAggState::Impl::Consume(ThreadId thread_id, const arrow::RecordBatch& batch) {
  if (finalized_) {
    return arrow::Status::Invalid("hash agg state is finalized");
  }
  if (thread_id >= dop_) {
    return arrow::Status::Invalid("hash agg thread id out of range");
  }
  ARROW_RETURN_NOT_OK(InitIfNeeded(batch));
  return ConsumeBatch(thread_id, batch);
}

arrow::Status HashAggState::Impl::ConsumeBatch(ThreadId thread_id, const arrow::RecordBatch& batch) {
  if (compiled_ == nullptr) {
    return arrow::Status::Invalid("hash agg is missing compiled expressions");
  }
  if (batch.schema() == nullptr) {
    return arrow::Status::Invalid("input batch schema must not be null");
  }
  if (static_cast<std::size_t>(batch.num_columns()) !=
      static_cast<std::size_t>(batch.schema()->num_fields())) {
    return arrow::Status::Invalid("input batch schema mismatch");
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
  agg_arg_arrays.resize(aggs_.size());
  for (std::size_t i = 0; i < aggs_.size(); ++i) {
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

  if (key_types_.empty()) {
    key_types_.reserve(key_arrays.size());
    for (const auto& arr : key_arrays) {
      if (arr == nullptr || arr->type() == nullptr) {
        return arrow::Status::Invalid("key array/type must not be null");
      }
      key_types_.emplace_back(arr->type().get());
    }

    aggregates_.clear();
    aggregates_.reserve(aggs_.size());
    agg_in_types_.clear();
    agg_in_types_.reserve(aggs_.size());
    for (std::size_t i = 0; i < aggs_.size(); ++i) {
      const auto& agg = aggs_[i];
      ARROW_ASSIGN_OR_RAISE(auto func_name, ToHashAggFunctionName(agg.func));
      if (func_name == "hash_count_all") {
        aggregates_.emplace_back(std::move(func_name), agg.name);
        agg_in_types_.push_back({});
        continue;
      }
      if (agg_arg_arrays[i] == nullptr || agg_arg_arrays[i]->type() == nullptr) {
        return arrow::Status::Invalid("aggregate argument type must not be null");
      }
      aggregates_.emplace_back(std::move(func_name), agg.name);
      agg_in_types_.push_back({arrow::TypeHolder(agg_arg_arrays[i]->type().get())});
    }

    ARROW_ASSIGN_OR_RAISE(agg_kernels_, GetKernels(&exec_context_, aggregates_, agg_in_types_));

    for (std::size_t tid = 0; tid < dop_; ++tid) {
      auto* tl = &thread_locals_[tid];
      if (grouper_factory_) {
        ARROW_ASSIGN_OR_RAISE(tl->grouper, grouper_factory_(key_types_, &exec_context_));
      } else {
        ARROW_ASSIGN_OR_RAISE(tl->grouper,
                              MakeDefaultGrouper(key_types_, input_schema_, keys_, &exec_context_));
      }
      if (tl->grouper == nullptr) {
        return arrow::Status::Invalid("grouper factory returned null");
      }
      ARROW_ASSIGN_OR_RAISE(tl->agg_states,
                            InitKernels(agg_kernels_, &exec_context_, aggregates_, agg_in_types_));
      if (tl->agg_states.size() != agg_kernels_.size()) {
        return arrow::Status::Invalid("aggregate state size mismatch");
      }
    }

    // Build output schema (aggregates first, then keys) to match TiForth/TiFlash expectations.
    std::vector<std::shared_ptr<arrow::Field>> out_fields;
    out_fields.reserve(aggs_.size() + keys_.size());

    auto* tl0 = &thread_locals_[0];
    if (tl0->agg_states.size() != aggs_.size()) {
      return arrow::Status::Invalid("aggregate state size mismatch");
    }

    for (std::size_t i = 0; i < aggs_.size(); ++i) {
      arrow::compute::KernelContext kernel_ctx{&exec_context_};
      kernel_ctx.SetState(tl0->agg_states[i].get());
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
        auto key_type = key_types_[i].GetSharedPtr();
        if (key_type == nullptr) {
          return arrow::Status::Invalid("key type must not be null");
        }
        field = arrow::field(keys_[i].name, std::move(key_type));
      }
      out_fields.push_back(std::move(field));
    }

    output_schema_ = arrow::schema(std::move(out_fields));
  }

  if (thread_id >= thread_locals_.size()) {
    return arrow::Status::Invalid("hash agg thread local index out of range");
  }
  auto* tl = &thread_locals_[thread_id];
  if (tl->grouper == nullptr) {
    return arrow::Status::Invalid("grouper must not be null");
  }
  if (tl->agg_states.size() != agg_kernels_.size()) {
    return arrow::Status::Invalid("aggregate state size mismatch");
  }

  std::vector<arrow::compute::ExecValue> keys_span_values;
  keys_span_values.reserve(key_arrays.size());
  for (const auto& array : key_arrays) {
    if (array == nullptr || array->data() == nullptr) {
      return arrow::Status::Invalid("key array data must not be null");
    }
    keys_span_values.emplace_back(*array->data());
  }
  arrow::compute::ExecSpan key_span(std::move(keys_span_values), length);
  ARROW_ASSIGN_OR_RAISE(auto id_batch, tl->grouper->Consume(key_span));
  if (!id_batch.is_array()) {
    return arrow::Status::Invalid("expected grouper Consume to return an array datum");
  }
  const auto& group_ids = id_batch.array();
  if (group_ids == nullptr) {
    return arrow::Status::Invalid("group id mapping array must not be null");
  }

  for (std::size_t i = 0; i < agg_kernels_.size(); ++i) {
    if (agg_kernels_[i] == nullptr || tl->agg_states[i] == nullptr) {
      return arrow::Status::Invalid("aggregate kernel/state must not be null");
    }

    arrow::compute::KernelContext kernel_ctx{&exec_context_};
    kernel_ctx.SetState(tl->agg_states[i].get());

    std::vector<arrow::compute::ExecValue> column_values;
    if (i < agg_arg_arrays.size() && agg_arg_arrays[i] != nullptr) {
      if (agg_arg_arrays[i]->data() == nullptr) {
        return arrow::Status::Invalid("aggregate argument array data must not be null");
      }
      column_values.emplace_back(*agg_arg_arrays[i]->data());
    }
    column_values.emplace_back(*group_ids);
    arrow::compute::ExecSpan agg_span(std::move(column_values), length);

    ARROW_RETURN_NOT_OK(agg_kernels_[i]->resize(&kernel_ctx, tl->grouper->num_groups()));
    ARROW_RETURN_NOT_OK(agg_kernels_[i]->consume(&kernel_ctx, agg_span));
  }

  return arrow::Status::OK();
}

arrow::Status HashAggState::Impl::Merge() {
  if (thread_locals_.empty()) {
    return arrow::Status::OK();
  }
  auto* state0 = &thread_locals_[0];
  if (state0->grouper == nullptr) {
    // No input has been consumed.
    return arrow::Status::OK();
  }

  for (std::size_t tid = 1; tid < thread_locals_.size(); ++tid) {
    auto* other = &thread_locals_[tid];
    if (other->grouper == nullptr) {
      continue;
    }

    ARROW_ASSIGN_OR_RAISE(auto other_keys, other->grouper->GetUniques());
    ARROW_ASSIGN_OR_RAISE(auto transposition,
                          state0->grouper->Consume(arrow::compute::ExecSpan(other_keys)));
    if (!transposition.is_array()) {
      return arrow::Status::Invalid("expected transposition to be an array datum");
    }

    for (std::size_t i = 0; i < agg_kernels_.size(); ++i) {
      if (agg_kernels_[i] == nullptr) {
        return arrow::Status::Invalid("aggregate kernel must not be null");
      }
      if (state0->agg_states.size() != agg_kernels_.size() ||
          other->agg_states.size() != agg_kernels_.size()) {
        return arrow::Status::Invalid("aggregate state size mismatch");
      }
      if (state0->agg_states[i] == nullptr || other->agg_states[i] == nullptr) {
        return arrow::Status::Invalid("aggregate state must not be null");
      }

      arrow::compute::KernelContext kernel_ctx{&exec_context_};
      kernel_ctx.SetState(state0->agg_states[i].get());
      ARROW_RETURN_NOT_OK(agg_kernels_[i]->resize(&kernel_ctx, state0->grouper->num_groups()));

      auto other_state = std::move(other->agg_states[i]);
      ARROW_RETURN_NOT_OK(
          agg_kernels_[i]->merge(&kernel_ctx, std::move(*other_state), *transposition.array()));
      other_state.reset();
    }

    other->grouper.reset();
    other->agg_states.clear();
  }
  return arrow::Status::OK();
}

arrow::Status HashAggState::Impl::Finalize() {
  if (finalized_) {
    return arrow::Status::OK();
  }

  if (output_schema_ == nullptr) {
    // No input has been consumed.
    finalized_ = true;
    output_batch_.reset();
    return arrow::Status::OK();
  }

  if (thread_locals_.empty() || thread_locals_[0].grouper == nullptr) {
    return arrow::Status::Invalid("grouper must not be null");
  }

  auto* state = &thread_locals_[0];

  const int64_t num_groups = static_cast<int64_t>(state->grouper->num_groups());
  if (num_groups < 0) {
    return arrow::Status::Invalid("negative group count");
  }
  if (num_groups > static_cast<int64_t>(std::numeric_limits<int32_t>::max())) {
    return arrow::Status::NotImplemented("too many groups for finalize");
  }

  ARROW_ASSIGN_OR_RAISE(auto uniques, state->grouper->GetUniques());
  if (uniques.length != num_groups) {
    return arrow::Status::Invalid("unique key batch length mismatch");
  }

  std::vector<std::shared_ptr<arrow::Array>> out_columns;
  out_columns.reserve(output_schema_->fields().size());

  for (std::size_t i = 0; i < agg_kernels_.size(); ++i) {
    if (agg_kernels_[i] == nullptr || state->agg_states[i] == nullptr) {
      return arrow::Status::Invalid("aggregate kernel/state must not be null");
    }

    arrow::compute::KernelContext kernel_ctx{&exec_context_};
    kernel_ctx.SetState(state->agg_states[i].get());
    arrow::Datum out;
    ARROW_RETURN_NOT_OK(agg_kernels_[i]->finalize(&kernel_ctx, &out));
    ARROW_ASSIGN_OR_RAISE(auto out_array,
                          detail::DatumToArray(out, num_groups, exec_context_.memory_pool()));
    out_columns.push_back(std::move(out_array));
    state->agg_states[i].reset();
  }

  for (std::size_t i = 0; i < keys_.size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(auto out_array,
                          detail::DatumToArray(uniques.values[i], num_groups,
                                               exec_context_.memory_pool()));
    out_columns.push_back(std::move(out_array));
  }

  output_batch_ = arrow::RecordBatch::Make(output_schema_, num_groups, std::move(out_columns));

  state->grouper.reset();
  agg_kernels_.clear();
  aggregates_.clear();
  agg_in_types_.clear();

  finalized_ = true;
  return arrow::Status::OK();
}

arrow::Status HashAggState::Impl::MergeAndFinalize() {
  if (finalized_) {
    return arrow::Status::OK();
  }
  ARROW_RETURN_NOT_OK(Merge());
  return Finalize();
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> HashAggState::Impl::OutputBatch() {
  ARROW_RETURN_NOT_OK(MergeAndFinalize());
  return output_batch_;
}

const Engine* HashAggState::engine() const {
  return impl_ != nullptr ? impl_->engine() : nullptr;
}

const std::vector<AggKey>& HashAggState::keys() const {
  static const std::vector<AggKey> kEmpty;
  return impl_ != nullptr ? impl_->keys() : kEmpty;
}

const std::vector<AggFunc>& HashAggState::aggs() const {
  static const std::vector<AggFunc> kEmpty;
  return impl_ != nullptr ? impl_->aggs() : kEmpty;
}

const HashAggState::GrouperFactory& HashAggState::grouper_factory() const {
  static const GrouperFactory kEmpty{};
  return impl_ != nullptr ? impl_->grouper_factory() : kEmpty;
}

arrow::MemoryPool* HashAggState::memory_pool() const {
  return impl_ != nullptr ? impl_->memory_pool() : nullptr;
}

arrow::Status HashAggState::Consume(ThreadId thread_id, const arrow::RecordBatch& batch) {
  if (impl_ == nullptr) {
    return arrow::Status::Invalid("hash agg state implementation must not be null");
  }
  return impl_->Consume(thread_id, batch);
}

arrow::Status HashAggState::MergeAndFinalize() {
  if (impl_ == nullptr) {
    return arrow::Status::Invalid("hash agg state implementation must not be null");
  }
  return impl_->MergeAndFinalize();
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> HashAggState::OutputBatch() {
  if (impl_ == nullptr) {
    return arrow::Status::Invalid("hash agg state implementation must not be null");
  }
  return impl_->OutputBatch();
}

HashAggSinkOp::HashAggSinkOp(std::shared_ptr<HashAggState> state) : state_(std::move(state)) {}

PipelineSink HashAggSinkOp::Sink() {
  return [this](const TaskContext&, ThreadId thread_id,
                std::optional<Batch> input) -> OpResult {
    if (state_ == nullptr) {
      return arrow::Status::Invalid("hash agg state must not be null");
    }
    if (!input.has_value()) {
      return OpOutput::PipeSinkNeedsMore();
    }
    auto batch = std::move(*input);
    if (batch == nullptr) {
      return arrow::Status::Invalid("hash agg input batch must not be null");
    }
    ARROW_RETURN_NOT_OK(state_->Consume(thread_id, *batch));
    return OpOutput::PipeSinkNeedsMore();
  };
}

TaskGroups HashAggSinkOp::Frontend() {
  Task merge_finalize{
      "HashAggMergeFinalize",
      [state = state_](const TaskContext&, TaskId) -> TaskResult {
        if (state == nullptr) {
          return arrow::Status::Invalid("hash agg state must not be null");
        }
        ARROW_RETURN_NOT_OK(state->MergeAndFinalize());
        return TaskStatus::Finished();
      }};
  TaskGroups groups;
  groups.emplace_back("HashAggMergeFinalize", std::move(merge_finalize), /*num_tasks=*/1);
  return groups;
}

std::optional<TaskGroup> HashAggSinkOp::Backend() { return std::nullopt; }

std::unique_ptr<SourceOp> HashAggSinkOp::ImplicitSource() {
  return std::make_unique<HashAggResultSourceOp>(state_);
}

HashAggResultSourceOp::HashAggResultSourceOp(std::shared_ptr<HashAggState> state, int64_t max_output_rows)
    : state_(std::move(state)),
      start_row_(0),
      end_row_(-1),
      next_row_(0),
      max_output_rows_(max_output_rows) {}

HashAggResultSourceOp::HashAggResultSourceOp(std::shared_ptr<HashAggState> state, int64_t start_row,
                                             int64_t end_row, int64_t max_output_rows)
    : state_(std::move(state)),
      start_row_(start_row),
      end_row_(end_row),
      next_row_(start_row),
      max_output_rows_(max_output_rows) {}

PipelineSource HashAggResultSourceOp::Source() {
  return [this](const TaskContext&, ThreadId) -> OpResult {
    if (state_ == nullptr) {
      return arrow::Status::Invalid("hash agg state must not be null");
    }
    if (max_output_rows_ <= 0) {
      return arrow::Status::Invalid("max_output_rows must be positive");
    }
    if (start_row_ < 0) {
      return arrow::Status::Invalid("start_row must be non-negative");
    }
    if (end_row_ < -1) {
      return arrow::Status::Invalid("end_row must be -1 or non-negative");
    }
    if (end_row_ >= 0 && end_row_ < start_row_) {
      return arrow::Status::Invalid("end_row must be >= start_row");
    }

    ARROW_ASSIGN_OR_RAISE(auto output, state_->OutputBatch());
    if (output == nullptr) {
      return OpOutput::Finished();
    }

    const int64_t total_rows = output->num_rows();
    if (total_rows < 0) {
      return arrow::Status::Invalid("negative output row count");
    }

    if (total_rows == 0) {
      if (emitted_empty_) {
        return OpOutput::Finished();
      }
      emitted_empty_ = true;
      return OpOutput::SourcePipeHasMore(std::move(output));
    }

    const int64_t effective_end =
        (end_row_ < 0 || end_row_ > total_rows) ? total_rows : end_row_;
    if (next_row_ >= effective_end || next_row_ >= total_rows) {
      return OpOutput::Finished();
    }

    const int64_t remaining = effective_end - next_row_;
    const int64_t length = std::min(max_output_rows_, remaining);
    if (length <= 0) {
      return OpOutput::Finished();
    }

    auto slice = output->Slice(next_row_, length);
    next_row_ += length;
    return OpOutput::SourcePipeHasMore(std::move(slice));
  };
}

TaskGroups HashAggResultSourceOp::Frontend() { return {}; }

std::optional<TaskGroup> HashAggResultSourceOp::Backend() { return std::nullopt; }

}  // namespace tiforth::op
