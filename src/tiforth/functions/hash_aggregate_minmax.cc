#include <algorithm>
#include <cstddef>
#include <cstring>
#include <memory>
#include <vector>

#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/compute/api_aggregate.h>
#include <arrow/compute/function.h>
#include <arrow/compute/kernel.h>
#include <arrow/compute/registry.h>
#include <arrow/result.h>
#include <arrow/stl_allocator.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/util/bit_util.h>

#include "tiforth/functions/hash_aggregate_common.h"

namespace tiforth::function {

namespace {

template <typename ValueCType>
struct HashMinMaxState final : public arrow::compute::KernelState {
  HashMinMaxState(const arrow::compute::ScalarAggregateOptions& options, arrow::MemoryPool* pool)
      : options(options),
        alloc_u8(pool),
        alloc_value(pool),
        alloc_count(pool),
        seen_null(alloc_u8),
        has_value(alloc_u8),
        values(alloc_value),
        counts(alloc_count) {}

  arrow::compute::ScalarAggregateOptions options;
  arrow::stl::allocator<uint8_t> alloc_u8;
  arrow::stl::allocator<ValueCType> alloc_value;
  arrow::stl::allocator<int64_t> alloc_count;

  std::vector<uint8_t, arrow::stl::allocator<uint8_t>> seen_null;
  std::vector<uint8_t, arrow::stl::allocator<uint8_t>> has_value;
  std::vector<ValueCType, arrow::stl::allocator<ValueCType>> values;
  std::vector<int64_t, arrow::stl::allocator<int64_t>> counts;
  int64_t num_groups = 0;
};

template <typename ValueCType>
arrow::Result<std::unique_ptr<arrow::compute::KernelState>> HashMinMaxInit(
    arrow::compute::KernelContext* ctx, const arrow::compute::KernelInitArgs& args) {
  if (ctx == nullptr) {
    return arrow::Status::Invalid("kernel context must not be null");
  }
  const auto* options = static_cast<const arrow::compute::ScalarAggregateOptions*>(args.options);
  if (options == nullptr) {
    return arrow::Status::Invalid("ScalarAggregateOptions must not be null");
  }
  return std::make_unique<HashMinMaxState<ValueCType>>(*options, ctx->memory_pool());
}

template <typename ValueCType>
arrow::Status HashMinMaxResize(arrow::compute::KernelContext* ctx, int64_t new_num_groups) {
  if (ctx == nullptr) {
    return arrow::Status::Invalid("kernel context must not be null");
  }
  auto* state = static_cast<HashMinMaxState<ValueCType>*>(ctx->state());
  if (state == nullptr) {
    return arrow::Status::Invalid("hash_min/max state must not be null");
  }
  if (new_num_groups < 0) {
    return arrow::Status::Invalid("negative group count");
  }
  if (new_num_groups > state->num_groups) {
    const std::size_t n = static_cast<std::size_t>(new_num_groups);
    state->seen_null.resize(n, 0);
    state->has_value.resize(n, 0);
    state->values.resize(n, ValueCType{});
    state->counts.resize(n, 0);
    state->num_groups = new_num_groups;
  }
  return arrow::Status::OK();
}

template <typename ValueCType, bool kIsMin>
arrow::Status HashMinMaxConsumeImpl(HashMinMaxState<ValueCType>* state,
                                   const arrow::ArraySpan& values, const uint32_t* group_ids,
                                   int64_t length) {
  if (state == nullptr) {
    return arrow::Status::Invalid("hash_min/max state must not be null");
  }
  if (group_ids == nullptr) {
    return arrow::Status::Invalid("group id buffer must not be null");
  }

  const auto* data = values.GetValues<ValueCType>(1);
  if (data == nullptr) {
    return arrow::Status::Invalid("value buffer must not be null");
  }

  for (int64_t i = 0; i < length; ++i) {
    const auto g = group_ids[i];
    if (values.IsValid(i)) {
      if (state->has_value[g] == 0) {
        state->values[g] = data[i];
        state->has_value[g] = 1;
      } else if constexpr (kIsMin) {
        if (data[i] < state->values[g]) {
          state->values[g] = data[i];
        }
      } else {
        if (data[i] > state->values[g]) {
          state->values[g] = data[i];
        }
      }
      state->counts[g] += 1;
      continue;
    }
    if (!state->options.skip_nulls) {
      state->seen_null[g] = 1;
    }
  }

  return arrow::Status::OK();
}

template <typename ValueCType, bool kIsMin>
arrow::Status HashMinMaxConsume(arrow::compute::KernelContext* ctx,
                               const arrow::compute::ExecSpan& batch) {
  if (ctx == nullptr) {
    return arrow::Status::Invalid("kernel context must not be null");
  }
  if (batch.num_values() != 2) {
    return arrow::Status::Invalid("hash_min/max expects 2 arguments (value, group_id)");
  }
  if (!batch[0].is_array() || !batch[1].is_array()) {
    return arrow::Status::Invalid("hash_min/max expects array arguments");
  }

  auto* state = static_cast<HashMinMaxState<ValueCType>*>(ctx->state());
  if (state == nullptr) {
    return arrow::Status::Invalid("hash_min/max state must not be null");
  }

  const auto& values = batch[0].array;
  const auto& group_ids = batch[1].array;
  const auto* g = group_ids.GetValues<uint32_t>(1);
  if (g == nullptr) {
    return arrow::Status::Invalid("group id buffer must not be null");
  }
  return HashMinMaxConsumeImpl<ValueCType, kIsMin>(state, values, g, batch.length);
}

template <typename ValueCType, bool kIsMin>
arrow::Status HashMinMaxMergeImpl(HashMinMaxState<ValueCType>* state,
                                 HashMinMaxState<ValueCType>* other_state,
                                 const arrow::ArrayData& group_id_mapping) {
  if (state == nullptr || other_state == nullptr) {
    return arrow::Status::Invalid("hash_min/max merge state must not be null");
  }
  if (group_id_mapping.type == nullptr || group_id_mapping.type->id() != arrow::Type::UINT32) {
    return arrow::Status::Invalid("hash_min/max group id mapping must be uint32");
  }
  const auto* g = group_id_mapping.GetValues<uint32_t>(1);
  if (g == nullptr) {
    return arrow::Status::Invalid("hash_min/max group id mapping buffer must not be null");
  }
  if (group_id_mapping.length != static_cast<int64_t>(other_state->counts.size())) {
    return arrow::Status::Invalid("hash_min/max group id mapping length mismatch");
  }

  for (int64_t other_g = 0; other_g < group_id_mapping.length; ++other_g) {
    const auto out_g = g[other_g];
    state->seen_null[out_g] |= other_state->seen_null[static_cast<std::size_t>(other_g)];
    if (other_state->has_value[static_cast<std::size_t>(other_g)] != 0) {
      if (state->has_value[out_g] == 0) {
        state->has_value[out_g] = 1;
        state->values[out_g] = other_state->values[static_cast<std::size_t>(other_g)];
      } else if constexpr (kIsMin) {
        const auto v = other_state->values[static_cast<std::size_t>(other_g)];
        if (v < state->values[out_g]) {
          state->values[out_g] = v;
        }
      } else {
        const auto v = other_state->values[static_cast<std::size_t>(other_g)];
        if (v > state->values[out_g]) {
          state->values[out_g] = v;
        }
      }
    }
    state->counts[out_g] += other_state->counts[static_cast<std::size_t>(other_g)];
  }
  return arrow::Status::OK();
}

template <typename ValueCType, bool kIsMin>
arrow::Status HashMinMaxMerge(arrow::compute::KernelContext* ctx, arrow::compute::KernelState&& other,
                             const arrow::ArrayData& group_id_mapping) {
  if (ctx == nullptr) {
    return arrow::Status::Invalid("kernel context must not be null");
  }
  auto* state = static_cast<HashMinMaxState<ValueCType>*>(ctx->state());
  auto* other_state = static_cast<HashMinMaxState<ValueCType>*>(&other);
  return HashMinMaxMergeImpl<ValueCType, kIsMin>(state, other_state, group_id_mapping);
}

template <typename ValueCType>
arrow::Result<std::shared_ptr<arrow::DataType>> HashMinMaxOutType();

template <>
arrow::Result<std::shared_ptr<arrow::DataType>> HashMinMaxOutType<float>() {
  return arrow::float32();
}

template <>
arrow::Result<std::shared_ptr<arrow::DataType>> HashMinMaxOutType<double>() {
  return arrow::float64();
}

template <typename ValueCType>
arrow::Status HashMinMaxFinalizeImpl(HashMinMaxState<ValueCType>* state,
                                     arrow::compute::KernelContext* ctx, arrow::Datum* out) {
  if (out == nullptr) {
    return arrow::Status::Invalid("output datum must not be null");
  }
  if (state == nullptr) {
    return arrow::Status::Invalid("hash_min/max state must not be null");
  }

  ARROW_ASSIGN_OR_RAISE(auto out_type, HashMinMaxOutType<ValueCType>());

  std::shared_ptr<arrow::Buffer> null_bitmap;
  int64_t null_count = 0;
  const int64_t min_count = std::max<int64_t>(0, state->options.min_count);

  ARROW_ASSIGN_OR_RAISE(
      auto values,
      arrow::AllocateBuffer(state->num_groups * static_cast<int64_t>(sizeof(ValueCType)),
                            ctx->memory_pool()));
  std::memset(values->mutable_data(), 0, values->size());
  auto* out_values = values->template mutable_data_as<ValueCType>();

  for (int64_t i = 0; i < state->num_groups; ++i) {
    const bool is_null =
        (!state->options.skip_nulls && state->seen_null[static_cast<std::size_t>(i)] != 0)
        || (state->counts[static_cast<std::size_t>(i)] < min_count)
        || (state->counts[static_cast<std::size_t>(i)] == 0);
    if (is_null) {
      if (null_bitmap == nullptr) {
        ARROW_ASSIGN_OR_RAISE(null_bitmap,
                              arrow::AllocateBitmap(state->num_groups, ctx->memory_pool()));
        arrow::bit_util::SetBitsTo(null_bitmap->mutable_data(), 0, state->num_groups, true);
      }
      ++null_count;
      arrow::bit_util::SetBitTo(null_bitmap->mutable_data(), i, false);
      continue;
    }

    out_values[i] = state->values[static_cast<std::size_t>(i)];
  }

  auto arr_data = arrow::ArrayData::Make(std::move(out_type), state->num_groups,
                                         {std::move(null_bitmap), std::move(values)}, null_count);
  *out = arrow::Datum(arrow::MakeArray(std::move(arr_data)));
  return arrow::Status::OK();
}

arrow::Status HashMinFloat32Finalize(arrow::compute::KernelContext* ctx, arrow::Datum* out) {
  auto* state = static_cast<HashMinMaxState<float>*>(ctx->state());
  return HashMinMaxFinalizeImpl(state, ctx, out);
}

arrow::Status HashMaxFloat32Finalize(arrow::compute::KernelContext* ctx, arrow::Datum* out) {
  auto* state = static_cast<HashMinMaxState<float>*>(ctx->state());
  return HashMinMaxFinalizeImpl(state, ctx, out);
}

arrow::Status HashMinFloat64Finalize(arrow::compute::KernelContext* ctx, arrow::Datum* out) {
  auto* state = static_cast<HashMinMaxState<double>*>(ctx->state());
  return HashMinMaxFinalizeImpl(state, ctx, out);
}

arrow::Status HashMaxFloat64Finalize(arrow::compute::KernelContext* ctx, arrow::Datum* out) {
  auto* state = static_cast<HashMinMaxState<double>*>(ctx->state());
  return HashMinMaxFinalizeImpl(state, ctx, out);
}

}  // namespace

arrow::Status RegisterHashMin(arrow::compute::FunctionRegistry* registry,
                             arrow::compute::FunctionRegistry* fallback_registry) {
  static const auto default_options = arrow::compute::ScalarAggregateOptions::Defaults();
  auto func = std::make_shared<arrow::compute::HashAggregateFunction>(
      "hash_min", arrow::compute::Arity::Binary(),
      arrow::compute::FunctionDoc{"Min values in each group (TiForth override for floats)",
                                  "TiForth override: float min uses `<` to match TiFlash NaN/-0 semantics.",
                                  {"array", "group_id_array"},
                                  "ScalarAggregateOptions"},
      &default_options);

  const std::vector<std::shared_ptr<arrow::DataType>> copy_types = {
      arrow::boolean(), arrow::int8(),  arrow::int16(),   arrow::int32(),   arrow::int64(),
      arrow::uint8(),   arrow::uint16(), arrow::uint32(), arrow::uint64(),  arrow::null(),
      arrow::decimal128(1, 0), arrow::decimal256(1, 0)};
  for (const auto& ty : copy_types) {
    ARROW_ASSIGN_OR_RAISE(auto kernel,
                          detail::CopyFallbackHashKernel(fallback_registry, "hash_min", ty));
    ARROW_RETURN_NOT_OK(func->AddKernel(std::move(kernel)));
  }

  {
    arrow::compute::HashAggregateKernel kernel;
    kernel.signature = arrow::compute::KernelSignature::Make(
        {arrow::compute::InputType(arrow::Type::FLOAT), arrow::Type::UINT32}, arrow::float32());
    kernel.init = HashMinMaxInit<float>;
    kernel.resize = HashMinMaxResize<float>;
    kernel.consume = HashMinMaxConsume<float, /*kIsMin=*/true>;
    kernel.merge = HashMinMaxMerge<float, /*kIsMin=*/true>;
    kernel.finalize = HashMinFloat32Finalize;
    kernel.ordered = false;
    ARROW_RETURN_NOT_OK(func->AddKernel(std::move(kernel)));
  }

  {
    arrow::compute::HashAggregateKernel kernel;
    kernel.signature = arrow::compute::KernelSignature::Make(
        {arrow::compute::InputType(arrow::Type::DOUBLE), arrow::Type::UINT32}, arrow::float64());
    kernel.init = HashMinMaxInit<double>;
    kernel.resize = HashMinMaxResize<double>;
    kernel.consume = HashMinMaxConsume<double, /*kIsMin=*/true>;
    kernel.merge = HashMinMaxMerge<double, /*kIsMin=*/true>;
    kernel.finalize = HashMinFloat64Finalize;
    kernel.ordered = false;
    ARROW_RETURN_NOT_OK(func->AddKernel(std::move(kernel)));
  }

  return registry->AddFunction(std::move(func), /*allow_overwrite=*/true);
}

arrow::Status RegisterHashMax(arrow::compute::FunctionRegistry* registry,
                             arrow::compute::FunctionRegistry* fallback_registry) {
  static const auto default_options = arrow::compute::ScalarAggregateOptions::Defaults();
  auto func = std::make_shared<arrow::compute::HashAggregateFunction>(
      "hash_max", arrow::compute::Arity::Binary(),
      arrow::compute::FunctionDoc{"Max values in each group (TiForth override for floats)",
                                  "TiForth override: float max uses `>` to match TiFlash NaN/-0 semantics.",
                                  {"array", "group_id_array"},
                                  "ScalarAggregateOptions"},
      &default_options);

  const std::vector<std::shared_ptr<arrow::DataType>> copy_types = {
      arrow::boolean(), arrow::int8(),  arrow::int16(),   arrow::int32(),   arrow::int64(),
      arrow::uint8(),   arrow::uint16(), arrow::uint32(), arrow::uint64(),  arrow::null(),
      arrow::decimal128(1, 0), arrow::decimal256(1, 0)};
  for (const auto& ty : copy_types) {
    ARROW_ASSIGN_OR_RAISE(auto kernel,
                          detail::CopyFallbackHashKernel(fallback_registry, "hash_max", ty));
    ARROW_RETURN_NOT_OK(func->AddKernel(std::move(kernel)));
  }

  {
    arrow::compute::HashAggregateKernel kernel;
    kernel.signature = arrow::compute::KernelSignature::Make(
        {arrow::compute::InputType(arrow::Type::FLOAT), arrow::Type::UINT32}, arrow::float32());
    kernel.init = HashMinMaxInit<float>;
    kernel.resize = HashMinMaxResize<float>;
    kernel.consume = HashMinMaxConsume<float, /*kIsMin=*/false>;
    kernel.merge = HashMinMaxMerge<float, /*kIsMin=*/false>;
    kernel.finalize = HashMaxFloat32Finalize;
    kernel.ordered = false;
    ARROW_RETURN_NOT_OK(func->AddKernel(std::move(kernel)));
  }

  {
    arrow::compute::HashAggregateKernel kernel;
    kernel.signature = arrow::compute::KernelSignature::Make(
        {arrow::compute::InputType(arrow::Type::DOUBLE), arrow::Type::UINT32}, arrow::float64());
    kernel.init = HashMinMaxInit<double>;
    kernel.resize = HashMinMaxResize<double>;
    kernel.consume = HashMinMaxConsume<double, /*kIsMin=*/false>;
    kernel.merge = HashMinMaxMerge<double, /*kIsMin=*/false>;
    kernel.finalize = HashMaxFloat64Finalize;
    kernel.ordered = false;
    ARROW_RETURN_NOT_OK(func->AddKernel(std::move(kernel)));
  }

  return registry->AddFunction(std::move(func), /*allow_overwrite=*/true);
}

}  // namespace tiforth::function

