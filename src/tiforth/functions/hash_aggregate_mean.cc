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

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <memory>
#include <type_traits>
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
#include <arrow/util/decimal.h>

#include "tiforth/functions/hash_aggregate_common.h"

namespace tiforth::function {

namespace {

template <typename SumType>
struct HashMeanState final : public arrow::compute::KernelState {
  HashMeanState(const arrow::compute::ScalarAggregateOptions& options, arrow::MemoryPool* pool)
      : options(options),
        alloc_u8(pool),
        alloc_sum(pool),
        alloc_count(pool),
        seen_null(alloc_u8),
        sums(alloc_sum),
        counts(alloc_count) {}

  arrow::compute::ScalarAggregateOptions options;
  arrow::stl::allocator<uint8_t> alloc_u8;
  arrow::stl::allocator<SumType> alloc_sum;
  arrow::stl::allocator<int64_t> alloc_count;
  std::vector<uint8_t, arrow::stl::allocator<uint8_t>> seen_null;
  std::vector<SumType, arrow::stl::allocator<SumType>> sums;
  std::vector<int64_t, arrow::stl::allocator<int64_t>> counts;
  int64_t num_groups = 0;
};

arrow::Result<std::unique_ptr<arrow::compute::KernelState>> HashMeanInit(
    arrow::compute::KernelContext* ctx, const arrow::compute::KernelInitArgs& args) {
  if (ctx == nullptr) {
    return arrow::Status::Invalid("kernel context must not be null");
  }
  const auto* options = static_cast<const arrow::compute::ScalarAggregateOptions*>(args.options);
  if (options == nullptr) {
    return arrow::Status::Invalid("ScalarAggregateOptions must not be null");
  }
  if (args.inputs.empty() || args.inputs[0].type == nullptr) {
    return arrow::Status::Invalid("hash_mean expects a value type");
  }

  const auto type_id = args.inputs[0].type->id();
  switch (type_id) {
    case arrow::Type::INT8:
    case arrow::Type::INT16:
    case arrow::Type::INT32:
    case arrow::Type::INT64:
      return std::make_unique<HashMeanState<int64_t>>(*options, ctx->memory_pool());
    case arrow::Type::UINT8:
    case arrow::Type::UINT16:
    case arrow::Type::UINT32:
    case arrow::Type::UINT64:
      return std::make_unique<HashMeanState<uint64_t>>(*options, ctx->memory_pool());
    default:
      return arrow::Status::Invalid("hash_mean int kernel got unsupported type: ",
                                    args.inputs[0].type->ToString());
  }
}

arrow::Status HashMeanResize(arrow::compute::KernelContext* ctx, int64_t new_num_groups) {
  if (ctx == nullptr) {
    return arrow::Status::Invalid("kernel context must not be null");
  }

  const auto resize_impl = [&](auto* state) -> arrow::Status {
    if (state == nullptr) {
      return arrow::Status::Invalid("hash_mean state must not be null");
    }
    if (new_num_groups < 0) {
      return arrow::Status::Invalid("negative group count");
    }
    if (new_num_groups > state->num_groups) {
      const std::size_t n = static_cast<std::size_t>(new_num_groups);
      state->seen_null.resize(n, 0);
      state->sums.resize(n, 0);
      state->counts.resize(n, 0);
      state->num_groups = new_num_groups;
    }
    return arrow::Status::OK();
  };

  if (auto* s = dynamic_cast<HashMeanState<int64_t>*>(ctx->state())) {
    return resize_impl(s);
  }
  if (auto* s = dynamic_cast<HashMeanState<uint64_t>*>(ctx->state())) {
    return resize_impl(s);
  }
  return arrow::Status::Invalid("hash_mean state type mismatch");
}

template <typename InCType, typename SumType>
arrow::Status HashMeanConsumeTyped(HashMeanState<SumType>* state, const arrow::ArraySpan& values,
                                  const uint32_t* group_ids, int64_t length) {
  const auto* data = values.GetValues<InCType>(1);
  for (int64_t i = 0; i < length; ++i) {
    const auto g = group_ids[i];
    if (values.IsValid(i)) {
      if constexpr (std::is_same_v<SumType, int64_t>) {
        state->sums[g] =
            detail::AddWrapSigned(state->sums[g], static_cast<int64_t>(data[i]));
      } else {
        state->sums[g] += static_cast<uint64_t>(data[i]);
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

arrow::Status HashMeanConsume(arrow::compute::KernelContext* ctx,
                             const arrow::compute::ExecSpan& batch) {
  if (ctx == nullptr) {
    return arrow::Status::Invalid("kernel context must not be null");
  }
  if (batch.num_values() != 2) {
    return arrow::Status::Invalid("hash_mean expects 2 arguments (value, group_id)");
  }
  if (!batch[0].is_array() || !batch[1].is_array()) {
    return arrow::Status::Invalid("hash_mean expects array arguments");
  }
  const auto& values = batch[0].array;
  const auto& group_ids = batch[1].array;
  const auto* g = group_ids.GetValues<uint32_t>(1);
  if (g == nullptr) {
    return arrow::Status::Invalid("group id buffer must not be null");
  }

  switch (values.type->id()) {
    case arrow::Type::INT8:
      return HashMeanConsumeTyped<int8_t>(static_cast<HashMeanState<int64_t>*>(ctx->state()),
                                          values, g, batch.length);
    case arrow::Type::INT16:
      return HashMeanConsumeTyped<int16_t>(static_cast<HashMeanState<int64_t>*>(ctx->state()),
                                           values, g, batch.length);
    case arrow::Type::INT32:
      return HashMeanConsumeTyped<int32_t>(static_cast<HashMeanState<int64_t>*>(ctx->state()),
                                           values, g, batch.length);
    case arrow::Type::INT64:
      return HashMeanConsumeTyped<int64_t>(static_cast<HashMeanState<int64_t>*>(ctx->state()),
                                           values, g, batch.length);
    case arrow::Type::UINT8:
      return HashMeanConsumeTyped<uint8_t>(static_cast<HashMeanState<uint64_t>*>(ctx->state()),
                                           values, g, batch.length);
    case arrow::Type::UINT16:
      return HashMeanConsumeTyped<uint16_t>(static_cast<HashMeanState<uint64_t>*>(ctx->state()),
                                            values, g, batch.length);
    case arrow::Type::UINT32:
      return HashMeanConsumeTyped<uint32_t>(static_cast<HashMeanState<uint64_t>*>(ctx->state()),
                                            values, g, batch.length);
    case arrow::Type::UINT64:
      return HashMeanConsumeTyped<uint64_t>(static_cast<HashMeanState<uint64_t>*>(ctx->state()),
                                            values, g, batch.length);
    default:
      return arrow::Status::Invalid("hash_mean int kernel got unsupported type: ",
                                    values.type->ToString());
  }
}

template <typename SumType>
arrow::Status HashMeanMergeImpl(HashMeanState<SumType>* state, HashMeanState<SumType>* other_state,
                                const arrow::ArrayData& group_id_mapping) {
  const auto* g = group_id_mapping.GetValues<uint32_t>(1);
  if (g == nullptr) {
    return arrow::Status::Invalid("hash_mean group id mapping buffer must not be null");
  }
  if (group_id_mapping.length != static_cast<int64_t>(other_state->counts.size())) {
    return arrow::Status::Invalid("hash_mean group id mapping length mismatch");
  }

  for (int64_t other_g = 0; other_g < group_id_mapping.length; ++other_g) {
    const auto out_g = g[other_g];
    state->seen_null[out_g] |= other_state->seen_null[static_cast<std::size_t>(other_g)];
    if constexpr (std::is_same_v<SumType, int64_t>) {
      state->sums[out_g] = detail::AddWrapSigned(
          state->sums[out_g], other_state->sums[static_cast<std::size_t>(other_g)]);
    } else {
      state->sums[out_g] += other_state->sums[static_cast<std::size_t>(other_g)];
    }
    state->counts[out_g] += other_state->counts[static_cast<std::size_t>(other_g)];
  }
  return arrow::Status::OK();
}

arrow::Status HashMeanMerge(arrow::compute::KernelContext* ctx, arrow::compute::KernelState&& other,
                           const arrow::ArrayData& group_id_mapping) {
  if (ctx == nullptr) {
    return arrow::Status::Invalid("kernel context must not be null");
  }
  if (group_id_mapping.type == nullptr || group_id_mapping.type->id() != arrow::Type::UINT32) {
    return arrow::Status::Invalid("hash_mean group id mapping must be uint32");
  }

  if (auto* s = dynamic_cast<HashMeanState<int64_t>*>(ctx->state())) {
    auto* o = static_cast<HashMeanState<int64_t>*>(&other);
    return HashMeanMergeImpl<int64_t>(s, o, group_id_mapping);
  }
  if (auto* s = dynamic_cast<HashMeanState<uint64_t>*>(ctx->state())) {
    auto* o = static_cast<HashMeanState<uint64_t>*>(&other);
    return HashMeanMergeImpl<uint64_t>(s, o, group_id_mapping);
  }
  return arrow::Status::Invalid("hash_mean state type mismatch");
}

template <typename SumType>
arrow::Status HashMeanFinalizeImpl(HashMeanState<SumType>* state, arrow::compute::KernelContext* ctx,
                                  arrow::Datum* out) {
  if (out == nullptr) {
    return arrow::Status::Invalid("output datum must not be null");
  }
  ARROW_ASSIGN_OR_RAISE(
      auto values,
      arrow::AllocateBuffer(state->num_groups * static_cast<int64_t>(sizeof(double)),
                            ctx->memory_pool()));
  auto* out_values = values->template mutable_data_as<double>();

  std::shared_ptr<arrow::Buffer> null_bitmap;
  int64_t null_count = 0;
  const int64_t min_count = std::max<int64_t>(0, state->options.min_count);

  for (int64_t i = 0; i < state->num_groups; ++i) {
    const bool is_null =
        (!state->options.skip_nulls && state->seen_null[static_cast<std::size_t>(i)] != 0)
        || (state->counts[static_cast<std::size_t>(i)] < min_count);
    if (is_null) {
      out_values[i] = 0.0;
      if (null_bitmap == nullptr) {
        ARROW_ASSIGN_OR_RAISE(null_bitmap,
                              arrow::AllocateBitmap(state->num_groups, ctx->memory_pool()));
        arrow::bit_util::SetBitsTo(null_bitmap->mutable_data(), 0, state->num_groups, true);
      }
      ++null_count;
      arrow::bit_util::SetBitTo(null_bitmap->mutable_data(), i, false);
      continue;
    }
    const auto cnt = state->counts[static_cast<std::size_t>(i)];
    if constexpr (std::is_same_v<SumType, int64_t>) {
      out_values[i] = static_cast<double>(state->sums[static_cast<std::size_t>(i)]) / cnt;
    } else {
      out_values[i] = static_cast<double>(state->sums[static_cast<std::size_t>(i)]) / cnt;
    }
  }

  auto arr_data = arrow::ArrayData::Make(arrow::float64(), state->num_groups,
                                         {std::move(null_bitmap), std::move(values)}, null_count);
  *out = arrow::Datum(arrow::MakeArray(std::move(arr_data)));
  return arrow::Status::OK();
}

arrow::Status HashMeanFinalize(arrow::compute::KernelContext* ctx, arrow::Datum* out) {
  if (auto* s = dynamic_cast<HashMeanState<int64_t>*>(ctx->state())) {
    return HashMeanFinalizeImpl<int64_t>(s, ctx, out);
  }
  if (auto* s = dynamic_cast<HashMeanState<uint64_t>*>(ctx->state())) {
    return HashMeanFinalizeImpl<uint64_t>(s, ctx, out);
  }
  return arrow::Status::Invalid("hash_mean state type mismatch");
}

struct DecimalMeanState final : public arrow::compute::KernelState {
  DecimalMeanState(const arrow::compute::ScalarAggregateOptions& options,
                   const std::shared_ptr<arrow::DataType>& out_type, int32_t in_scale,
                   int32_t out_scale, int32_t out_byte_width, arrow::MemoryPool* pool)
      : options(options),
        out_type(out_type),
        in_scale(in_scale),
        out_scale(out_scale),
        out_byte_width(out_byte_width),
        alloc_u8(pool),
        alloc_dec(pool),
        alloc_count(pool),
        seen_null(alloc_u8),
        sums(alloc_dec),
        counts(alloc_count) {}

  arrow::compute::ScalarAggregateOptions options;
  std::shared_ptr<arrow::DataType> out_type;
  int32_t in_scale = 0;
  int32_t out_scale = 0;
  int32_t out_byte_width = 0;

  arrow::stl::allocator<uint8_t> alloc_u8;
  arrow::stl::allocator<arrow::Decimal256> alloc_dec;
  arrow::stl::allocator<int64_t> alloc_count;
  std::vector<uint8_t, arrow::stl::allocator<uint8_t>> seen_null;
  std::vector<arrow::Decimal256, arrow::stl::allocator<arrow::Decimal256>> sums;
  std::vector<int64_t, arrow::stl::allocator<int64_t>> counts;
  int64_t num_groups = 0;
};

arrow::Result<std::unique_ptr<arrow::compute::KernelState>> HashMeanDecimalInit(
    arrow::compute::KernelContext* ctx, const arrow::compute::KernelInitArgs& args) {
  if (ctx == nullptr) {
    return arrow::Status::Invalid("kernel context must not be null");
  }
  const auto* options = static_cast<const arrow::compute::ScalarAggregateOptions*>(args.options);
  if (options == nullptr) {
    return arrow::Status::Invalid("ScalarAggregateOptions must not be null");
  }
  if (args.inputs.empty() || args.inputs[0].type == nullptr) {
    return arrow::Status::Invalid("hash_mean(decimal) expects a value type");
  }

  ARROW_ASSIGN_OR_RAISE(auto ps, detail::GetDecimalPrecisionAndScale(*args.inputs[0].type));
  const auto [out_prec, out_scale] = detail::InferAvgDecimalPrecisionAndScale(ps.first, ps.second);
  ARROW_ASSIGN_OR_RAISE(auto out_type, detail::MakeDecimalOutType(out_prec, out_scale));
  const int32_t out_byte_width = static_cast<int32_t>(out_type->byte_width());
  return std::make_unique<DecimalMeanState>(*options, out_type, /*in_scale=*/ps.second, out_scale,
                                           out_byte_width, ctx->memory_pool());
}

arrow::Status HashMeanDecimalResize(arrow::compute::KernelContext* ctx, int64_t new_num_groups) {
  auto* state = static_cast<DecimalMeanState*>(ctx->state());
  if (state == nullptr) {
    return arrow::Status::Invalid("hash_mean(decimal) state must not be null");
  }
  if (new_num_groups < 0) {
    return arrow::Status::Invalid("negative group count");
  }
  if (new_num_groups > state->num_groups) {
    const std::size_t n = static_cast<std::size_t>(new_num_groups);
    state->seen_null.resize(n, 0);
    state->sums.resize(n, arrow::Decimal256(0));
    state->counts.resize(n, 0);
    state->num_groups = new_num_groups;
  }
  return arrow::Status::OK();
}

arrow::Status HashMeanDecimalConsume(arrow::compute::KernelContext* ctx,
                                    const arrow::compute::ExecSpan& batch) {
  auto* state = static_cast<DecimalMeanState*>(ctx->state());
  if (state == nullptr) {
    return arrow::Status::Invalid("hash_mean(decimal) state must not be null");
  }
  if (batch.num_values() != 2) {
    return arrow::Status::Invalid("hash_mean(decimal) expects 2 arguments (value, group_id)");
  }
  if (!batch[0].is_array() || !batch[1].is_array()) {
    return arrow::Status::Invalid("hash_mean(decimal) expects array arguments");
  }

  const auto& values = batch[0].array;
  const auto& group_ids = batch[1].array;
  const auto* g = group_ids.GetValues<uint32_t>(1);
  if (g == nullptr) {
    return arrow::Status::Invalid("group id buffer must not be null");
  }

  const int32_t byte_width = static_cast<const arrow::DecimalType&>(*values.type).byte_width();
  const uint8_t* value_bytes = values.buffers[1].data;
  if (value_bytes == nullptr) {
    return arrow::Status::Invalid("decimal values buffer must not be null");
  }
  value_bytes += static_cast<int64_t>(values.offset) * byte_width;

  for (int64_t i = 0; i < batch.length; ++i) {
    const auto group = g[i];
    if (values.IsValid(i)) {
      if (byte_width == 16) {
        state->sums[group] += arrow::Decimal256(arrow::Decimal128(value_bytes + i * 16));
      } else if (byte_width == 32) {
        state->sums[group] += arrow::Decimal256(value_bytes + i * 32);
      } else {
        return arrow::Status::Invalid("unexpected decimal byte width: ", byte_width);
      }
      state->counts[group] += 1;
      continue;
    }
    if (!state->options.skip_nulls) {
      state->seen_null[group] = 1;
    }
  }
  return arrow::Status::OK();
}

arrow::Status HashMeanDecimalMerge(arrow::compute::KernelContext* ctx,
                                  arrow::compute::KernelState&& other,
                                  const arrow::ArrayData& group_id_mapping) {
  auto* state = static_cast<DecimalMeanState*>(ctx->state());
  auto* other_state = static_cast<DecimalMeanState*>(&other);
  if (state == nullptr || other_state == nullptr) {
    return arrow::Status::Invalid("hash_mean(decimal) merge state must not be null");
  }
  if (group_id_mapping.type == nullptr || group_id_mapping.type->id() != arrow::Type::UINT32) {
    return arrow::Status::Invalid("hash_mean(decimal) group id mapping must be uint32");
  }
  const auto* g = group_id_mapping.GetValues<uint32_t>(1);
  if (g == nullptr) {
    return arrow::Status::Invalid("hash_mean(decimal) group id mapping buffer must not be null");
  }
  if (group_id_mapping.length != static_cast<int64_t>(other_state->counts.size())) {
    return arrow::Status::Invalid("hash_mean(decimal) group id mapping length mismatch");
  }

  for (int64_t other_g = 0; other_g < group_id_mapping.length; ++other_g) {
    const auto out_g = g[other_g];
    state->seen_null[out_g] |= other_state->seen_null[static_cast<std::size_t>(other_g)];
    state->sums[out_g] += other_state->sums[static_cast<std::size_t>(other_g)];
    state->counts[out_g] += other_state->counts[static_cast<std::size_t>(other_g)];
  }
  return arrow::Status::OK();
}

arrow::Status HashMeanDecimalFinalize(arrow::compute::KernelContext* ctx, arrow::Datum* out) {
  if (out == nullptr) {
    return arrow::Status::Invalid("output datum must not be null");
  }
  auto* state = static_cast<DecimalMeanState*>(ctx->state());
  if (state == nullptr) {
    return arrow::Status::Invalid("hash_mean(decimal) state must not be null");
  }

  std::shared_ptr<arrow::Buffer> null_bitmap;
  int64_t null_count = 0;
  const int64_t min_count = std::max<int64_t>(0, state->options.min_count);

  ARROW_ASSIGN_OR_RAISE(
      auto values,
      arrow::AllocateBuffer(state->num_groups * static_cast<int64_t>(state->out_byte_width),
                            ctx->memory_pool()));
  std::memset(values->mutable_data(), 0, values->size());

  const int32_t scale_delta = state->out_scale - state->in_scale;
  for (int64_t i = 0; i < state->num_groups; ++i) {
    const bool is_null =
        (!state->options.skip_nulls && state->seen_null[static_cast<std::size_t>(i)] != 0)
        || (state->counts[static_cast<std::size_t>(i)] < min_count);
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

    arrow::Decimal256 scaled =
        state->sums[static_cast<std::size_t>(i)].IncreaseScaleBy(scale_delta);
    const auto divisor = arrow::Decimal256(state->counts[static_cast<std::size_t>(i)]);
    ARROW_ASSIGN_OR_RAISE(auto qr, scaled.Divide(divisor));
    const auto& quotient = qr.first;

    const auto bytes = quotient.ToBytes();
    if (state->out_byte_width == 16) {
      std::memcpy(values->mutable_data() + i * 16, bytes.data(), 16);
    } else if (state->out_byte_width == 32) {
      std::memcpy(values->mutable_data() + i * 32, bytes.data(), 32);
    } else {
      return arrow::Status::Invalid("unexpected output decimal byte width: ", state->out_byte_width);
    }
  }

  auto arr_data = arrow::ArrayData::Make(state->out_type, state->num_groups,
                                         {std::move(null_bitmap), std::move(values)}, null_count);
  *out = arrow::Datum(arrow::MakeArray(std::move(arr_data)));
  return arrow::Status::OK();
}

arrow::Result<arrow::TypeHolder> HashMeanDecimalOutType(arrow::compute::KernelContext*,
                                                       const std::vector<arrow::TypeHolder>& args) {
  if (args.empty() || args[0].type == nullptr) {
    return arrow::Status::Invalid("hash_mean(decimal) out type resolver expects value type");
  }
  ARROW_ASSIGN_OR_RAISE(auto ps, detail::GetDecimalPrecisionAndScale(*args[0].type));
  const auto [out_prec, out_scale] = detail::InferAvgDecimalPrecisionAndScale(ps.first, ps.second);
  ARROW_ASSIGN_OR_RAISE(auto out_type, detail::MakeDecimalOutType(out_prec, out_scale));
  return arrow::TypeHolder(out_type);
}

}  // namespace

arrow::Status RegisterHashMean(arrow::compute::FunctionRegistry* registry,
                              arrow::compute::FunctionRegistry* fallback_registry) {
  static const auto default_options = arrow::compute::ScalarAggregateOptions::Defaults();
  auto func = std::make_shared<arrow::compute::HashAggregateFunction>(
      "hash_mean", arrow::compute::Arity::Binary(),
      arrow::compute::FunctionDoc{"Mean/avg values in each group (TiForth override for int/decimal)",
                                  "TiForth override: int mean uses overflowed int sum semantics;\n"
                                  "decimal mean matches TiFlash div precision increment and truncation.",
                                  {"array", "group_id_array"},
                                  "ScalarAggregateOptions"},
      &default_options);

  // Copy fallback kernels for non-integer, non-decimal types we care about.
  const std::vector<std::shared_ptr<arrow::DataType>> copy_types = {arrow::boolean(), arrow::float32(),
                                                                    arrow::float64(), arrow::null()};
  for (const auto& ty : copy_types) {
    ARROW_ASSIGN_OR_RAISE(auto kernel,
                          detail::CopyFallbackHashKernel(fallback_registry, "hash_mean", ty));
    ARROW_RETURN_NOT_OK(func->AddKernel(std::move(kernel)));
  }

  const std::vector<std::shared_ptr<arrow::DataType>> int_types = {
      arrow::int8(),  arrow::int16(),  arrow::int32(),  arrow::int64(),
      arrow::uint8(), arrow::uint16(), arrow::uint32(), arrow::uint64()};
  for (const auto& ty : int_types) {
    arrow::compute::HashAggregateKernel kernel;
    kernel.signature = arrow::compute::KernelSignature::Make(
        {arrow::compute::InputType(ty->id()), arrow::Type::UINT32}, arrow::float64());
    kernel.init = HashMeanInit;
    kernel.resize = HashMeanResize;
    kernel.consume = HashMeanConsume;
    kernel.merge = HashMeanMerge;
    kernel.finalize = HashMeanFinalize;
    kernel.ordered = false;
    ARROW_RETURN_NOT_OK(func->AddKernel(std::move(kernel)));
  }

  {
    arrow::compute::HashAggregateKernel kernel;
    kernel.signature = arrow::compute::KernelSignature::Make(
        {arrow::compute::InputType(arrow::Type::DECIMAL128), arrow::Type::UINT32},
        arrow::compute::OutputType(HashMeanDecimalOutType));
    kernel.init = HashMeanDecimalInit;
    kernel.resize = HashMeanDecimalResize;
    kernel.consume = HashMeanDecimalConsume;
    kernel.merge = HashMeanDecimalMerge;
    kernel.finalize = HashMeanDecimalFinalize;
    kernel.ordered = false;
    ARROW_RETURN_NOT_OK(func->AddKernel(std::move(kernel)));
  }
  {
    arrow::compute::HashAggregateKernel kernel;
    kernel.signature = arrow::compute::KernelSignature::Make(
        {arrow::compute::InputType(arrow::Type::DECIMAL256), arrow::Type::UINT32},
        arrow::compute::OutputType(HashMeanDecimalOutType));
    kernel.init = HashMeanDecimalInit;
    kernel.resize = HashMeanDecimalResize;
    kernel.consume = HashMeanDecimalConsume;
    kernel.merge = HashMeanDecimalMerge;
    kernel.finalize = HashMeanDecimalFinalize;
    kernel.ordered = false;
    ARROW_RETURN_NOT_OK(func->AddKernel(std::move(kernel)));
  }

  return registry->AddFunction(std::move(func), /*allow_overwrite=*/true);
}

}  // namespace tiforth::function

