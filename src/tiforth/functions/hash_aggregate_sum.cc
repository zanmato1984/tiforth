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
#include <arrow/util/decimal.h>

#include "tiforth/functions/hash_aggregate_common.h"

namespace tiforth::function {

namespace {

struct DecimalSumState final : public arrow::compute::KernelState {
  DecimalSumState(const arrow::compute::ScalarAggregateOptions& options,
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

arrow::Result<std::unique_ptr<arrow::compute::KernelState>> HashSumDecimalInit(
    arrow::compute::KernelContext* ctx, const arrow::compute::KernelInitArgs& args) {
  if (ctx == nullptr) {
    return arrow::Status::Invalid("kernel context must not be null");
  }
  const auto* options = static_cast<const arrow::compute::ScalarAggregateOptions*>(args.options);
  if (options == nullptr) {
    return arrow::Status::Invalid("ScalarAggregateOptions must not be null");
  }
  if (args.inputs.empty() || args.inputs[0].type == nullptr) {
    return arrow::Status::Invalid("hash_sum(decimal) expects a value type");
  }

  ARROW_ASSIGN_OR_RAISE(auto ps, detail::GetDecimalPrecisionAndScale(*args.inputs[0].type));
  const auto [out_prec, out_scale] = detail::InferSumDecimalPrecisionAndScale(ps.first, ps.second);
  ARROW_ASSIGN_OR_RAISE(auto out_type, detail::MakeDecimalOutType(out_prec, out_scale));
  const int32_t out_byte_width = static_cast<int32_t>(out_type->byte_width());
  return std::make_unique<DecimalSumState>(*options, out_type, /*in_scale=*/ps.second, out_scale,
                                          out_byte_width, ctx->memory_pool());
}

arrow::Status HashSumDecimalResize(arrow::compute::KernelContext* ctx, int64_t new_num_groups) {
  auto* state = static_cast<DecimalSumState*>(ctx->state());
  if (state == nullptr) {
    return arrow::Status::Invalid("hash_sum(decimal) state must not be null");
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

arrow::Status HashSumDecimalConsume(arrow::compute::KernelContext* ctx,
                                   const arrow::compute::ExecSpan& batch) {
  auto* state = static_cast<DecimalSumState*>(ctx->state());
  if (state == nullptr) {
    return arrow::Status::Invalid("hash_sum(decimal) state must not be null");
  }
  if (batch.num_values() != 2) {
    return arrow::Status::Invalid("hash_sum(decimal) expects 2 arguments (value, group_id)");
  }
  if (!batch[0].is_array() || !batch[1].is_array()) {
    return arrow::Status::Invalid("hash_sum(decimal) expects array arguments");
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

arrow::Status HashSumDecimalMerge(arrow::compute::KernelContext* ctx,
                                 arrow::compute::KernelState&& other,
                                 const arrow::ArrayData& group_id_mapping) {
  auto* state = static_cast<DecimalSumState*>(ctx->state());
  auto* other_state = static_cast<DecimalSumState*>(&other);
  if (state == nullptr || other_state == nullptr) {
    return arrow::Status::Invalid("hash_sum(decimal) merge state must not be null");
  }
  if (group_id_mapping.type == nullptr || group_id_mapping.type->id() != arrow::Type::UINT32) {
    return arrow::Status::Invalid("hash_sum(decimal) group id mapping must be uint32");
  }
  const auto* g = group_id_mapping.GetValues<uint32_t>(1);
  if (g == nullptr) {
    return arrow::Status::Invalid("hash_sum(decimal) group id mapping buffer must not be null");
  }
  if (group_id_mapping.length != static_cast<int64_t>(other_state->counts.size())) {
    return arrow::Status::Invalid("hash_sum(decimal) group id mapping length mismatch");
  }

  for (int64_t other_g = 0; other_g < group_id_mapping.length; ++other_g) {
    const auto out_g = g[other_g];
    state->seen_null[out_g] |= other_state->seen_null[static_cast<std::size_t>(other_g)];
    state->sums[out_g] += other_state->sums[static_cast<std::size_t>(other_g)];
    state->counts[out_g] += other_state->counts[static_cast<std::size_t>(other_g)];
  }
  return arrow::Status::OK();
}

arrow::Status HashSumDecimalFinalize(arrow::compute::KernelContext* ctx, arrow::Datum* out) {
  if (out == nullptr) {
    return arrow::Status::Invalid("output datum must not be null");
  }
  auto* state = static_cast<DecimalSumState*>(ctx->state());
  if (state == nullptr) {
    return arrow::Status::Invalid("hash_sum(decimal) state must not be null");
  }

  std::shared_ptr<arrow::Buffer> null_bitmap;
  int64_t null_count = 0;
  const int64_t min_count = std::max<int64_t>(0, state->options.min_count);

  ARROW_ASSIGN_OR_RAISE(
      auto values,
      arrow::AllocateBuffer(state->num_groups * static_cast<int64_t>(state->out_byte_width),
                            ctx->memory_pool()));
  std::memset(values->mutable_data(), 0, values->size());

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

    const auto bytes = state->sums[static_cast<std::size_t>(i)].ToBytes();
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

arrow::Result<arrow::TypeHolder> HashSumDecimalOutType(arrow::compute::KernelContext*,
                                                      const std::vector<arrow::TypeHolder>& args) {
  if (args.empty() || args[0].type == nullptr) {
    return arrow::Status::Invalid("hash_sum(decimal) out type resolver expects value type");
  }
  ARROW_ASSIGN_OR_RAISE(auto ps, detail::GetDecimalPrecisionAndScale(*args[0].type));
  const auto [out_prec, out_scale] = detail::InferSumDecimalPrecisionAndScale(ps.first, ps.second);
  ARROW_ASSIGN_OR_RAISE(auto out_type, detail::MakeDecimalOutType(out_prec, out_scale));
  return arrow::TypeHolder(out_type);
}

}  // namespace

arrow::Status RegisterHashSum(arrow::compute::FunctionRegistry* registry,
                             arrow::compute::FunctionRegistry* fallback_registry) {
  static const auto default_options = arrow::compute::ScalarAggregateOptions::Defaults();
  auto func = std::make_shared<arrow::compute::HashAggregateFunction>(
      "hash_sum", arrow::compute::Arity::Binary(),
      arrow::compute::FunctionDoc{"Sum values in each group (TiForth override for decimal)",
                                  "TiForth override: decimal output precision matches TiFlash (prec+22),\n"
                                  "scale preserved.",
                                  {"array", "group_id_array"},
                                  "ScalarAggregateOptions"},
      &default_options);

  const std::vector<std::shared_ptr<arrow::DataType>> copy_types = {
      arrow::boolean(), arrow::int8(),  arrow::int16(),  arrow::int32(),  arrow::int64(),
      arrow::uint8(),   arrow::uint16(), arrow::uint32(), arrow::uint64(), arrow::float32(),
      arrow::float64(), arrow::null()};

  for (const auto& ty : copy_types) {
    ARROW_ASSIGN_OR_RAISE(auto kernel,
                          detail::CopyFallbackHashKernel(fallback_registry, "hash_sum", ty));
    ARROW_RETURN_NOT_OK(func->AddKernel(std::move(kernel)));
  }

  {
    arrow::compute::HashAggregateKernel kernel;
    kernel.signature = arrow::compute::KernelSignature::Make(
        {arrow::compute::InputType(arrow::Type::DECIMAL128), arrow::Type::UINT32},
        arrow::compute::OutputType(HashSumDecimalOutType));
    kernel.init = HashSumDecimalInit;
    kernel.resize = HashSumDecimalResize;
    kernel.consume = HashSumDecimalConsume;
    kernel.merge = HashSumDecimalMerge;
    kernel.finalize = HashSumDecimalFinalize;
    kernel.ordered = false;
    ARROW_RETURN_NOT_OK(func->AddKernel(std::move(kernel)));
  }
  {
    arrow::compute::HashAggregateKernel kernel;
    kernel.signature = arrow::compute::KernelSignature::Make(
        {arrow::compute::InputType(arrow::Type::DECIMAL256), arrow::Type::UINT32},
        arrow::compute::OutputType(HashSumDecimalOutType));
    kernel.init = HashSumDecimalInit;
    kernel.resize = HashSumDecimalResize;
    kernel.consume = HashSumDecimalConsume;
    kernel.merge = HashSumDecimalMerge;
    kernel.finalize = HashSumDecimalFinalize;
    kernel.ordered = false;
    ARROW_RETURN_NOT_OK(func->AddKernel(std::move(kernel)));
  }

  return registry->AddFunction(std::move(func), /*allow_overwrite=*/true);
}

}  // namespace tiforth::function

