#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/compute/api_aggregate.h>
#include <arrow/compute/function.h>
#include <arrow/compute/kernel.h>
#include <arrow/compute/registry.h>
#include <arrow/stl_allocator.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/util/bit_util.h>
#include <arrow/util/decimal.h>
#include <arrow/util/logging.h>

namespace tiforth::functions {

namespace {

constexpr int32_t kTiFlashDecimalMaxPrec = 65;
constexpr int32_t kTiFlashDecimalMaxScale = 30;
constexpr int32_t kTiFlashDecimalLongLongDigits = 22;
constexpr int32_t kTiFlashDefaultDivPrecisionIncrement = 4;

arrow::Result<std::shared_ptr<arrow::DataType>> MakeDecimalOutType(int32_t precision,
                                                                   int32_t scale) {
  if (precision <= 0 || precision > kTiFlashDecimalMaxPrec) {
    return arrow::Status::Invalid("invalid decimal precision: ", precision);
  }
  if (scale < 0 || scale > kTiFlashDecimalMaxScale) {
    return arrow::Status::Invalid("invalid decimal scale: ", scale);
  }

  constexpr int32_t kArrowDecimal128MaxPrecision = 38;
  if (precision <= kArrowDecimal128MaxPrecision) {
    return arrow::decimal128(precision, scale);
  }
  return arrow::decimal256(precision, scale);
}

arrow::Result<std::pair<int32_t, int32_t>> GetDecimalPrecisionAndScale(
    const arrow::DataType& type) {
  if (type.id() != arrow::Type::DECIMAL128 && type.id() != arrow::Type::DECIMAL256) {
    return arrow::Status::Invalid("expected decimal type, got: ", type.ToString());
  }
  const auto& dec = static_cast<const arrow::DecimalType&>(type);
  return std::make_pair(dec.precision(), dec.scale());
}

std::pair<int32_t, int32_t> InferSumDecimalPrecisionAndScale(int32_t prec, int32_t scale) {
  const int32_t out_prec = std::min(prec + kTiFlashDecimalLongLongDigits, kTiFlashDecimalMaxPrec);
  return {out_prec, scale};
}

std::pair<int32_t, int32_t> InferAvgDecimalPrecisionAndScale(int32_t prec, int32_t scale) {
  const int32_t out_prec =
      std::min(prec + kTiFlashDefaultDivPrecisionIncrement, kTiFlashDecimalMaxPrec);
  const int32_t out_scale =
      std::min(scale + kTiFlashDefaultDivPrecisionIncrement, kTiFlashDecimalMaxScale);
  return {out_prec, out_scale};
}

inline int64_t AddWrapSigned(int64_t a, int64_t b) {
  const uint64_t ua = static_cast<uint64_t>(a);
  const uint64_t ub = static_cast<uint64_t>(b);
  return static_cast<int64_t>(ua + ub);
}

arrow::Result<arrow::compute::HashAggregateKernel> CopyFallbackHashKernel(
    arrow::compute::FunctionRegistry* fallback_registry, std::string_view func_name,
    const std::shared_ptr<arrow::DataType>& value_type) {
  if (fallback_registry == nullptr) {
    return arrow::Status::Invalid("fallback registry must not be null");
  }
  ARROW_ASSIGN_OR_RAISE(auto function,
                        fallback_registry->GetFunction(std::string(func_name)));
  std::vector<arrow::TypeHolder> in_types;
  in_types.reserve(2);
  in_types.emplace_back(value_type);
  in_types.emplace_back(arrow::uint32());
  ARROW_ASSIGN_OR_RAISE(const arrow::compute::Kernel* kernel, function->DispatchExact(in_types));
  return *static_cast<const arrow::compute::HashAggregateKernel*>(kernel);
}

struct HashCountState final : public arrow::compute::KernelState {
  explicit HashCountState(const arrow::compute::CountOptions& options, arrow::MemoryPool* pool)
      : options(options), alloc(pool), counts(alloc) {}

  arrow::compute::CountOptions options;
  arrow::stl::allocator<uint64_t> alloc;
  std::vector<uint64_t, arrow::stl::allocator<uint64_t>> counts;
  int64_t num_groups = 0;
};

arrow::Result<std::unique_ptr<arrow::compute::KernelState>> HashCountInit(
    arrow::compute::KernelContext* ctx, const arrow::compute::KernelInitArgs& args) {
  if (ctx == nullptr) {
    return arrow::Status::Invalid("kernel context must not be null");
  }
  const auto* options = static_cast<const arrow::compute::CountOptions*>(args.options);
  if (options == nullptr) {
    return arrow::Status::Invalid("CountOptions must not be null");
  }
  return std::make_unique<HashCountState>(*options, ctx->memory_pool());
}

arrow::Status HashCountResize(arrow::compute::KernelContext* ctx, int64_t new_num_groups) {
  if (ctx == nullptr) {
    return arrow::Status::Invalid("kernel context must not be null");
  }
  auto* state = static_cast<HashCountState*>(ctx->state());
  if (state == nullptr) {
    return arrow::Status::Invalid("hash_count state must not be null");
  }
  if (new_num_groups < 0) {
    return arrow::Status::Invalid("negative group count");
  }
  if (new_num_groups > state->num_groups) {
    state->counts.resize(static_cast<std::size_t>(new_num_groups), 0);
    state->num_groups = new_num_groups;
  }
  return arrow::Status::OK();
}

arrow::Status HashCountConsume(arrow::compute::KernelContext* ctx,
                              const arrow::compute::ExecSpan& batch) {
  auto* state = static_cast<HashCountState*>(ctx->state());
  if (state == nullptr) {
    return arrow::Status::Invalid("hash_count state must not be null");
  }
  if (batch.num_values() != 2) {
    return arrow::Status::Invalid("hash_count expects 2 arguments (value, group_id)");
  }
  if (!batch[0].is_array() || !batch[1].is_array()) {
    return arrow::Status::Invalid("hash_count expects array arguments");
  }

  const auto& values = batch[0].array;
  const auto& group_ids = batch[1].array;
  const auto* g = group_ids.GetValues<uint32_t>(1);
  if (g == nullptr) {
    return arrow::Status::Invalid("group id buffer must not be null");
  }

  switch (state->options.mode) {
    case arrow::compute::CountOptions::ALL:
      for (int64_t i = 0; i < batch.length; ++i) {
        state->counts[g[i]] += 1;
      }
      break;
    case arrow::compute::CountOptions::ONLY_VALID:
      for (int64_t i = 0; i < batch.length; ++i) {
        state->counts[g[i]] += values.IsValid(i);
      }
      break;
    case arrow::compute::CountOptions::ONLY_NULL:
      for (int64_t i = 0; i < batch.length; ++i) {
        state->counts[g[i]] += !values.IsValid(i);
      }
      break;
  }
  return arrow::Status::OK();
}

arrow::Status HashCountMerge(arrow::compute::KernelContext* ctx, arrow::compute::KernelState&& other,
                            const arrow::ArrayData& group_id_mapping) {
  auto* state = static_cast<HashCountState*>(ctx->state());
  auto* other_state = static_cast<HashCountState*>(&other);
  if (state == nullptr || other_state == nullptr) {
    return arrow::Status::Invalid("hash_count merge state must not be null");
  }
  if (group_id_mapping.type == nullptr || group_id_mapping.type->id() != arrow::Type::UINT32) {
    return arrow::Status::Invalid("hash_count group id mapping must be uint32");
  }
  const auto* g = group_id_mapping.GetValues<uint32_t>(1);
  if (g == nullptr) {
    return arrow::Status::Invalid("hash_count group id mapping buffer must not be null");
  }
  if (group_id_mapping.length != static_cast<int64_t>(other_state->counts.size())) {
    return arrow::Status::Invalid("hash_count group id mapping length mismatch");
  }
  for (int64_t other_g = 0; other_g < group_id_mapping.length; ++other_g) {
    state->counts[g[other_g]] += other_state->counts[static_cast<std::size_t>(other_g)];
  }
  return arrow::Status::OK();
}

arrow::Status HashCountFinalize(arrow::compute::KernelContext* ctx, arrow::Datum* out) {
  if (out == nullptr) {
    return arrow::Status::Invalid("output datum must not be null");
  }
  auto* state = static_cast<HashCountState*>(ctx->state());
  if (state == nullptr) {
    return arrow::Status::Invalid("hash_count state must not be null");
  }
  ARROW_ASSIGN_OR_RAISE(
      auto data,
      arrow::AllocateBuffer(state->num_groups * static_cast<int64_t>(sizeof(uint64_t)),
                            ctx->memory_pool()));
  std::memcpy(data->mutable_data(), state->counts.data(),
              state->counts.size() * sizeof(uint64_t));
  *out = std::make_shared<arrow::UInt64Array>(state->num_groups, std::move(data));
  return arrow::Status::OK();
}

struct HashCountAllState final : public arrow::compute::KernelState {
  explicit HashCountAllState(arrow::MemoryPool* pool) : alloc(pool), counts(alloc) {}

  arrow::stl::allocator<uint64_t> alloc;
  std::vector<uint64_t, arrow::stl::allocator<uint64_t>> counts;
  int64_t num_groups = 0;
};

arrow::Result<std::unique_ptr<arrow::compute::KernelState>> HashCountAllInit(
    arrow::compute::KernelContext* ctx, const arrow::compute::KernelInitArgs&) {
  if (ctx == nullptr) {
    return arrow::Status::Invalid("kernel context must not be null");
  }
  return std::make_unique<HashCountAllState>(ctx->memory_pool());
}

arrow::Status HashCountAllResize(arrow::compute::KernelContext* ctx, int64_t new_num_groups) {
  if (ctx == nullptr) {
    return arrow::Status::Invalid("kernel context must not be null");
  }
  auto* state = static_cast<HashCountAllState*>(ctx->state());
  if (state == nullptr) {
    return arrow::Status::Invalid("hash_count_all state must not be null");
  }
  if (new_num_groups < 0) {
    return arrow::Status::Invalid("negative group count");
  }
  if (new_num_groups > state->num_groups) {
    state->counts.resize(static_cast<std::size_t>(new_num_groups), 0);
    state->num_groups = new_num_groups;
  }
  return arrow::Status::OK();
}

arrow::Status HashCountAllConsume(arrow::compute::KernelContext* ctx,
                                 const arrow::compute::ExecSpan& batch) {
  auto* state = static_cast<HashCountAllState*>(ctx->state());
  if (state == nullptr) {
    return arrow::Status::Invalid("hash_count_all state must not be null");
  }
  if (batch.num_values() != 1) {
    return arrow::Status::Invalid("hash_count_all expects 1 argument (group_id)");
  }
  if (!batch[0].is_array()) {
    return arrow::Status::Invalid("hash_count_all expects array argument");
  }
  const auto& group_ids = batch[0].array;
  const auto* g = group_ids.GetValues<uint32_t>(1);
  if (g == nullptr) {
    return arrow::Status::Invalid("group id buffer must not be null");
  }
  for (int64_t i = 0; i < batch.length; ++i) {
    state->counts[g[i]] += 1;
  }
  return arrow::Status::OK();
}

arrow::Status HashCountAllMerge(arrow::compute::KernelContext* ctx,
                               arrow::compute::KernelState&& other,
                               const arrow::ArrayData& group_id_mapping) {
  auto* state = static_cast<HashCountAllState*>(ctx->state());
  auto* other_state = static_cast<HashCountAllState*>(&other);
  if (state == nullptr || other_state == nullptr) {
    return arrow::Status::Invalid("hash_count_all merge state must not be null");
  }
  if (group_id_mapping.type == nullptr || group_id_mapping.type->id() != arrow::Type::UINT32) {
    return arrow::Status::Invalid("hash_count_all group id mapping must be uint32");
  }
  const auto* g = group_id_mapping.GetValues<uint32_t>(1);
  if (g == nullptr) {
    return arrow::Status::Invalid("hash_count_all group id mapping buffer must not be null");
  }
  if (group_id_mapping.length != static_cast<int64_t>(other_state->counts.size())) {
    return arrow::Status::Invalid("hash_count_all group id mapping length mismatch");
  }
  for (int64_t other_g = 0; other_g < group_id_mapping.length; ++other_g) {
    state->counts[g[other_g]] += other_state->counts[static_cast<std::size_t>(other_g)];
  }
  return arrow::Status::OK();
}

arrow::Status HashCountAllFinalize(arrow::compute::KernelContext* ctx, arrow::Datum* out) {
  if (out == nullptr) {
    return arrow::Status::Invalid("output datum must not be null");
  }
  auto* state = static_cast<HashCountAllState*>(ctx->state());
  if (state == nullptr) {
    return arrow::Status::Invalid("hash_count_all state must not be null");
  }
  ARROW_ASSIGN_OR_RAISE(
      auto data,
      arrow::AllocateBuffer(state->num_groups * static_cast<int64_t>(sizeof(uint64_t)),
                            ctx->memory_pool()));
  std::memcpy(data->mutable_data(), state->counts.data(),
              state->counts.size() * sizeof(uint64_t));
  *out = std::make_shared<arrow::UInt64Array>(state->num_groups, std::move(data));
  return arrow::Status::OK();
}

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
  const auto* options =
      static_cast<const arrow::compute::ScalarAggregateOptions*>(args.options);
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
                                   const arrow::ArraySpan& values,
                                   const uint32_t* group_ids, int64_t length) {
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
arrow::Status HashMinMaxMerge(arrow::compute::KernelContext* ctx,
                             arrow::compute::KernelState&& other,
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
    const bool is_null = (!state->options.skip_nulls && state->seen_null[static_cast<std::size_t>(i)] != 0)
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
                                         {std::move(null_bitmap), std::move(values)},
                                         null_count);
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
        state->sums[g] = AddWrapSigned(state->sums[g], static_cast<int64_t>(data[i]));
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
      state->sums[out_g] =
          AddWrapSigned(state->sums[out_g], other_state->sums[static_cast<std::size_t>(other_g)]);
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
        ARROW_ASSIGN_OR_RAISE(null_bitmap, arrow::AllocateBitmap(state->num_groups, ctx->memory_pool()));
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
                                         {std::move(null_bitmap), std::move(values)},
                                         null_count);
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

  ARROW_ASSIGN_OR_RAISE(auto ps, GetDecimalPrecisionAndScale(*args.inputs[0].type));
  const auto [out_prec, out_scale] = InferSumDecimalPrecisionAndScale(ps.first, ps.second);
  ARROW_ASSIGN_OR_RAISE(auto out_type, MakeDecimalOutType(out_prec, out_scale));
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
      return arrow::Status::Invalid("unexpected output decimal byte width: ",
                                    state->out_byte_width);
    }
  }

  auto arr_data = arrow::ArrayData::Make(state->out_type, state->num_groups,
                                         {std::move(null_bitmap), std::move(values)},
                                         null_count);
  *out = arrow::Datum(arrow::MakeArray(std::move(arr_data)));
  return arrow::Status::OK();
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

  ARROW_ASSIGN_OR_RAISE(auto ps, GetDecimalPrecisionAndScale(*args.inputs[0].type));
  const auto [out_prec, out_scale] = InferAvgDecimalPrecisionAndScale(ps.first, ps.second);
  ARROW_ASSIGN_OR_RAISE(auto out_type, MakeDecimalOutType(out_prec, out_scale));
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

    arrow::Decimal256 scaled = state->sums[static_cast<std::size_t>(i)].IncreaseScaleBy(scale_delta);
    const auto divisor = arrow::Decimal256(state->counts[static_cast<std::size_t>(i)]);
    ARROW_ASSIGN_OR_RAISE(auto qr, scaled.Divide(divisor));
    const auto& quotient = qr.first;

    const auto bytes = quotient.ToBytes();
    if (state->out_byte_width == 16) {
      std::memcpy(values->mutable_data() + i * 16, bytes.data(), 16);
    } else if (state->out_byte_width == 32) {
      std::memcpy(values->mutable_data() + i * 32, bytes.data(), 32);
    } else {
      return arrow::Status::Invalid("unexpected output decimal byte width: ",
                                    state->out_byte_width);
    }
  }

  auto arr_data = arrow::ArrayData::Make(state->out_type, state->num_groups,
                                         {std::move(null_bitmap), std::move(values)},
                                         null_count);
  *out = arrow::Datum(arrow::MakeArray(std::move(arr_data)));
  return arrow::Status::OK();
}

arrow::Result<arrow::TypeHolder> HashSumDecimalOutType(arrow::compute::KernelContext*,
                                                      const std::vector<arrow::TypeHolder>& args) {
  if (args.empty() || args[0].type == nullptr) {
    return arrow::Status::Invalid("hash_sum(decimal) out type resolver expects value type");
  }
  ARROW_ASSIGN_OR_RAISE(auto ps, GetDecimalPrecisionAndScale(*args[0].type));
  const auto [out_prec, out_scale] = InferSumDecimalPrecisionAndScale(ps.first, ps.second);
  ARROW_ASSIGN_OR_RAISE(auto out_type, MakeDecimalOutType(out_prec, out_scale));
  return arrow::TypeHolder(out_type);
}

arrow::Result<arrow::TypeHolder> HashMeanDecimalOutType(arrow::compute::KernelContext*,
                                                       const std::vector<arrow::TypeHolder>& args) {
  if (args.empty() || args[0].type == nullptr) {
    return arrow::Status::Invalid("hash_mean(decimal) out type resolver expects value type");
  }
  ARROW_ASSIGN_OR_RAISE(auto ps, GetDecimalPrecisionAndScale(*args[0].type));
  const auto [out_prec, out_scale] = InferAvgDecimalPrecisionAndScale(ps.first, ps.second);
  ARROW_ASSIGN_OR_RAISE(auto out_type, MakeDecimalOutType(out_prec, out_scale));
  return arrow::TypeHolder(out_type);
}

arrow::Status RegisterHashCount(arrow::compute::FunctionRegistry* registry) {
  static const auto default_count_options = arrow::compute::CountOptions::Defaults();
  auto func = std::make_shared<arrow::compute::HashAggregateFunction>(
      "hash_count", arrow::compute::Arity::Binary(),
      arrow::compute::FunctionDoc{"Count values in each group (TiForth override; UInt64 output)",
                                  "TiForth override: output type is UInt64 (to match TiFlash).",
                                  {"array", "group_id_array"},
                                  "CountOptions"},
      &default_count_options);

  arrow::compute::HashAggregateKernel kernel;
  kernel.signature = arrow::compute::KernelSignature::Make(
      {arrow::compute::InputType::Any(), arrow::Type::UINT32}, arrow::uint64());
  kernel.init = HashCountInit;
  kernel.resize = HashCountResize;
  kernel.consume = HashCountConsume;
  kernel.merge = HashCountMerge;
  kernel.finalize = HashCountFinalize;
  kernel.ordered = false;

  ARROW_RETURN_NOT_OK(func->AddKernel(std::move(kernel)));
  return registry->AddFunction(std::move(func), /*allow_overwrite=*/true);
}

arrow::Status RegisterHashCountAll(arrow::compute::FunctionRegistry* registry) {
  auto func = std::make_shared<arrow::compute::HashAggregateFunction>(
      "hash_count_all", arrow::compute::Arity::Unary(),
      arrow::compute::FunctionDoc{"Count rows in each group (TiForth override; UInt64 output)",
                                  "TiForth override: output type is UInt64 (to match TiFlash).",
                                  {"group_id_array"}},
      /*default_options=*/nullptr);

  arrow::compute::HashAggregateKernel kernel;
  kernel.signature =
      arrow::compute::KernelSignature::Make({arrow::Type::UINT32}, arrow::uint64());
  kernel.init = HashCountAllInit;
  kernel.resize = HashCountAllResize;
  kernel.consume = HashCountAllConsume;
  kernel.merge = HashCountAllMerge;
  kernel.finalize = HashCountAllFinalize;
  kernel.ordered = false;

  ARROW_RETURN_NOT_OK(func->AddKernel(std::move(kernel)));
  return registry->AddFunction(std::move(func), /*allow_overwrite=*/true);
}

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
    ARROW_ASSIGN_OR_RAISE(auto kernel, CopyFallbackHashKernel(fallback_registry, "hash_sum", ty));
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

arrow::Status RegisterHashMin(arrow::compute::FunctionRegistry* registry,
                             arrow::compute::FunctionRegistry* fallback_registry) {
  static const auto default_options = arrow::compute::ScalarAggregateOptions::Defaults();
  auto func = std::make_shared<arrow::compute::HashAggregateFunction>(
      "hash_min", arrow::compute::Arity::Binary(),
      arrow::compute::FunctionDoc{
          "Min values in each group (TiForth override for floats)",
          "TiForth override: float min uses `<` to match TiFlash NaN/-0 semantics.",
          {"array", "group_id_array"},
          "ScalarAggregateOptions"},
      &default_options);

  const std::vector<std::shared_ptr<arrow::DataType>> copy_types = {
      arrow::boolean(), arrow::int8(),  arrow::int16(),   arrow::int32(),   arrow::int64(),
      arrow::uint8(),   arrow::uint16(), arrow::uint32(), arrow::uint64(),  arrow::null(),
      arrow::decimal128(1, 0), arrow::decimal256(1, 0)};
  for (const auto& ty : copy_types) {
    ARROW_ASSIGN_OR_RAISE(auto kernel, CopyFallbackHashKernel(fallback_registry, "hash_min", ty));
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
      arrow::compute::FunctionDoc{
          "Max values in each group (TiForth override for floats)",
          "TiForth override: float max uses `>` to match TiFlash NaN/-0 semantics.",
          {"array", "group_id_array"},
          "ScalarAggregateOptions"},
      &default_options);

  const std::vector<std::shared_ptr<arrow::DataType>> copy_types = {
      arrow::boolean(), arrow::int8(),  arrow::int16(),   arrow::int32(),   arrow::int64(),
      arrow::uint8(),   arrow::uint16(), arrow::uint32(), arrow::uint64(),  arrow::null(),
      arrow::decimal128(1, 0), arrow::decimal256(1, 0)};
  for (const auto& ty : copy_types) {
    ARROW_ASSIGN_OR_RAISE(auto kernel, CopyFallbackHashKernel(fallback_registry, "hash_max", ty));
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
    ARROW_ASSIGN_OR_RAISE(auto kernel, CopyFallbackHashKernel(fallback_registry, "hash_mean", ty));
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

}  // namespace

arrow::Status RegisterHashAggregateFunctions(arrow::compute::FunctionRegistry* registry,
                                            arrow::compute::FunctionRegistry* fallback_registry) {
  if (registry == nullptr) {
    return arrow::Status::Invalid("function registry must not be null");
  }
  if (fallback_registry == nullptr) {
    return arrow::Status::Invalid("fallback function registry must not be null");
  }

  ARROW_RETURN_NOT_OK(RegisterHashCount(registry));
  ARROW_RETURN_NOT_OK(RegisterHashCountAll(registry));
  ARROW_RETURN_NOT_OK(RegisterHashSum(registry, fallback_registry));
  ARROW_RETURN_NOT_OK(RegisterHashMin(registry, fallback_registry));
  ARROW_RETURN_NOT_OK(RegisterHashMax(registry, fallback_registry));
  ARROW_RETURN_NOT_OK(RegisterHashMean(registry, fallback_registry));
  return arrow::Status::OK();
}

}  // namespace tiforth::functions
