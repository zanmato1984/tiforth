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

namespace tiforth::function {

namespace {

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
  std::memcpy(data->mutable_data(), state->counts.data(), state->counts.size() * sizeof(uint64_t));
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
  std::memcpy(data->mutable_data(), state->counts.data(), state->counts.size() * sizeof(uint64_t));
  *out = std::make_shared<arrow::UInt64Array>(state->num_groups, std::move(data));
  return arrow::Status::OK();
}

}  // namespace

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
  kernel.signature = arrow::compute::KernelSignature::Make({arrow::Type::UINT32}, arrow::uint64());
  kernel.init = HashCountAllInit;
  kernel.resize = HashCountAllResize;
  kernel.consume = HashCountAllConsume;
  kernel.merge = HashCountAllMerge;
  kernel.finalize = HashCountAllFinalize;
  kernel.ordered = false;

  ARROW_RETURN_NOT_OK(func->AddKernel(std::move(kernel)));
  return registry->AddFunction(std::move(func), /*allow_overwrite=*/true);
}

}  // namespace tiforth::function

