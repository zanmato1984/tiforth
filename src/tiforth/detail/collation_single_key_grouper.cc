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

#include "tiforth/detail/collation_single_key_grouper.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <string_view>
#include <utility>
#include <vector>

#include <arrow/array/array_binary.h>
#include <arrow/array/data.h>
#include <arrow/array/util.h>
#include <arrow/builder.h>
#include <arrow/memory_pool.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/util/logging.h>

namespace tiforth::detail {

namespace {

arrow::Result<std::shared_ptr<arrow::Array>> ExecSpanToArray(const arrow::compute::ExecSpan& batch) {
  if (batch.num_values() != 1) {
    return arrow::Status::Invalid("expected 1 key column, got ", batch.num_values());
  }
  const auto& value = batch[0];
  if (!value.is_array()) {
    return arrow::Status::Invalid("expected key value to be an array");
  }
  return arrow::MakeArray(value.array.ToArrayData());
}

arrow::Result<std::pair<int64_t, int64_t>> NormalizeSlice(int64_t batch_length, int64_t offset,
                                                          int64_t length) {
  if (batch_length < 0) {
    return arrow::Status::Invalid("negative batch length");
  }
  if (offset < 0 || offset > batch_length) {
    return arrow::Status::Invalid("slice offset out of range");
  }
  const int64_t out_length = (length < 0) ? (batch_length - offset) : length;
  if (out_length < 0 || offset + out_length > batch_length) {
    return arrow::Status::Invalid("slice length out of range");
  }
  return std::pair<int64_t, int64_t>{offset, out_length};
}

arrow::Result<std::shared_ptr<arrow::UInt32Array>> MakeGroupIdArrayNoNulls(arrow::MemoryPool* pool,
                                                                           int64_t length) {
  if (length < 0) {
    return arrow::Status::Invalid("negative output length");
  }
  ARROW_ASSIGN_OR_RAISE(
      auto values_unique,
      arrow::AllocateBuffer(length * static_cast<int64_t>(sizeof(uint32_t)), pool));
  std::shared_ptr<arrow::Buffer> values = std::move(values_unique);
  auto data = arrow::ArrayData::Make(arrow::uint32(), length, {nullptr, std::move(values)});
  return std::static_pointer_cast<arrow::UInt32Array>(arrow::MakeArray(std::move(data)));
}

}  // namespace

CollationSingleKeyGrouper::CollationSingleKeyGrouper(std::shared_ptr<arrow::DataType> key_type,
                                                     int32_t collation_id,
                                                     arrow::compute::ExecContext* ctx)
    : key_type_(std::move(key_type)),
      collation_(CollationFromId(collation_id)),
      pool_(ctx != nullptr ? ctx->memory_pool() : nullptr),
      arena_(std::make_unique<Arena>(pool_)),
      key_to_group_id_(pool_, /*arena=*/nullptr),
      scratch_sort_key_(pool_) {
  if (pool_ == nullptr) {
    pool_ = arrow::default_memory_pool();
  }
  ARROW_DCHECK(collation_.kind != CollationKind::kUnsupported);
  ARROW_DCHECK(key_type_ != nullptr);
  const auto id = key_type_->id();
  ARROW_DCHECK(id == arrow::Type::BINARY || id == arrow::Type::STRING);
  key_to_group_id_ = KeyHashTable(pool_, arena_.get());
}

CollationSingleKeyGrouper::~CollationSingleKeyGrouper() = default;

arrow::Status CollationSingleKeyGrouper::Reset() { return ResetImpl(); }

arrow::Status CollationSingleKeyGrouper::ResetImpl() {
  num_groups_ = 0;
  null_group_id_.reset();
  arena_ = std::make_unique<Arena>(pool_);
  key_to_group_id_ = KeyHashTable(pool_, arena_.get());
  scratch_sort_key_.Reset();
  return arrow::Status::OK();
}

uint32_t CollationSingleKeyGrouper::num_groups() const { return num_groups_; }

CollationSingleKeyGrouper::GroupKey* CollationSingleKeyGrouper::GroupKeys() {
  return reinterpret_cast<GroupKey*>(group_keys_buffer_->mutable_data());
}

const CollationSingleKeyGrouper::GroupKey* CollationSingleKeyGrouper::GroupKeys() const {
  return reinterpret_cast<const GroupKey*>(group_keys_buffer_->data());
}

arrow::Status CollationSingleKeyGrouper::EnsureGroupKeyCapacity(int64_t desired_groups) {
  if (desired_groups < 0) {
    return arrow::Status::Invalid("negative group count");
  }
  if (desired_groups == 0) {
    return arrow::Status::OK();
  }
  if (group_key_capacity_ >= desired_groups) {
    return arrow::Status::OK();
  }

  const int64_t new_capacity =
      group_key_capacity_ == 0 ? std::max<int64_t>(16, desired_groups)
                               : std::max<int64_t>(group_key_capacity_ * 2, desired_groups);
  const int64_t bytes = new_capacity * static_cast<int64_t>(sizeof(GroupKey));
  if (bytes / static_cast<int64_t>(sizeof(GroupKey)) != new_capacity) {
    return arrow::Status::Invalid("group key buffer overflow");
  }

  ARROW_ASSIGN_OR_RAISE(auto next, arrow::AllocateBuffer(bytes, pool_));
  if (group_keys_buffer_ != nullptr && num_groups_ > 0) {
    std::memcpy(next->mutable_data(), group_keys_buffer_->data(),
                static_cast<std::size_t>(num_groups_) * sizeof(GroupKey));
  }
  group_keys_buffer_ = std::move(next);
  group_key_capacity_ = new_capacity;
  return arrow::Status::OK();
}

arrow::Result<uint32_t> CollationSingleKeyGrouper::AppendNullGroup() {
  if (null_group_id_.has_value()) {
    return *null_group_id_;
  }
  if (num_groups_ == std::numeric_limits<uint32_t>::max()) {
    return arrow::Status::NotImplemented("too many groups for uint32 output");
  }
  const uint32_t group_id = num_groups_;
  ARROW_RETURN_NOT_OK(EnsureGroupKeyCapacity(static_cast<int64_t>(group_id) + 1));
  auto* keys = GroupKeys();
  keys[group_id].is_null = 1;
  keys[group_id].original = ByteSlice{};
  null_group_id_ = group_id;
  ++num_groups_;
  return group_id;
}

arrow::Result<uint32_t> CollationSingleKeyGrouper::AppendGroupKey(std::string_view original) {
  if (num_groups_ == std::numeric_limits<uint32_t>::max()) {
    return arrow::Status::NotImplemented("too many groups for uint32 output");
  }
  if (arena_ == nullptr) {
    return arrow::Status::Invalid("arena must not be null");
  }
  const uint32_t group_id = num_groups_;
  ARROW_RETURN_NOT_OK(EnsureGroupKeyCapacity(static_cast<int64_t>(group_id) + 1));
  const auto* data = reinterpret_cast<const uint8_t*>(original.data());
  const auto size = static_cast<int32_t>(original.size());
  ARROW_ASSIGN_OR_RAISE(const auto* stored, arena_->Append(data, size));
  auto* keys = GroupKeys();
  keys[group_id].is_null = 0;
  keys[group_id].original = ByteSlice{stored, size};
  ++num_groups_;
  return group_id;
}

arrow::Result<std::string_view> CollationSingleKeyGrouper::SortKey(std::string_view original) {
  scratch_sort_key_.Reset();
  SortKeyStringTo(collation_, original, &scratch_sort_key_);
  ARROW_RETURN_NOT_OK(scratch_sort_key_.status());
  return scratch_sort_key_.view();
}

arrow::Result<arrow::Datum> CollationSingleKeyGrouper::ConsumeOrLookupImpl(
    const arrow::compute::ExecSpan& batch, bool insert, int64_t offset, int64_t length) {
  ARROW_ASSIGN_OR_RAISE(auto array, ExecSpanToArray(batch));
  ARROW_ASSIGN_OR_RAISE(auto slice, NormalizeSlice(array->length(), offset, length));
  const int64_t start = slice.first;
  const int64_t out_length = slice.second;

  std::shared_ptr<arrow::BinaryArray> bin;
  std::shared_ptr<arrow::StringArray> str;
  if (array->type() == nullptr) {
    return arrow::Status::Invalid("key array type must not be null");
  }
  switch (array->type()->id()) {
    case arrow::Type::BINARY:
      bin = std::dynamic_pointer_cast<arrow::BinaryArray>(array);
      break;
    case arrow::Type::STRING:
      str = std::dynamic_pointer_cast<arrow::StringArray>(array);
      break;
    default:
      return arrow::Status::Invalid("unsupported key type: ", array->type()->ToString());
  }
  if (bin == nullptr && str == nullptr) {
    return arrow::Status::Invalid("failed to cast key array");
  }

  if (insert) {
    ARROW_ASSIGN_OR_RAISE(auto out_ids, MakeGroupIdArrayNoNulls(pool_, out_length));
    auto* out_values =
        reinterpret_cast<uint32_t*>(out_ids->data()->buffers[1]->mutable_data());
    for (int64_t i = 0; i < out_length; ++i) {
      const int64_t row = start + i;
      const bool is_null = (bin != nullptr) ? bin->IsNull(row) : str->IsNull(row);
      if (is_null) {
        ARROW_ASSIGN_OR_RAISE(out_values[i], AppendNullGroup());
        continue;
      }
      const auto raw = (bin != nullptr) ? bin->GetView(row) : str->GetView(row);
      ARROW_ASSIGN_OR_RAISE(const auto norm, SortKey(raw));
      const auto* norm_data = reinterpret_cast<const uint8_t*>(norm.data());
      const auto norm_size = static_cast<int32_t>(norm.size());
      ARROW_ASSIGN_OR_RAISE(const auto found,
                            key_to_group_id_.FindOrInsert(norm_data, norm_size,
                                                          HashBytes(norm_data, norm_size),
                                                          num_groups_));
      const uint32_t gid = found.first;
      if (found.second) {
        ARROW_ASSIGN_OR_RAISE(const auto appended, AppendGroupKey(raw));
        if (appended != gid) {
          return arrow::Status::Invalid("group id mismatch");
        }
      }
      out_values[i] = gid;
    }
    return arrow::Datum(out_ids->data());
  }

  arrow::UInt32Builder builder(pool_);
  ARROW_RETURN_NOT_OK(builder.Reserve(out_length));
  for (int64_t i = 0; i < out_length; ++i) {
    const int64_t row = start + i;
    const bool is_null = (bin != nullptr) ? bin->IsNull(row) : str->IsNull(row);
    if (is_null) {
      if (null_group_id_.has_value()) {
        ARROW_RETURN_NOT_OK(builder.Append(*null_group_id_));
      } else {
        ARROW_RETURN_NOT_OK(builder.AppendNull());
      }
      continue;
    }
    const auto raw = (bin != nullptr) ? bin->GetView(row) : str->GetView(row);
    ARROW_ASSIGN_OR_RAISE(const auto norm, SortKey(raw));
    const auto* norm_data = reinterpret_cast<const uint8_t*>(norm.data());
    const auto norm_size = static_cast<int32_t>(norm.size());
    ARROW_ASSIGN_OR_RAISE(const auto found,
                          key_to_group_id_.Find(norm_data, norm_size,
                                                HashBytes(norm_data, norm_size)));
    if (found.has_value()) {
      ARROW_RETURN_NOT_OK(builder.Append(*found));
    } else {
      ARROW_RETURN_NOT_OK(builder.AppendNull());
    }
  }
  std::shared_ptr<arrow::Array> out;
  ARROW_RETURN_NOT_OK(builder.Finish(&out));
  if (out == nullptr || out->data() == nullptr) {
    return arrow::Status::Invalid("failed to build group id array");
  }
  return arrow::Datum(out->data());
}

arrow::Status CollationSingleKeyGrouper::PopulateImpl(const arrow::compute::ExecSpan& batch,
                                                      int64_t offset, int64_t length) {
  ARROW_ASSIGN_OR_RAISE(auto array, ExecSpanToArray(batch));
  ARROW_ASSIGN_OR_RAISE(auto slice, NormalizeSlice(array->length(), offset, length));
  const int64_t start = slice.first;
  const int64_t out_length = slice.second;

  std::shared_ptr<arrow::BinaryArray> bin;
  std::shared_ptr<arrow::StringArray> str;
  if (array->type() == nullptr) {
    return arrow::Status::Invalid("key array type must not be null");
  }
  switch (array->type()->id()) {
    case arrow::Type::BINARY:
      bin = std::dynamic_pointer_cast<arrow::BinaryArray>(array);
      break;
    case arrow::Type::STRING:
      str = std::dynamic_pointer_cast<arrow::StringArray>(array);
      break;
    default:
      return arrow::Status::Invalid("unsupported key type: ", array->type()->ToString());
  }
  if (bin == nullptr && str == nullptr) {
    return arrow::Status::Invalid("failed to cast key array");
  }

  for (int64_t i = 0; i < out_length; ++i) {
    const int64_t row = start + i;
    const bool is_null = (bin != nullptr) ? bin->IsNull(row) : str->IsNull(row);
    if (is_null) {
      ARROW_RETURN_NOT_OK(AppendNullGroup());
      continue;
    }
    const auto raw = (bin != nullptr) ? bin->GetView(row) : str->GetView(row);
    ARROW_ASSIGN_OR_RAISE(const auto norm, SortKey(raw));
    const auto* norm_data = reinterpret_cast<const uint8_t*>(norm.data());
    const auto norm_size = static_cast<int32_t>(norm.size());
    ARROW_ASSIGN_OR_RAISE(const auto found,
                          key_to_group_id_.FindOrInsert(norm_data, norm_size,
                                                        HashBytes(norm_data, norm_size),
                                                        num_groups_));
    if (found.second) {
      ARROW_ASSIGN_OR_RAISE(const auto appended, AppendGroupKey(raw));
      if (appended != found.first) {
        return arrow::Status::Invalid("group id mismatch");
      }
    }
  }
  return arrow::Status::OK();
}

arrow::Result<arrow::Datum> CollationSingleKeyGrouper::Consume(const arrow::compute::ExecSpan& batch,
                                                               int64_t offset, int64_t length) {
  return ConsumeOrLookupImpl(batch, /*insert=*/true, offset, length);
}

arrow::Result<arrow::Datum> CollationSingleKeyGrouper::Lookup(const arrow::compute::ExecSpan& batch,
                                                              int64_t offset, int64_t length) {
  return ConsumeOrLookupImpl(batch, /*insert=*/false, offset, length);
}

arrow::Status CollationSingleKeyGrouper::Populate(const arrow::compute::ExecSpan& batch,
                                                  int64_t offset, int64_t length) {
  return PopulateImpl(batch, offset, length);
}

arrow::Result<arrow::compute::ExecBatch> CollationSingleKeyGrouper::GetUniques() {
  const int64_t num_groups = static_cast<int64_t>(num_groups_);
  if (group_keys_buffer_ == nullptr || num_groups == 0) {
    if (key_type_ == nullptr) {
      return arrow::Status::Invalid("key type must not be null");
    }
    if (key_type_->id() == arrow::Type::BINARY) {
      arrow::BinaryBuilder builder(pool_);
      std::shared_ptr<arrow::Array> out;
      ARROW_RETURN_NOT_OK(builder.Finish(&out));
      return arrow::compute::ExecBatch({out}, /*length=*/0);
    }
    if (key_type_->id() == arrow::Type::STRING) {
      arrow::StringBuilder builder(pool_);
      std::shared_ptr<arrow::Array> out;
      ARROW_RETURN_NOT_OK(builder.Finish(&out));
      return arrow::compute::ExecBatch({out}, /*length=*/0);
    }
    return arrow::Status::Invalid("unsupported key type for uniques: ", key_type_->ToString());
  }

  if (key_type_ == nullptr) {
    return arrow::Status::Invalid("key type must not be null");
  }

  const auto* keys = GroupKeys();
  if (keys == nullptr) {
    return arrow::Status::Invalid("group keys buffer must not be null");
  }

  if (key_type_->id() == arrow::Type::BINARY) {
    arrow::BinaryBuilder builder(pool_);
    ARROW_RETURN_NOT_OK(builder.Reserve(num_groups));
    for (int64_t i = 0; i < num_groups; ++i) {
      if (keys[i].is_null != 0) {
        ARROW_RETURN_NOT_OK(builder.AppendNull());
        continue;
      }
      const auto& v = keys[i].original;
      if (v.size <= 0) {
        ARROW_RETURN_NOT_OK(builder.Append(""));
        continue;
      }
      if (v.data == nullptr) {
        return arrow::Status::Invalid("original key data must not be null");
      }
      ARROW_RETURN_NOT_OK(builder.Append(reinterpret_cast<const char*>(v.data), v.size));
    }
    std::shared_ptr<arrow::Array> out;
    ARROW_RETURN_NOT_OK(builder.Finish(&out));
    return arrow::compute::ExecBatch({out}, num_groups);
  }

  if (key_type_->id() == arrow::Type::STRING) {
    arrow::StringBuilder builder(pool_);
    ARROW_RETURN_NOT_OK(builder.Reserve(num_groups));
    for (int64_t i = 0; i < num_groups; ++i) {
      if (keys[i].is_null != 0) {
        ARROW_RETURN_NOT_OK(builder.AppendNull());
        continue;
      }
      const auto& v = keys[i].original;
      if (v.size <= 0) {
        ARROW_RETURN_NOT_OK(builder.Append(""));
        continue;
      }
      if (v.data == nullptr) {
        return arrow::Status::Invalid("original key data must not be null");
      }
      ARROW_RETURN_NOT_OK(builder.Append(reinterpret_cast<const char*>(v.data), v.size));
    }
    std::shared_ptr<arrow::Array> out;
    ARROW_RETURN_NOT_OK(builder.Finish(&out));
    return arrow::compute::ExecBatch({out}, num_groups);
  }

  return arrow::Status::Invalid("unsupported key type for uniques: ", key_type_->ToString());
}

}  // namespace tiforth::detail
