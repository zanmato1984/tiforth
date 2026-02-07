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

#include "tiforth/detail/small_string_single_key_grouper.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
#include <utility>
#include <vector>

#include <arrow/array/array_binary.h>
#include <arrow/array/data.h>
#include <arrow/array/util.h>
#include <arrow/buffer.h>
#include <arrow/builder.h>
#include <arrow/memory_pool.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/util/logging.h>

#include "tiforth/detail/arena.h"
#include "tiforth/detail/key_hash_table.h"

namespace tiforth::detail {

namespace {

constexpr uint32_t kEmptyGroupId = std::numeric_limits<uint32_t>::max();

int64_t RoundUpToPowerOfTwo(int64_t v) {
  if (v <= 1) {
    return 1;
  }
  uint64_t x = static_cast<uint64_t>(v - 1);
  x |= x >> 1;
  x |= x >> 2;
  x |= x >> 4;
  x |= x >> 8;
  x |= x >> 16;
  x |= x >> 32;
  x += 1;
  if (x > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
    return std::numeric_limits<int64_t>::max();
  }
  return static_cast<int64_t>(x);
}

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

SmallStringSingleKeyGrouper::SmallStringSingleKeyGrouper(std::shared_ptr<arrow::DataType> key_type,
                                                         arrow::compute::ExecContext* ctx)
    : key_type_(std::move(key_type)),
      pool_(ctx != nullptr ? ctx->memory_pool() : nullptr),
      arena_(std::make_unique<Arena>(pool_)) {
  if (pool_ == nullptr) {
    pool_ = arrow::default_memory_pool();
  }
}

SmallStringSingleKeyGrouper::~SmallStringSingleKeyGrouper() = default;

arrow::Status SmallStringSingleKeyGrouper::Reset() { return ResetImpl(); }

arrow::Status SmallStringSingleKeyGrouper::ResetImpl() {
  table_size_ = 0;
  num_groups_ = 0;
  null_group_id_.reset();
  if (entries_buffer_ != nullptr && table_capacity_ > 0) {
    std::memset(entries_buffer_->mutable_data(), 0xFF,
                static_cast<std::size_t>(table_capacity_) * sizeof(Entry));
  }
  arena_ = std::make_unique<Arena>(pool_);
  return arrow::Status::OK();
}

uint32_t SmallStringSingleKeyGrouper::num_groups() const { return num_groups_; }

SmallStringSingleKeyGrouper::Entry* SmallStringSingleKeyGrouper::Entries() {
  return reinterpret_cast<Entry*>(entries_buffer_->mutable_data());
}

const SmallStringSingleKeyGrouper::Entry* SmallStringSingleKeyGrouper::Entries() const {
  return reinterpret_cast<const Entry*>(entries_buffer_->data());
}

SmallStringSingleKeyGrouper::GroupKey* SmallStringSingleKeyGrouper::GroupKeys() {
  return reinterpret_cast<GroupKey*>(group_keys_buffer_->mutable_data());
}

const SmallStringSingleKeyGrouper::GroupKey* SmallStringSingleKeyGrouper::GroupKeys() const {
  return reinterpret_cast<const GroupKey*>(group_keys_buffer_->data());
}

arrow::Status SmallStringSingleKeyGrouper::EnsureTableInitialized(int64_t capacity) {
  if (capacity <= 0) {
    return arrow::Status::Invalid("invalid table capacity");
  }
  capacity = std::max<int64_t>(capacity, 16);
  capacity = RoundUpToPowerOfTwo(capacity);
  if (capacity <= 0) {
    return arrow::Status::Invalid("table capacity overflow");
  }
  const int64_t bytes = capacity * static_cast<int64_t>(sizeof(Entry));
  if (bytes / static_cast<int64_t>(sizeof(Entry)) != capacity) {
    return arrow::Status::Invalid("table size overflow");
  }
  ARROW_ASSIGN_OR_RAISE(entries_buffer_, arrow::AllocateBuffer(bytes, pool_));
  std::memset(entries_buffer_->mutable_data(), 0xFF, static_cast<std::size_t>(bytes));
  table_capacity_ = capacity;
  table_size_ = 0;
  table_max_load_ = static_cast<int64_t>(static_cast<double>(table_capacity_) * 0.7);
  if (table_max_load_ < 1) {
    table_max_load_ = 1;
  }
  return arrow::Status::OK();
}

arrow::Status SmallStringSingleKeyGrouper::EnsureTableCapacityForInsert() {
  if (table_capacity_ == 0) {
    return EnsureTableInitialized(/*capacity=*/16);
  }
  if (table_size_ + 1 <= table_max_load_) {
    return arrow::Status::OK();
  }
  return Rehash(table_capacity_ * 2);
}

arrow::Status SmallStringSingleKeyGrouper::Rehash(int64_t new_capacity) {
  if (new_capacity <= table_capacity_) {
    return arrow::Status::OK();
  }
  new_capacity = std::max<int64_t>(new_capacity, 16);
  new_capacity = RoundUpToPowerOfTwo(new_capacity);
  if (new_capacity <= 0) {
    return arrow::Status::Invalid("rehash capacity overflow");
  }

  std::unique_ptr<arrow::Buffer> next_buffer;
  {
    const int64_t bytes = new_capacity * static_cast<int64_t>(sizeof(Entry));
    if (bytes / static_cast<int64_t>(sizeof(Entry)) != new_capacity) {
      return arrow::Status::Invalid("rehash size overflow");
    }
    ARROW_ASSIGN_OR_RAISE(next_buffer, arrow::AllocateBuffer(bytes, pool_));
    std::memset(next_buffer->mutable_data(), 0xFF, static_cast<std::size_t>(bytes));
  }

  auto* next_entries = reinterpret_cast<Entry*>(next_buffer->mutable_data());
  const auto* old_entries = Entries();
  const int64_t old_capacity = table_capacity_;

  const int64_t mask = new_capacity - 1;
  for (int64_t i = 0; i < old_capacity; ++i) {
    const auto& e = old_entries[i];
    if (e.group_id == kEmptyGroupId) {
      continue;
    }
    int64_t idx = static_cast<int64_t>(e.hash) & mask;
    while (true) {
      auto& dst = next_entries[idx];
      if (dst.group_id == kEmptyGroupId) {
        dst = e;
        break;
      }
      idx = (idx + 1) & mask;
    }
  }

  entries_buffer_ = std::move(next_buffer);
  table_capacity_ = new_capacity;
  table_max_load_ = static_cast<int64_t>(static_cast<double>(table_capacity_) * 0.7);
  if (table_max_load_ < 1) {
    table_max_load_ = 1;
  }
  return arrow::Status::OK();
}

arrow::Status SmallStringSingleKeyGrouper::EnsureGroupKeyCapacity(int64_t desired_groups) {
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

arrow::Result<uint32_t> SmallStringSingleKeyGrouper::AppendNullGroup() {
  if (null_group_id_.has_value()) {
    return *null_group_id_;
  }
  if (num_groups_ == kEmptyGroupId) {
    return arrow::Status::NotImplemented("too many groups for uint32 output");
  }
  const uint32_t group_id = num_groups_;
  ARROW_RETURN_NOT_OK(EnsureGroupKeyCapacity(static_cast<int64_t>(group_id) + 1));
  auto* keys = GroupKeys();
  auto& dst = keys[group_id];
  dst.is_null = 1;
  dst.is_inline = 0;
  dst.length = 0;
  dst.data = nullptr;
  std::memset(dst.inline_bytes, 0, kInlineBytesSize);
  null_group_id_ = group_id;
  ++num_groups_;
  return group_id;
}

arrow::Result<uint32_t> SmallStringSingleKeyGrouper::AppendGroupKey(const uint8_t* data,
                                                                    int32_t length) {
  if (length < 0) {
    return arrow::Status::Invalid("negative key length");
  }
  if (length > 0 && data == nullptr) {
    return arrow::Status::Invalid("key data must not be null for non-zero length");
  }
  if (num_groups_ == kEmptyGroupId) {
    return arrow::Status::NotImplemented("too many groups for uint32 output");
  }
  const uint32_t group_id = num_groups_;
  ARROW_RETURN_NOT_OK(EnsureGroupKeyCapacity(static_cast<int64_t>(group_id) + 1));
  auto* keys = GroupKeys();
  auto& dst = keys[group_id];
  dst.is_null = 0;
  dst.length = length;
  if (length <= kInlineBytes) {
    dst.is_inline = 1;
    dst.data = nullptr;
    if (length > 0) {
      std::memcpy(dst.inline_bytes, data, static_cast<std::size_t>(length));
    }
    if (length < kInlineBytes) {
      std::memset(dst.inline_bytes + length, 0,
                  static_cast<std::size_t>(kInlineBytes - length));
    }
  } else {
    dst.is_inline = 0;
    if (arena_ == nullptr) {
      return arrow::Status::Invalid("arena must not be null");
    }
    ARROW_ASSIGN_OR_RAISE(const auto* stored, arena_->Append(data, length));
    dst.data = stored;
    std::memset(dst.inline_bytes, 0, kInlineBytesSize);
  }
  ++num_groups_;
  return group_id;
}

bool SmallStringSingleKeyGrouper::KeyEquals(uint32_t group_id, const uint8_t* data,
                                            int32_t length) const {
  ARROW_DCHECK(group_id != kEmptyGroupId);
  ARROW_DCHECK(group_id < num_groups_);
  const auto* keys = GroupKeys();
  const auto& stored = keys[group_id];
  if (stored.is_null != 0) {
    return false;
  }
  if (stored.length != length) {
    return false;
  }
  if (length == 0) {
    return true;
  }
  const uint8_t* lhs = (stored.is_inline != 0) ? stored.inline_bytes : stored.data;
  if (lhs == nullptr) {
    return false;
  }
  return std::memcmp(lhs, data, static_cast<std::size_t>(length)) == 0;
}

arrow::Result<std::optional<uint32_t>> SmallStringSingleKeyGrouper::Find(const uint8_t* data,
                                                                         int32_t length,
                                                                         uint64_t hash) const {
  if (length < 0) {
    return arrow::Status::Invalid("negative key length");
  }
  if (length > 0 && data == nullptr) {
    return arrow::Status::Invalid("key data must not be null for non-zero length");
  }
  if (table_capacity_ == 0) {
    return std::optional<uint32_t>{};
  }

  const int64_t mask = table_capacity_ - 1;
  int64_t idx = static_cast<int64_t>(hash) & mask;
  const auto* entries = Entries();
  while (true) {
    const auto& e = entries[idx];
    if (e.group_id == kEmptyGroupId) {
      return std::optional<uint32_t>{};
    }
    if (e.hash == hash && KeyEquals(e.group_id, data, length)) {
      return std::optional<uint32_t>{e.group_id};
    }
    idx = (idx + 1) & mask;
  }
}

arrow::Result<uint32_t> SmallStringSingleKeyGrouper::FindOrInsert(const uint8_t* data,
                                                                  int32_t length, uint64_t hash) {
  if (length < 0) {
    return arrow::Status::Invalid("negative key length");
  }
  if (length > 0 && data == nullptr) {
    return arrow::Status::Invalid("key data must not be null for non-zero length");
  }

  if (table_capacity_ == 0) {
    ARROW_RETURN_NOT_OK(EnsureTableInitialized(/*capacity=*/16));
  }

  while (true) {
    const int64_t mask = table_capacity_ - 1;
    int64_t idx = static_cast<int64_t>(hash) & mask;
    auto* entries = Entries();
    while (true) {
      auto& e = entries[idx];
      if (e.group_id == kEmptyGroupId) {
        break;
      }
      if (e.hash == hash && KeyEquals(e.group_id, data, length)) {
        return e.group_id;
      }
      idx = (idx + 1) & mask;
    }

    ARROW_RETURN_NOT_OK(EnsureTableCapacityForInsert());
    const int64_t new_mask = table_capacity_ - 1;
    int64_t ins_idx = static_cast<int64_t>(hash) & new_mask;
    entries = Entries();
    while (true) {
      auto& e = entries[ins_idx];
      if (e.group_id == kEmptyGroupId) {
        break;
      }
      if (e.hash == hash && KeyEquals(e.group_id, data, length)) {
        return e.group_id;
      }
      ins_idx = (ins_idx + 1) & new_mask;
    }

    ARROW_ASSIGN_OR_RAISE(const uint32_t group_id, AppendGroupKey(data, length));
    entries[ins_idx].hash = hash;
    entries[ins_idx].group_id = group_id;
    ++table_size_;
    return group_id;
  }
}

arrow::Result<arrow::Datum> SmallStringSingleKeyGrouper::ConsumeOrLookupImpl(
    const arrow::compute::ExecSpan& batch, bool insert, int64_t offset, int64_t length) {
  ARROW_ASSIGN_OR_RAISE(const auto slice, NormalizeSlice(batch.length, offset, length));
  const int64_t start = slice.first;
  const int64_t out_length = slice.second;

  ARROW_ASSIGN_OR_RAISE(auto key_array_any, ExecSpanToArray(batch));
  if (key_array_any == nullptr) {
    return arrow::Status::Invalid("key array must not be null");
  }
  if (key_type_ != nullptr && !key_array_any->type()->Equals(*key_type_)) {
    return arrow::Status::Invalid("key type mismatch: expected=", key_type_->ToString(),
                                  " got=", key_array_any->type()->ToString());
  }

  if (!insert) {
    arrow::UInt32Builder builder(pool_);
    ARROW_RETURN_NOT_OK(builder.Reserve(out_length));

    const auto append_id_or_null = [&](bool is_null_key, const uint8_t* data_ptr,
                                       int32_t data_len) -> arrow::Status {
      if (is_null_key) {
        if (!null_group_id_.has_value()) {
          return builder.AppendNull();
        }
        return builder.Append(*null_group_id_);
      }
      const uint64_t h = HashBytes(data_ptr, data_len);
      ARROW_ASSIGN_OR_RAISE(const auto maybe_id, Find(data_ptr, data_len, h));
      if (!maybe_id.has_value()) {
        return builder.AppendNull();
      }
      return builder.Append(*maybe_id);
    };

    if (key_array_any->type_id() == arrow::Type::BINARY) {
      const auto& key_array = static_cast<const arrow::BinaryArray&>(*key_array_any);
      for (int64_t i = start; i < start + out_length; ++i) {
        if (key_array.IsNull(i)) {
          ARROW_RETURN_NOT_OK(append_id_or_null(/*is_null_key=*/true, nullptr, 0));
          continue;
        }
        const int32_t len = static_cast<int32_t>(key_array.value_length(i));
        const auto* data_ptr = key_array.raw_data() + key_array.value_offset(i);
        ARROW_RETURN_NOT_OK(append_id_or_null(/*is_null_key=*/false, data_ptr, len));
      }
    } else if (key_array_any->type_id() == arrow::Type::STRING) {
      const auto& key_array = static_cast<const arrow::StringArray&>(*key_array_any);
      for (int64_t i = start; i < start + out_length; ++i) {
        if (key_array.IsNull(i)) {
          ARROW_RETURN_NOT_OK(append_id_or_null(/*is_null_key=*/true, nullptr, 0));
          continue;
        }
        const int32_t len = static_cast<int32_t>(key_array.value_length(i));
        const auto* data_ptr = reinterpret_cast<const uint8_t*>(key_array.raw_data() + key_array.value_offset(i));
        ARROW_RETURN_NOT_OK(append_id_or_null(/*is_null_key=*/false, data_ptr, len));
      }
    } else {
      return arrow::Status::NotImplemented("SmallStringSingleKeyGrouper does not support type ",
                                           key_array_any->type()->ToString());
    }

    std::shared_ptr<arrow::Array> out;
    ARROW_RETURN_NOT_OK(builder.Finish(&out));
    return arrow::Datum(std::move(out));
  }

  ARROW_ASSIGN_OR_RAISE(auto out_ids, MakeGroupIdArrayNoNulls(pool_, out_length));
  auto* out_values = out_ids->data()->GetMutableValues<uint32_t>(1);

  const auto consume_value = [&](bool is_null_key, const uint8_t* data_ptr, int32_t data_len,
                                 uint32_t* out_id) -> arrow::Status {
    if (out_id == nullptr) {
      return arrow::Status::Invalid("output id pointer must not be null");
    }
    if (is_null_key) {
      ARROW_ASSIGN_OR_RAISE(*out_id, AppendNullGroup());
      return arrow::Status::OK();
    }
    const uint64_t h = HashBytes(data_ptr, data_len);
    ARROW_ASSIGN_OR_RAISE(*out_id, FindOrInsert(data_ptr, data_len, h));
    return arrow::Status::OK();
  };

  if (key_array_any->type_id() == arrow::Type::BINARY) {
    const auto& key_array = static_cast<const arrow::BinaryArray&>(*key_array_any);
    for (int64_t i = 0; i < out_length; ++i) {
      const int64_t row = start + i;
      if (key_array.IsNull(row)) {
        ARROW_RETURN_NOT_OK(consume_value(/*is_null_key=*/true, nullptr, 0, &out_values[i]));
        continue;
      }
      const int32_t len = static_cast<int32_t>(key_array.value_length(row));
      const auto* data_ptr = key_array.raw_data() + key_array.value_offset(row);
      ARROW_RETURN_NOT_OK(consume_value(/*is_null_key=*/false, data_ptr, len, &out_values[i]));
    }
  } else if (key_array_any->type_id() == arrow::Type::STRING) {
    const auto& key_array = static_cast<const arrow::StringArray&>(*key_array_any);
    for (int64_t i = 0; i < out_length; ++i) {
      const int64_t row = start + i;
      if (key_array.IsNull(row)) {
        ARROW_RETURN_NOT_OK(consume_value(/*is_null_key=*/true, nullptr, 0, &out_values[i]));
        continue;
      }
      const int32_t len = static_cast<int32_t>(key_array.value_length(row));
      const auto* data_ptr =
          reinterpret_cast<const uint8_t*>(key_array.raw_data() + key_array.value_offset(row));
      ARROW_RETURN_NOT_OK(consume_value(/*is_null_key=*/false, data_ptr, len, &out_values[i]));
    }
  } else {
    return arrow::Status::NotImplemented("SmallStringSingleKeyGrouper does not support type ",
                                         key_array_any->type()->ToString());
  }

  return arrow::Datum(std::move(out_ids));
}

arrow::Status SmallStringSingleKeyGrouper::PopulateImpl(const arrow::compute::ExecSpan& batch,
                                                        int64_t offset, int64_t length) {
  ARROW_ASSIGN_OR_RAISE(const auto slice, NormalizeSlice(batch.length, offset, length));
  const int64_t start = slice.first;
  const int64_t out_length = slice.second;
  if (out_length == 0) {
    return arrow::Status::OK();
  }

  ARROW_ASSIGN_OR_RAISE(auto key_array_any, ExecSpanToArray(batch));
  if (key_array_any == nullptr) {
    return arrow::Status::Invalid("key array must not be null");
  }

  const auto consume_value = [&](bool is_null_key, const uint8_t* data_ptr,
                                 int32_t data_len) -> arrow::Status {
    if (is_null_key) {
      ARROW_ASSIGN_OR_RAISE(auto ignored, AppendNullGroup());
      (void)ignored;
      return arrow::Status::OK();
    }
    const uint64_t h = HashBytes(data_ptr, data_len);
    ARROW_ASSIGN_OR_RAISE(auto ignored, FindOrInsert(data_ptr, data_len, h));
    (void)ignored;
    return arrow::Status::OK();
  };

  if (key_array_any->type_id() == arrow::Type::BINARY) {
    const auto& key_array = static_cast<const arrow::BinaryArray&>(*key_array_any);
    for (int64_t row = start; row < start + out_length; ++row) {
      if (key_array.IsNull(row)) {
        ARROW_RETURN_NOT_OK(consume_value(/*is_null_key=*/true, nullptr, 0));
        continue;
      }
      const int32_t len = static_cast<int32_t>(key_array.value_length(row));
      const auto* data_ptr = key_array.raw_data() + key_array.value_offset(row);
      ARROW_RETURN_NOT_OK(consume_value(/*is_null_key=*/false, data_ptr, len));
    }
  } else if (key_array_any->type_id() == arrow::Type::STRING) {
    const auto& key_array = static_cast<const arrow::StringArray&>(*key_array_any);
    for (int64_t row = start; row < start + out_length; ++row) {
      if (key_array.IsNull(row)) {
        ARROW_RETURN_NOT_OK(consume_value(/*is_null_key=*/true, nullptr, 0));
        continue;
      }
      const int32_t len = static_cast<int32_t>(key_array.value_length(row));
      const auto* data_ptr =
          reinterpret_cast<const uint8_t*>(key_array.raw_data() + key_array.value_offset(row));
      ARROW_RETURN_NOT_OK(consume_value(/*is_null_key=*/false, data_ptr, len));
    }
  } else {
    return arrow::Status::NotImplemented("SmallStringSingleKeyGrouper does not support type ",
                                         key_array_any->type()->ToString());
  }

  return arrow::Status::OK();
}

arrow::Result<arrow::Datum> SmallStringSingleKeyGrouper::Consume(const arrow::compute::ExecSpan& batch,
                                                                 int64_t offset, int64_t length) {
  return ConsumeOrLookupImpl(batch, /*insert=*/true, offset, length);
}

arrow::Result<arrow::Datum> SmallStringSingleKeyGrouper::Lookup(const arrow::compute::ExecSpan& batch,
                                                                int64_t offset, int64_t length) {
  return ConsumeOrLookupImpl(batch, /*insert=*/false, offset, length);
}

arrow::Status SmallStringSingleKeyGrouper::Populate(const arrow::compute::ExecSpan& batch,
                                                    int64_t offset, int64_t length) {
  return PopulateImpl(batch, offset, length);
}

arrow::Result<arrow::compute::ExecBatch> SmallStringSingleKeyGrouper::GetUniques() {
  if (key_type_ == nullptr) {
    return arrow::Status::Invalid("key type must not be null");
  }
  if (num_groups_ == 0) {
    if (key_type_->id() == arrow::Type::BINARY) {
      arrow::BinaryBuilder builder(pool_);
      std::shared_ptr<arrow::Array> out;
      ARROW_RETURN_NOT_OK(builder.Finish(&out));
      return arrow::compute::ExecBatch({arrow::Datum(std::move(out))}, 0);
    }
    if (key_type_->id() == arrow::Type::STRING) {
      arrow::StringBuilder builder(pool_);
      std::shared_ptr<arrow::Array> out;
      ARROW_RETURN_NOT_OK(builder.Finish(&out));
      return arrow::compute::ExecBatch({arrow::Datum(std::move(out))}, 0);
    }
    ARROW_ASSIGN_OR_RAISE(auto out, arrow::MakeArrayOfNull(key_type_, 0, pool_));
    return arrow::compute::ExecBatch({arrow::Datum(std::move(out))}, 0);
  }
  if (group_keys_buffer_ == nullptr) {
    return arrow::Status::Invalid("group key buffer must not be null");
  }

  if (key_type_->id() == arrow::Type::BINARY) {
    arrow::BinaryBuilder builder(pool_);
    ARROW_RETURN_NOT_OK(builder.Reserve(num_groups_));
    const auto* keys = GroupKeys();
    for (uint32_t i = 0; i < num_groups_; ++i) {
      const auto& k = keys[i];
      if (k.is_null != 0) {
        ARROW_RETURN_NOT_OK(builder.AppendNull());
        continue;
      }
      const uint8_t* data_ptr = (k.is_inline != 0) ? k.inline_bytes : k.data;
      if (k.length > 0 && data_ptr == nullptr) {
        return arrow::Status::Invalid("group key data must not be null");
      }
      ARROW_RETURN_NOT_OK(builder.Append(data_ptr, k.length));
    }
    std::shared_ptr<arrow::Array> out;
    ARROW_RETURN_NOT_OK(builder.Finish(&out));
    return arrow::compute::ExecBatch({arrow::Datum(std::move(out))},
                                     static_cast<int64_t>(num_groups_));
  }

  if (key_type_->id() == arrow::Type::STRING) {
    arrow::StringBuilder builder(pool_);
    ARROW_RETURN_NOT_OK(builder.Reserve(num_groups_));
    const auto* keys = GroupKeys();
    for (uint32_t i = 0; i < num_groups_; ++i) {
      const auto& k = keys[i];
      if (k.is_null != 0) {
        ARROW_RETURN_NOT_OK(builder.AppendNull());
        continue;
      }
      const uint8_t* data_ptr = (k.is_inline != 0) ? k.inline_bytes : k.data;
      if (k.length > 0 && data_ptr == nullptr) {
        return arrow::Status::Invalid("group key data must not be null");
      }
      ARROW_RETURN_NOT_OK(
          builder.Append(reinterpret_cast<const char*>(data_ptr), static_cast<int32_t>(k.length)));
    }
    std::shared_ptr<arrow::Array> out;
    ARROW_RETURN_NOT_OK(builder.Finish(&out));
    return arrow::compute::ExecBatch({arrow::Datum(std::move(out))},
                                     static_cast<int64_t>(num_groups_));
  }

  return arrow::Status::NotImplemented("SmallStringSingleKeyGrouper does not support key type ",
                                       key_type_->ToString());
}

}  // namespace tiforth::detail
