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

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <type_traits>
#include <vector>

#include <arrow/compute/row/grouper.h>
#include <arrow/result.h>

namespace arrow {
class Buffer;
class DataType;
class MemoryPool;
namespace compute {
class ExecContext;
}  // namespace compute
}  // namespace arrow

namespace tiforth::detail {

class Arena;

class SmallStringSingleKeyGrouper final : public arrow::compute::Grouper {
 public:
  static constexpr int32_t kInlineBytes = 24;

  SmallStringSingleKeyGrouper(std::shared_ptr<arrow::DataType> key_type,
                              arrow::compute::ExecContext* ctx);
  ~SmallStringSingleKeyGrouper() override;

  arrow::Status Reset() override;

  arrow::Result<arrow::Datum> Consume(const arrow::compute::ExecSpan& batch, int64_t offset = 0,
                                      int64_t length = -1) override;
  arrow::Result<arrow::Datum> Lookup(const arrow::compute::ExecSpan& batch, int64_t offset = 0,
                                     int64_t length = -1) override;
  arrow::Status Populate(const arrow::compute::ExecSpan& batch, int64_t offset = 0,
                         int64_t length = -1) override;

  arrow::Result<arrow::compute::ExecBatch> GetUniques() override;

  uint32_t num_groups() const override;

 private:
  static constexpr std::size_t kInlineBytesSize = static_cast<std::size_t>(kInlineBytes);

  struct Entry {
    uint64_t hash = 0;
    uint32_t group_id = 0;
    uint32_t padding = 0;
  };

  struct GroupKey {
    const uint8_t* data = nullptr;  // non-null for long keys stored in arena
    int32_t length = 0;
    uint8_t is_null = 0;
    uint8_t is_inline = 0;
    uint8_t padding[2] = {0, 0};
    uint8_t inline_bytes[kInlineBytesSize] = {};
  };

  static_assert(std::is_trivially_copyable_v<Entry>);
  static_assert(std::is_trivially_copyable_v<GroupKey>);

  arrow::Status ResetImpl();
  arrow::Result<arrow::Datum> ConsumeOrLookupImpl(const arrow::compute::ExecSpan& batch,
                                                  bool insert, int64_t offset, int64_t length);
  arrow::Status PopulateImpl(const arrow::compute::ExecSpan& batch, int64_t offset, int64_t length);

  arrow::Status EnsureTableInitialized(int64_t capacity);
  arrow::Status EnsureTableCapacityForInsert();
  arrow::Status Rehash(int64_t new_capacity);

  arrow::Status EnsureGroupKeyCapacity(int64_t desired_groups);
  arrow::Result<uint32_t> AppendNullGroup();
  arrow::Result<uint32_t> AppendGroupKey(const uint8_t* data, int32_t length);
  bool KeyEquals(uint32_t group_id, const uint8_t* data, int32_t length) const;

  arrow::Result<uint32_t> FindOrInsert(const uint8_t* data, int32_t length, uint64_t hash);
  arrow::Result<std::optional<uint32_t>> Find(const uint8_t* data, int32_t length,
                                              uint64_t hash) const;

  Entry* Entries();
  const Entry* Entries() const;
  GroupKey* GroupKeys();
  const GroupKey* GroupKeys() const;

  std::shared_ptr<arrow::DataType> key_type_;
  arrow::MemoryPool* pool_ = nullptr;

  std::unique_ptr<Arena> arena_;

  std::unique_ptr<arrow::Buffer> entries_buffer_;
  int64_t table_capacity_ = 0;
  int64_t table_size_ = 0;
  int64_t table_max_load_ = 0;

  std::unique_ptr<arrow::Buffer> group_keys_buffer_;
  int64_t group_key_capacity_ = 0;
  uint32_t num_groups_ = 0;
  std::optional<uint32_t> null_group_id_;
};

}  // namespace tiforth::detail
