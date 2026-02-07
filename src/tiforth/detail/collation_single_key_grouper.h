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

#include <cstdint>
#include <memory>
#include <optional>
#include <string_view>
#include <type_traits>

#include <arrow/buffer.h>
#include <arrow/compute/row/grouper.h>
#include <arrow/result.h>

#include "tiforth/collation.h"
#include "tiforth/detail/arena.h"
#include "tiforth/detail/key_hash_table.h"
#include "tiforth/detail/scratch_bytes.h"

namespace arrow {
class DataType;
class MemoryPool;
namespace compute {
class ExecContext;
}  // namespace compute
}  // namespace arrow

namespace tiforth::detail {

class CollationSingleKeyGrouper final : public arrow::compute::Grouper {
 public:
  CollationSingleKeyGrouper(std::shared_ptr<arrow::DataType> key_type, int32_t collation_id,
                            arrow::compute::ExecContext* ctx);
  ~CollationSingleKeyGrouper() override;

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
  struct GroupKey {
    ByteSlice original;
    uint8_t is_null = 0;
    uint8_t padding[7] = {0, 0, 0, 0, 0, 0, 0};
  };
  static_assert(std::is_trivially_copyable_v<GroupKey>);

  arrow::Status ResetImpl();
  arrow::Result<arrow::Datum> ConsumeOrLookupImpl(const arrow::compute::ExecSpan& batch, bool insert,
                                                  int64_t offset, int64_t length);
  arrow::Status PopulateImpl(const arrow::compute::ExecSpan& batch, int64_t offset, int64_t length);

  arrow::Status EnsureGroupKeyCapacity(int64_t desired_groups);
  arrow::Result<uint32_t> AppendNullGroup();
  arrow::Result<uint32_t> AppendGroupKey(std::string_view original);

  GroupKey* GroupKeys();
  const GroupKey* GroupKeys() const;

  arrow::Result<std::string_view> SortKey(std::string_view original);

  std::shared_ptr<arrow::DataType> key_type_;
  Collation collation_{};

  arrow::MemoryPool* pool_ = nullptr;
  std::unique_ptr<Arena> arena_;
  KeyHashTable key_to_group_id_;
  ScratchBytes scratch_sort_key_;

  std::unique_ptr<arrow::Buffer> group_keys_buffer_;
  int64_t group_key_capacity_ = 0;
  uint32_t num_groups_ = 0;
  std::optional<uint32_t> null_group_id_;
};

}  // namespace tiforth::detail
