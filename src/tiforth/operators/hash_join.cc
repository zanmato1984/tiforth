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

#include "tiforth/operators/hash_join.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <memory_resource>
#include <new>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include <arrow/array.h>
#include <arrow/array/concatenate.h>
#include <arrow/builder.h>
#include <arrow/compute/exec.h>
#include <arrow/memory_pool.h>
#include <arrow/compute/api_vector.h>
#include <arrow/status.h>
#include <arrow/type.h>

#include "tiforth/engine.h"
#include "tiforth/collation.h"
#include "tiforth/detail/arena.h"
#include "tiforth/detail/arrow_compute.h"
#include "tiforth/detail/arrow_memory_pool_resource.h"
#include "tiforth/detail/key_hash_table.h"
#include "tiforth/detail/scratch_bytes.h"
#include "tiforth/type_metadata.h"

namespace tiforth::op {

namespace {
}  // namespace

struct HashJoinPipeOp::Impl {
  using Decimal128Bytes = std::array<uint8_t, 16>;
  using Decimal256Bytes = std::array<uint8_t, 32>;
  static constexpr uint32_t kInvalidIndex = std::numeric_limits<uint32_t>::max();

  struct BuildRowNode {
    uint64_t build_row = 0;
    uint32_t next = kInvalidIndex;
  };

  struct BuildRowList {
    uint32_t head = kInvalidIndex;
    uint32_t tail = kInvalidIndex;
  };

  Impl(const Engine* engine, std::vector<std::shared_ptr<arrow::RecordBatch>> build_batches,
       JoinKey key, arrow::MemoryPool* memory_pool);

  arrow::Status BuildIndex();
  arrow::Result<std::shared_ptr<arrow::Schema>> BuildOutputSchema(
      const std::shared_ptr<arrow::Schema>& left_schema) const;
  arrow::Result<std::shared_ptr<arrow::RecordBatch>> Probe(const arrow::RecordBatch& batch);

  std::vector<std::shared_ptr<arrow::RecordBatch>> build_batches_;
  JoinKey key_;

  std::shared_ptr<arrow::Schema> build_schema_;
  std::shared_ptr<arrow::RecordBatch> build_combined_;
  std::shared_ptr<arrow::Schema> output_schema_;

  uint8_t key_count_ = 0;
  std::array<int, 2> build_key_indices_ = {-1, -1};
  std::array<int, 2> probe_key_indices_ = {-1, -1};

  std::array<int32_t, 2> key_collation_ids_ = {-1, -1};

  arrow::MemoryPool* memory_pool_ = nullptr;
  detail::Arena key_arena_;
  detail::KeyHashTable key_to_key_id_;
  detail::ScratchBytes scratch_normalized_key_;
  detail::ScratchBytes scratch_sort_key_;
  // Owns the memory_resource used by PMR containers in the join index so the allocator stays
  // valid for the lifetime of the hash table/state.
  std::unique_ptr<std::pmr::memory_resource> pmr_resource_;

  std::pmr::vector<BuildRowList> key_rows_;
  std::pmr::vector<BuildRowNode> row_nodes_;
  bool index_built_ = false;
  arrow::compute::ExecContext exec_context_;
};

HashJoinPipeOp::Impl::Impl(
    const Engine* engine, std::vector<std::shared_ptr<arrow::RecordBatch>> build_batches,
    JoinKey key, arrow::MemoryPool* memory_pool)
    : build_batches_(std::move(build_batches)),
      key_(std::move(key)),
      memory_pool_(memory_pool != nullptr
                       ? memory_pool
                       : (engine != nullptr ? engine->memory_pool() : arrow::default_memory_pool())),
      key_arena_(memory_pool_),
      key_to_key_id_(memory_pool_, &key_arena_),
      scratch_normalized_key_(memory_pool_),
      scratch_sort_key_(memory_pool_),
      pmr_resource_(std::make_unique<detail::ArrowMemoryPoolResource>(memory_pool_)),
      key_rows_(std::pmr::polymorphic_allocator<BuildRowList>(pmr_resource_.get())),
      row_nodes_(std::pmr::polymorphic_allocator<BuildRowNode>(pmr_resource_.get())),
      exec_context_(memory_pool_, /*executor=*/nullptr,
                    engine != nullptr ? engine->function_registry() : nullptr) {}

HashJoinPipeOp::HashJoinPipeOp(
    const Engine* engine, std::vector<std::shared_ptr<arrow::RecordBatch>> build_batches,
    JoinKey key, arrow::MemoryPool* memory_pool)
    : impl_(std::make_unique<Impl>(engine, std::move(build_batches), std::move(key), memory_pool)) {}

HashJoinPipeOp::~HashJoinPipeOp() = default;

arrow::Status HashJoinPipeOp::Impl::BuildIndex() {
  if (index_built_) {
    return arrow::Status::OK();
  }

  if (build_batches_.empty()) {
    return arrow::Status::NotImplemented("MS6A requires non-empty build side batches");
  }

  build_schema_ = build_batches_[0]->schema();
  if (build_schema_ == nullptr) {
    return arrow::Status::Invalid("build schema must not be null");
  }

  if (key_.left.size() != key_.right.size()) {
    return arrow::Status::Invalid("join key arity mismatch between probe and build");
  }
  key_count_ = static_cast<uint8_t>(key_.right.size());
  if (key_count_ == 0 || key_count_ > 2) {
    return arrow::Status::NotImplemented("MS9 supports 1 or 2 join keys");
  }

  key_collation_ids_.fill(-1);

  for (uint8_t i = 0; i < key_count_; ++i) {
    if (key_.right[i].empty()) {
      return arrow::Status::Invalid("build join key name must not be empty");
    }
    const int idx = build_schema_->GetFieldIndex(key_.right[i]);
    if (idx < 0 || idx >= build_schema_->num_fields()) {
      return arrow::Status::Invalid("unknown build join key: ", key_.right[i]);
    }
    build_key_indices_[i] = idx;

    // Decode collation metadata if present; missing metadata defaults to BINARY behavior.
    if (const auto& field = build_schema_->field(idx); field != nullptr) {
      ARROW_ASSIGN_OR_RAISE(const auto logical_type, GetLogicalType(*field));
      if (logical_type.id == LogicalTypeId::kString) {
        const int32_t collation_id = logical_type.collation_id >= 0 ? logical_type.collation_id : 63;
        const auto collation = CollationFromId(collation_id);
        if (collation.kind == CollationKind::kUnsupported) {
          return arrow::Status::NotImplemented("unsupported collation id: ", collation_id);
        }
        key_collation_ids_[i] = collation_id;
      }
    }
  }

  int64_t total_rows = 0;
  for (std::size_t batch_index = 0; batch_index < build_batches_.size(); ++batch_index) {
    const auto& batch = build_batches_[batch_index];
    if (batch == nullptr) {
      return arrow::Status::Invalid("build batch must not be null");
    }
    if (!build_schema_->Equals(*batch->schema(), /*check_metadata=*/true)) {
      return arrow::Status::Invalid("build schema mismatch");
    }
    total_rows += batch->num_rows();
  }

  // Concatenate build side batches once (preserves batch order) so join output can use Take.
  std::vector<std::shared_ptr<arrow::Array>> combined_columns;
  combined_columns.reserve(static_cast<std::size_t>(build_schema_->num_fields()));
  for (int col = 0; col < build_schema_->num_fields(); ++col) {
    std::vector<std::shared_ptr<arrow::Array>> chunks;
    chunks.reserve(build_batches_.size());
    for (const auto& batch : build_batches_) {
      chunks.push_back(batch->column(col));
    }
    ARROW_ASSIGN_OR_RAISE(auto combined, arrow::Concatenate(chunks, memory_pool_));
    combined_columns.push_back(std::move(combined));
  }
  build_combined_ =
      arrow::RecordBatch::Make(build_schema_, total_rows, std::move(combined_columns));

  std::array<std::shared_ptr<arrow::Array>, 2> key_arrays{};
  for (uint8_t i = 0; i < key_count_; ++i) {
    key_arrays[i] = build_combined_->column(build_key_indices_[i]);
    if (key_arrays[i] == nullptr) {
      return arrow::Status::Invalid("build join key column must not be null");
    }
  }

  std::array<Collation, 2> collations{CollationFromId(63), CollationFromId(63)};
  for (uint8_t i = 0; i < key_count_; ++i) {
    if (key_collation_ids_[i] >= 0) {
      collations[i] = CollationFromId(key_collation_ids_[i]);
    }
  }

  const auto append_u32_le = [](detail::ScratchBytes& out, uint32_t v) {
    const char bytes[4] = {static_cast<char>(v),
                           static_cast<char>(v >> 8),
                           static_cast<char>(v >> 16),
                           static_cast<char>(v >> 24)};
    out.append(bytes, sizeof(bytes));
  };
  const auto append_u64_le = [](detail::ScratchBytes& out, uint64_t v) {
    const char bytes[8] = {static_cast<char>(v),
                           static_cast<char>(v >> 8),
                           static_cast<char>(v >> 16),
                           static_cast<char>(v >> 24),
                           static_cast<char>(v >> 32),
                           static_cast<char>(v >> 40),
                           static_cast<char>(v >> 48),
                           static_cast<char>(v >> 56)};
    out.append(bytes, sizeof(bytes));
  };

  // Build the hash index on the concatenated build side.
  scratch_normalized_key_.Reset();
  scratch_sort_key_.Reset();
  scratch_normalized_key_.reserve(static_cast<std::size_t>(key_count_) * 16);
  ARROW_RETURN_NOT_OK(scratch_normalized_key_.status());

  const int64_t rows = build_combined_->num_rows();
  for (int64_t row = 0; row < rows; ++row) {
    bool any_null = false;
    for (uint8_t i = 0; i < key_count_; ++i) {
      if (key_arrays[i] != nullptr && key_arrays[i]->IsNull(row)) {
        any_null = true;
        break;
      }
    }
    if (any_null) {
      continue;
    }

    scratch_normalized_key_.Reset();
      for (uint8_t i = 0; i < key_count_; ++i) {
        const auto& array = *key_arrays[i];
        switch (array.type_id()) {
          case arrow::Type::INT8: {
            const auto& arr = static_cast<const arrow::Int8Array&>(array);
            const int64_t v = static_cast<int64_t>(arr.Value(row));
            append_u64_le(scratch_normalized_key_, static_cast<uint64_t>(v));
            break;
          }
          case arrow::Type::INT16: {
            const auto& arr = static_cast<const arrow::Int16Array&>(array);
            const int64_t v = static_cast<int64_t>(arr.Value(row));
            append_u64_le(scratch_normalized_key_, static_cast<uint64_t>(v));
            break;
          }
          case arrow::Type::INT32: {
            const auto& arr = static_cast<const arrow::Int32Array&>(array);
            const int64_t v = static_cast<int64_t>(arr.Value(row));
            append_u64_le(scratch_normalized_key_, static_cast<uint64_t>(v));
            break;
          }
          case arrow::Type::INT64: {
            const auto& arr = static_cast<const arrow::Int64Array&>(array);
            const int64_t v = arr.Value(row);
            append_u64_le(scratch_normalized_key_, static_cast<uint64_t>(v));
            break;
          }
          case arrow::Type::UINT8: {
            const auto& arr = static_cast<const arrow::UInt8Array&>(array);
            append_u64_le(scratch_normalized_key_,
                          static_cast<uint64_t>(arr.Value(row)));
            break;
          }
          case arrow::Type::UINT16: {
            const auto& arr = static_cast<const arrow::UInt16Array&>(array);
            append_u64_le(scratch_normalized_key_,
                          static_cast<uint64_t>(arr.Value(row)));
            break;
          }
          case arrow::Type::UINT32: {
            const auto& arr = static_cast<const arrow::UInt32Array&>(array);
            append_u64_le(scratch_normalized_key_,
                          static_cast<uint64_t>(arr.Value(row)));
            break;
          }
          case arrow::Type::UINT64: {
            const auto& arr = static_cast<const arrow::UInt64Array&>(array);
            append_u64_le(scratch_normalized_key_, arr.Value(row));
            break;
          }
        case arrow::Type::DECIMAL128: {
          const auto& fixed = static_cast<const arrow::FixedSizeBinaryArray&>(array);
          if (fixed.byte_width() != static_cast<int>(Decimal128Bytes{}.size())) {
            return arrow::Status::Invalid("unexpected decimal128 byte width");
          }
          scratch_normalized_key_.append(reinterpret_cast<const char*>(fixed.GetValue(row)),
                                         Decimal128Bytes{}.size());
          break;
        }
        case arrow::Type::DECIMAL256: {
          const auto& fixed = static_cast<const arrow::FixedSizeBinaryArray&>(array);
          if (fixed.byte_width() != static_cast<int>(Decimal256Bytes{}.size())) {
            return arrow::Status::Invalid("unexpected decimal256 byte width");
          }
          scratch_normalized_key_.append(reinterpret_cast<const char*>(fixed.GetValue(row)),
                                         Decimal256Bytes{}.size());
          break;
        }
        case arrow::Type::BINARY: {
          const auto& bin = static_cast<const arrow::BinaryArray&>(array);
          scratch_sort_key_.Reset();
          SortKeyStringTo(collations[i], bin.GetView(row), &scratch_sort_key_);
          ARROW_RETURN_NOT_OK(scratch_sort_key_.status());
          if (scratch_sort_key_.size() >
              static_cast<int64_t>(std::numeric_limits<uint32_t>::max())) {
            return arrow::Status::Invalid("normalized join key string too large");
          }
          append_u32_le(scratch_normalized_key_,
                        static_cast<uint32_t>(scratch_sort_key_.size()));
          scratch_normalized_key_.append(scratch_sort_key_.view());
          break;
        }
        default:
          return arrow::Status::NotImplemented("unsupported join key type: ",
                                               array.type()->ToString());
      }
    }

    ARROW_RETURN_NOT_OK(scratch_normalized_key_.status());
    if (scratch_normalized_key_.size() >
        static_cast<int64_t>(std::numeric_limits<int32_t>::max())) {
      return arrow::Status::Invalid("normalized join key too large");
    }
    const auto* key_data = scratch_normalized_key_.data();
    const auto key_size = static_cast<int32_t>(scratch_normalized_key_.size());
    const uint64_t key_hash = detail::HashBytes(key_data, key_size);

    if (key_rows_.size() > static_cast<std::size_t>(std::numeric_limits<uint32_t>::max())) {
      return arrow::Status::Invalid("too many distinct join keys");
    }
    const uint32_t candidate_key_id = static_cast<uint32_t>(key_rows_.size());
    ARROW_ASSIGN_OR_RAISE(
        auto res, key_to_key_id_.FindOrInsert(key_data, key_size, key_hash, candidate_key_id));
    const uint32_t key_id = res.first;
    const bool inserted = res.second;
    if (inserted) {
      if (key_id != candidate_key_id) {
        return arrow::Status::Invalid("unexpected join key id assignment");
      }
      key_rows_.push_back(BuildRowList{});
    }
    if (key_id >= key_rows_.size()) {
      return arrow::Status::Invalid("internal error: join key id out of range");
    }

    if (row_nodes_.size() > static_cast<std::size_t>(std::numeric_limits<uint32_t>::max() - 1)) {
      return arrow::Status::Invalid("too many build rows in join index");
    }
    const uint32_t node_id = static_cast<uint32_t>(row_nodes_.size());
    row_nodes_.push_back(BuildRowNode{.build_row = static_cast<uint64_t>(row), .next = kInvalidIndex});
    auto& list = key_rows_[key_id];
    if (list.head == kInvalidIndex) {
      list.head = node_id;
      list.tail = node_id;
    } else {
      if (list.tail >= row_nodes_.size()) {
        return arrow::Status::Invalid("internal error: join list tail out of range");
      }
      row_nodes_[list.tail].next = node_id;
      list.tail = node_id;
    }
  }

  index_built_ = true;
  return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::Schema>> HashJoinPipeOp::Impl::BuildOutputSchema(
    const std::shared_ptr<arrow::Schema>& left_schema) const {
  if (left_schema == nullptr) {
    return arrow::Status::Invalid("left schema must not be null");
  }
  if (build_schema_ == nullptr) {
    return arrow::Status::Invalid("build schema must not be null");
  }

  std::vector<std::shared_ptr<arrow::Field>> fields;
  fields.reserve(static_cast<std::size_t>(left_schema->num_fields() + build_schema_->num_fields()));
  for (const auto& field : left_schema->fields()) {
    fields.push_back(field);
  }
  for (const auto& field : build_schema_->fields()) {
    fields.push_back(field);
  }
  return arrow::schema(std::move(fields));
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> HashJoinPipeOp::Impl::Probe(
    const arrow::RecordBatch& probe) {
  ARROW_RETURN_NOT_OK(BuildIndex());
  if (build_schema_ == nullptr || build_combined_ == nullptr) {
    return arrow::Status::Invalid("build side must be initialized before probing");
  }

  const auto probe_schema = probe.schema();
  if (probe_schema == nullptr) {
    return arrow::Status::Invalid("probe schema must not be null");
  }

  if (output_schema_ == nullptr) {
    if (key_.left.size() != key_.right.size()) {
      return arrow::Status::Invalid("join key arity mismatch between probe and build");
    }
    if (key_count_ == 0) {
      key_count_ = static_cast<uint8_t>(key_.left.size());
    }
    if (key_count_ == 0 || key_count_ > 2) {
      return arrow::Status::NotImplemented("MS9 supports 1 or 2 join keys");
    }

    for (uint8_t i = 0; i < key_count_; ++i) {
      if (key_.left[i].empty()) {
        return arrow::Status::Invalid("probe join key name must not be empty");
      }
      const int idx = probe_schema->GetFieldIndex(key_.left[i]);
      if (idx < 0 || idx >= probe_schema->num_fields()) {
        return arrow::Status::Invalid("unknown probe join key: ", key_.left[i]);
      }
      probe_key_indices_[i] = idx;

      if (key_collation_ids_[i] >= 0) {
        if (const auto& field = probe_schema->field(idx); field != nullptr) {
          ARROW_ASSIGN_OR_RAISE(const auto logical_type, GetLogicalType(*field));
          if (logical_type.id != LogicalTypeId::kString) {
            return arrow::Status::NotImplemented("join key collation mismatch between build and probe");
          }
          const int32_t collation_id =
              logical_type.collation_id >= 0 ? logical_type.collation_id : 63;
          if (collation_id != key_collation_ids_[i]) {
            return arrow::Status::NotImplemented("join key collation mismatch between build and probe");
          }
        }
      }
    }
    ARROW_ASSIGN_OR_RAISE(output_schema_, BuildOutputSchema(probe_schema));
  } else {
    // MS6A assumes a stable probe schema across batches.
    if (probe_schema->num_fields() + build_schema_->num_fields() != output_schema_->num_fields()) {
      return arrow::Status::Invalid("hash join output schema field count mismatch");
    }
  }

  std::array<std::shared_ptr<arrow::Array>, 2> probe_key_arrays{};
  std::array<std::shared_ptr<arrow::Array>, 2> build_key_arrays{};
  for (uint8_t i = 0; i < key_count_; ++i) {
    probe_key_arrays[i] = probe.column(probe_key_indices_[i]);
    if (probe_key_arrays[i] == nullptr) {
      return arrow::Status::Invalid("probe join key column must not be null");
    }
    build_key_arrays[i] = build_combined_->column(build_key_indices_[i]);
    if (build_key_arrays[i] == nullptr) {
      return arrow::Status::Invalid("build join key column must not be null");
    }
    if (probe_key_arrays[i]->type_id() != build_key_arrays[i]->type_id()) {
      return arrow::Status::NotImplemented("join key type mismatch between build and probe");
    }
  }

  std::array<Collation, 2> collations{CollationFromId(63), CollationFromId(63)};
  for (uint8_t i = 0; i < key_count_; ++i) {
    if (key_collation_ids_[i] >= 0) {
      collations[i] = CollationFromId(key_collation_ids_[i]);
    }
  }

  const auto append_u32_le = [](detail::ScratchBytes& out, uint32_t v) {
    const char bytes[4] = {static_cast<char>(v),
                           static_cast<char>(v >> 8),
                           static_cast<char>(v >> 16),
                           static_cast<char>(v >> 24)};
    out.append(bytes, sizeof(bytes));
  };
  const auto append_u64_le = [](detail::ScratchBytes& out, uint64_t v) {
    const char bytes[8] = {static_cast<char>(v),
                           static_cast<char>(v >> 8),
                           static_cast<char>(v >> 16),
                           static_cast<char>(v >> 24),
                           static_cast<char>(v >> 32),
                           static_cast<char>(v >> 40),
                           static_cast<char>(v >> 48),
                           static_cast<char>(v >> 56)};
    out.append(bytes, sizeof(bytes));
  };

  scratch_normalized_key_.Reset();
  scratch_sort_key_.Reset();
  scratch_normalized_key_.reserve(static_cast<std::size_t>(key_count_) * 16);
  ARROW_RETURN_NOT_OK(scratch_normalized_key_.status());

  arrow::UInt64Builder probe_indices(memory_pool_);
  arrow::UInt64Builder build_indices(memory_pool_);

  const int64_t probe_rows = probe.num_rows();
  for (int64_t row = 0; row < probe_rows; ++row) {
    bool any_null = false;
    for (uint8_t i = 0; i < key_count_; ++i) {
      if (probe_key_arrays[i] != nullptr && probe_key_arrays[i]->IsNull(row)) {
        any_null = true;
        break;
      }
    }
    if (any_null) {
      continue;
    }

    scratch_normalized_key_.Reset();
    for (uint8_t i = 0; i < key_count_; ++i) {
      const auto& array = *probe_key_arrays[i];
      switch (array.type_id()) {
        case arrow::Type::INT8: {
          const auto& arr = static_cast<const arrow::Int8Array&>(array);
          const int64_t v = static_cast<int64_t>(arr.Value(row));
          append_u64_le(scratch_normalized_key_, static_cast<uint64_t>(v));
          break;
        }
        case arrow::Type::INT16: {
          const auto& arr = static_cast<const arrow::Int16Array&>(array);
          const int64_t v = static_cast<int64_t>(arr.Value(row));
          append_u64_le(scratch_normalized_key_, static_cast<uint64_t>(v));
          break;
        }
        case arrow::Type::INT32: {
          const auto& arr = static_cast<const arrow::Int32Array&>(array);
          const int64_t v = static_cast<int64_t>(arr.Value(row));
          append_u64_le(scratch_normalized_key_, static_cast<uint64_t>(v));
          break;
        }
        case arrow::Type::INT64: {
          const auto& arr = static_cast<const arrow::Int64Array&>(array);
          const int64_t v = arr.Value(row);
          append_u64_le(scratch_normalized_key_, static_cast<uint64_t>(v));
          break;
        }
        case arrow::Type::UINT8: {
          const auto& arr = static_cast<const arrow::UInt8Array&>(array);
          append_u64_le(scratch_normalized_key_,
                        static_cast<uint64_t>(arr.Value(row)));
          break;
        }
        case arrow::Type::UINT16: {
          const auto& arr = static_cast<const arrow::UInt16Array&>(array);
          append_u64_le(scratch_normalized_key_,
                        static_cast<uint64_t>(arr.Value(row)));
          break;
        }
        case arrow::Type::UINT32: {
          const auto& arr = static_cast<const arrow::UInt32Array&>(array);
          append_u64_le(scratch_normalized_key_,
                        static_cast<uint64_t>(arr.Value(row)));
          break;
        }
        case arrow::Type::UINT64: {
          const auto& arr = static_cast<const arrow::UInt64Array&>(array);
          append_u64_le(scratch_normalized_key_, arr.Value(row));
          break;
        }
        case arrow::Type::DECIMAL128: {
          const auto& fixed = static_cast<const arrow::FixedSizeBinaryArray&>(array);
          if (fixed.byte_width() != static_cast<int>(Decimal128Bytes{}.size())) {
            return arrow::Status::Invalid("unexpected decimal128 byte width");
          }
          scratch_normalized_key_.append(reinterpret_cast<const char*>(fixed.GetValue(row)),
                                         Decimal128Bytes{}.size());
          break;
        }
        case arrow::Type::DECIMAL256: {
          const auto& fixed = static_cast<const arrow::FixedSizeBinaryArray&>(array);
          if (fixed.byte_width() != static_cast<int>(Decimal256Bytes{}.size())) {
            return arrow::Status::Invalid("unexpected decimal256 byte width");
          }
          scratch_normalized_key_.append(reinterpret_cast<const char*>(fixed.GetValue(row)),
                                         Decimal256Bytes{}.size());
          break;
        }
        case arrow::Type::BINARY: {
          const auto& bin = static_cast<const arrow::BinaryArray&>(array);
          scratch_sort_key_.Reset();
          SortKeyStringTo(collations[i], bin.GetView(row), &scratch_sort_key_);
          ARROW_RETURN_NOT_OK(scratch_sort_key_.status());
          if (scratch_sort_key_.size() >
              static_cast<int64_t>(std::numeric_limits<uint32_t>::max())) {
            return arrow::Status::Invalid("normalized join key string too large");
          }
          append_u32_le(scratch_normalized_key_,
                        static_cast<uint32_t>(scratch_sort_key_.size()));
          scratch_normalized_key_.append(scratch_sort_key_.view());
          break;
        }
        default:
          return arrow::Status::NotImplemented("unsupported join key type: ",
                                               array.type()->ToString());
      }
    }

    ARROW_RETURN_NOT_OK(scratch_normalized_key_.status());
    if (scratch_normalized_key_.size() >
        static_cast<int64_t>(std::numeric_limits<int32_t>::max())) {
      return arrow::Status::Invalid("normalized join key too large");
    }
    const auto* key_data = scratch_normalized_key_.data();
    const auto key_size = static_cast<int32_t>(scratch_normalized_key_.size());
    const uint64_t key_hash = detail::HashBytes(key_data, key_size);
    ARROW_ASSIGN_OR_RAISE(auto found, key_to_key_id_.Find(key_data, key_size, key_hash));
    if (!found.has_value()) {
      continue;
    }

    const uint32_t key_id = *found;
    if (key_id >= key_rows_.size()) {
      return arrow::Status::Invalid("internal error: join key id out of range");
    }
    uint32_t node = key_rows_[key_id].head;
    while (node != kInvalidIndex) {
      if (node >= row_nodes_.size()) {
        return arrow::Status::Invalid("internal error: join node id out of range");
      }
      const uint64_t build_row = row_nodes_[node].build_row;
      ARROW_RETURN_NOT_OK(probe_indices.Append(static_cast<uint64_t>(row)));
      ARROW_RETURN_NOT_OK(build_indices.Append(static_cast<uint64_t>(build_row)));
      node = row_nodes_[node].next;
    }
  }

  std::shared_ptr<arrow::Array> probe_indices_array;
  std::shared_ptr<arrow::Array> build_indices_array;
  ARROW_RETURN_NOT_OK(probe_indices.Finish(&probe_indices_array));
  ARROW_RETURN_NOT_OK(build_indices.Finish(&build_indices_array));

  ARROW_RETURN_NOT_OK(detail::EnsureArrowComputeInitialized());
  const auto take_options = arrow::compute::TakeOptions::NoBoundsCheck();

  std::vector<std::shared_ptr<arrow::Array>> out_arrays;
  out_arrays.reserve(static_cast<std::size_t>(probe.num_columns() + build_schema_->num_fields()));

  for (int ci = 0; ci < probe.num_columns(); ++ci) {
    ARROW_ASSIGN_OR_RAISE(
        auto taken,
        arrow::compute::Take(arrow::Datum(probe.column(ci)), arrow::Datum(probe_indices_array),
                             take_options, &exec_context_));
    ARROW_ASSIGN_OR_RAISE(auto out_array,
                          detail::DatumToArray(taken, exec_context_.memory_pool()));
    out_arrays.push_back(std::move(out_array));
  }

  for (int ci = 0; ci < build_schema_->num_fields(); ++ci) {
    ARROW_ASSIGN_OR_RAISE(
        auto taken,
        arrow::compute::Take(arrow::Datum(build_combined_->column(ci)),
                             arrow::Datum(build_indices_array), take_options, &exec_context_));
    ARROW_ASSIGN_OR_RAISE(auto out_array,
                          detail::DatumToArray(taken, exec_context_.memory_pool()));
    out_arrays.push_back(std::move(out_array));
  }

  const int64_t out_rows = probe_indices_array->length();
  return arrow::RecordBatch::Make(output_schema_, out_rows, std::move(out_arrays));
}

PipelinePipe HashJoinPipeOp::Pipe() {
  return [this](const TaskContext&, ThreadId, std::optional<Batch> input) -> OpResult {
    if (!input.has_value()) {
      return OpOutput::PipeSinkNeedsMore();
    }
    auto batch = std::move(*input);
    if (batch == nullptr) {
      return arrow::Status::Invalid("hash join input batch must not be null");
    }
    if (impl_ == nullptr) {
      return arrow::Status::Invalid("hash join operator has null implementation");
    }
    ARROW_ASSIGN_OR_RAISE(auto out, impl_->Probe(*batch));
    if (out == nullptr) {
      return arrow::Status::Invalid("hash join output batch must not be null");
    }
    return OpOutput::PipeEven(std::move(out));
  };
}

PipelineDrain HashJoinPipeOp::Drain() {
  return [](const TaskContext&, ThreadId) -> OpResult { return OpOutput::Finished(); };
}

std::unique_ptr<SourceOp> HashJoinPipeOp::ImplicitSource() { return nullptr; }

}  // namespace tiforth::op
