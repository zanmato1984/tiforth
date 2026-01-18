#pragma once

#include <array>
#include <cstdint>
#include <limits>
#include <memory>
#include <memory_resource>
#include <string>
#include <utility>
#include <vector>

#include <arrow/compute/exec.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>

#include "tiforth/detail/arena.h"
#include "tiforth/detail/key_hash_table.h"
#include "tiforth/detail/scratch_bytes.h"
#include "tiforth/operators.h"

namespace arrow {
class Array;
class MemoryPool;
class Schema;
}  // namespace arrow

namespace tiforth {

class Engine;

struct JoinKey {
  std::vector<std::string> left;
  std::vector<std::string> right;
};

class HashJoinTransformOp final : public TransformOp {
 public:
  HashJoinTransformOp(const Engine* engine, std::vector<std::shared_ptr<arrow::RecordBatch>> build_batches,
                      JoinKey key, arrow::MemoryPool* memory_pool = nullptr);

 protected:
  arrow::Result<OperatorStatus> TransformImpl(
      std::shared_ptr<arrow::RecordBatch>* batch) override;

 private:
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

  arrow::Status BuildIndex();
  arrow::Result<std::shared_ptr<arrow::Schema>> BuildOutputSchema(
      const std::shared_ptr<arrow::Schema>& left_schema) const;

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

}  // namespace tiforth
