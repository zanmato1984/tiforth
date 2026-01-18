#pragma once

#include <array>
#include <cstdint>
#include <memory>
#include <memory_resource>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include <arrow/compute/exec.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>

#include "tiforth/expr.h"
#include "tiforth/operators.h"
#include "tiforth/detail/arena.h"
#include "tiforth/detail/key_hash_table.h"

namespace arrow {
class Array;
class MemoryPool;
class Schema;
}  // namespace arrow

namespace tiforth {

struct AggKey {
  std::string name;
  ExprPtr expr;
};

struct AggFunc {
  std::string name;
  // Supported: "count_all", "count", "sum", "min", "max" (some aliases accepted).
  std::string func;
  ExprPtr arg;       // unused for "count_all"
};

class HashAggContext final {
 public:
  HashAggContext(const Engine* engine, std::vector<AggKey> keys, std::vector<AggFunc> aggs,
                 arrow::MemoryPool* memory_pool = nullptr);

  HashAggContext(const HashAggContext&) = delete;
  HashAggContext& operator=(const HashAggContext&) = delete;

  ~HashAggContext();

  arrow::Status ConsumeBatch(const arrow::RecordBatch& input);
  arrow::Status FinishBuild();
 arrow::Result<std::shared_ptr<arrow::RecordBatch>> ReadNextOutputBatch(int64_t max_rows);

 private:
  static constexpr std::size_t kMaxKeys = 8;

  using Decimal128Bytes = std::array<uint8_t, 16>;
  using Decimal256Bytes = std::array<uint8_t, 32>;
  using KeyValue =
      std::variant<int64_t, uint64_t, Decimal128Bytes, Decimal256Bytes, std::pmr::string>;

  struct KeyPart {
    bool is_null = false;
    KeyValue value;
  };

  struct OutputKey {
    uint8_t key_count = 0;
    std::array<KeyPart, kMaxKeys> parts;
  };

  arrow::Result<uint32_t> InsertGroup(std::string_view normalized_key_bytes, uint64_t hash,
                                      OutputKey output_key);
  arrow::Result<std::shared_ptr<arrow::Schema>> BuildOutputSchema() const;
  arrow::Result<std::shared_ptr<arrow::RecordBatch>> FinalizeOutput();

  struct AggState {
    enum class Kind {
      kUnsupported,
      kCountAll,
      kCount,
      kSum,
      kMin,
      kMax,
    };

    enum class SumKind {
      kUnresolved,
      kInt64,
      kUInt64,
    };

    std::string name;
    std::string func;
    Kind kind;
    ExprPtr arg;
    SumKind sum_kind = SumKind::kUnresolved;

    std::vector<uint64_t> count_all;
    std::vector<uint64_t> count;
    std::vector<int64_t> sum_i64;
    std::vector<uint64_t> sum_u64;
    std::vector<uint8_t> sum_has_value;
    std::vector<KeyPart> extreme_out;
    std::vector<KeyPart> extreme_norm;
  };

  struct Compiled;

  const Engine* engine_ = nullptr;
  std::vector<AggKey> keys_;
  std::vector<AggState> aggs_;

  std::shared_ptr<arrow::Schema> output_schema_;
  std::vector<std::shared_ptr<arrow::Field>> output_key_fields_;
  std::vector<std::shared_ptr<arrow::Field>> output_agg_fields_;

  arrow::MemoryPool* memory_pool_ = nullptr;
  detail::Arena group_key_arena_;
  detail::KeyHashTable key_to_group_id_;
  // Owns the memory_resource used by pmr group keys (normalized/original string keys) so the
  // allocator stays valid for the lifetime of the hash table and group key storage.
  std::unique_ptr<std::pmr::memory_resource> pmr_resource_;
  std::unique_ptr<Compiled> compiled_;
  arrow::compute::ExecContext exec_context_;

  std::vector<OutputKey> group_keys_;

  bool build_finished_ = false;
  std::shared_ptr<arrow::RecordBatch> output_all_;
  int64_t next_output_row_ = 0;
};

class HashAggBuildSinkOp final : public SinkOp {
 public:
  explicit HashAggBuildSinkOp(std::shared_ptr<HashAggContext> context);

 protected:
  arrow::Result<OperatorStatus> WriteImpl(std::shared_ptr<arrow::RecordBatch> batch) override;

 private:
  std::shared_ptr<HashAggContext> context_;
};

class HashAggConvergentSourceOp final : public SourceOp {
 public:
  HashAggConvergentSourceOp(std::shared_ptr<HashAggContext> context, int64_t max_output_rows = 65536);

 protected:
  arrow::Result<OperatorStatus> ReadImpl(std::shared_ptr<arrow::RecordBatch>* batch) override;

 private:
  std::shared_ptr<HashAggContext> context_;
  int64_t max_output_rows_ = 65536;
};

class HashAggTransformOp final : public TransformOp {
 public:
  HashAggTransformOp(const Engine* engine, std::vector<AggKey> keys, std::vector<AggFunc> aggs,
                     arrow::MemoryPool* memory_pool = nullptr);
  ~HashAggTransformOp() override;

 protected:
  arrow::Result<OperatorStatus> TransformImpl(
      std::shared_ptr<arrow::RecordBatch>* batch) override;

 private:
  std::shared_ptr<HashAggContext> context_;
  bool finalized_ = false;
  bool eos_forwarded_ = false;
};

}  // namespace tiforth
