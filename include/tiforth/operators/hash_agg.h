#pragma once

#include <array>
#include <cstdint>
#include <memory>
#include <memory_resource>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>

#include <arrow/compute/exec.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>

#include "tiforth/expr.h"
#include "tiforth/operators.h"

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

class HashAggTransformOp final : public TransformOp {
 public:
  HashAggTransformOp(const Engine* engine, std::vector<AggKey> keys, std::vector<AggFunc> aggs,
                     arrow::MemoryPool* memory_pool = nullptr);
  ~HashAggTransformOp() override;

 protected:
  arrow::Result<OperatorStatus> TransformImpl(
      std::shared_ptr<arrow::RecordBatch>* batch) override;

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

  struct NormalizedKey {
    uint8_t key_count = 0;
    std::array<KeyPart, kMaxKeys> parts;
  };
  struct OutputKey {
    uint8_t key_count = 0;
    std::array<KeyPart, kMaxKeys> parts;
  };
  struct KeyHash {
    std::size_t operator()(const NormalizedKey& key) const noexcept;
  };
  struct KeyEq {
    bool operator()(const NormalizedKey& lhs, const NormalizedKey& rhs) const noexcept;
  };

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

  arrow::Status ConsumeBatch(const arrow::RecordBatch& input);
  arrow::Result<std::shared_ptr<arrow::RecordBatch>> FinalizeOutput();
  arrow::Result<std::shared_ptr<arrow::Schema>> BuildOutputSchema() const;

  arrow::Result<uint32_t> GetOrAddGroup(NormalizedKey key, OutputKey output_key);

  struct Compiled;

  const Engine* engine_ = nullptr;
  std::vector<AggKey> keys_;
  std::vector<AggState> aggs_;

  std::shared_ptr<arrow::Schema> output_schema_;
  std::vector<std::shared_ptr<arrow::Field>> output_key_fields_;
  std::vector<std::shared_ptr<arrow::Field>> output_agg_fields_;

  arrow::MemoryPool* memory_pool_ = nullptr;
  // Owns the memory_resource used by pmr group keys (normalized/original string keys) so the
  // allocator stays valid for the lifetime of the hash table and group key storage.
  std::unique_ptr<std::pmr::memory_resource> pmr_resource_;
  std::unique_ptr<Compiled> compiled_;
  arrow::compute::ExecContext exec_context_;

  bool finalized_ = false;
  bool eos_forwarded_ = false;

  std::unordered_map<NormalizedKey, uint32_t, KeyHash, KeyEq> key_to_group_id_;
  std::vector<OutputKey> group_keys_;
};

}  // namespace tiforth
