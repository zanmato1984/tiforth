#include "tiforth/operators/hash_agg.h"

#include <array>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <limits>
#include <memory_resource>
#include <new>
#include <optional>
#include <string_view>
#include <type_traits>
#include <utility>

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/compute/exec.h>
#include <arrow/memory_pool.h>
#include <arrow/status.h>
#include <arrow/type.h>

#include "tiforth/compiled_expr.h"
#include "tiforth/engine.h"
#include "tiforth/collation.h"
#include "tiforth/type_metadata.h"

namespace tiforth {

namespace {

std::size_t HashBytes(const uint8_t* data, std::size_t size) noexcept {
  uint64_t h = 14695981039346656037ULL;
  for (std::size_t i = 0; i < size; ++i) {
    h ^= static_cast<uint64_t>(data[i]);
    h *= 1099511628211ULL;
  }
  return static_cast<std::size_t>(h);
}

template <std::size_t N>
std::size_t HashBytes(const std::array<uint8_t, N>& bytes) noexcept {
  return HashBytes(bytes.data(), bytes.size());
}

uint64_t Float32ToBits(float value) noexcept {
  uint32_t bits = 0;
  std::memcpy(&bits, &value, sizeof(bits));
  return static_cast<uint64_t>(bits);
}

float BitsToFloat32(uint64_t bits) noexcept {
  const uint32_t bits32 = static_cast<uint32_t>(bits);
  float value = 0.0F;
  std::memcpy(&value, &bits32, sizeof(value));
  return value;
}

uint64_t Float64ToBits(double value) noexcept {
  uint64_t bits = 0;
  std::memcpy(&bits, &value, sizeof(bits));
  return bits;
}

double BitsToFloat64(uint64_t bits) noexcept {
  double value = 0.0;
  std::memcpy(&value, &bits, sizeof(value));
  return value;
}

uint64_t CanonicalizeFloat32Bits(float value) noexcept {
  if (value == 0.0F) {
    return 0;
  }
  if (std::isnan(value)) {
    return Float32ToBits(std::numeric_limits<float>::quiet_NaN());
  }
  return Float32ToBits(value);
}

uint64_t CanonicalizeFloat64Bits(double value) noexcept {
  if (value == 0.0) {
    return 0;
  }
  if (std::isnan(value)) {
    return Float64ToBits(std::numeric_limits<double>::quiet_NaN());
  }
  return Float64ToBits(value);
}

class ArrowMemoryPoolResource final : public std::pmr::memory_resource {
 public:
  explicit ArrowMemoryPoolResource(arrow::MemoryPool* pool)
      : pool_(pool != nullptr ? pool : arrow::default_memory_pool()) {}

 private:
  void* do_allocate(std::size_t bytes, std::size_t /*alignment*/) override {
    uint8_t* out = nullptr;
    const auto st = pool_->Allocate(static_cast<int64_t>(bytes), &out);
    if (!st.ok()) {
      throw std::bad_alloc();
    }
    return out;
  }

  void do_deallocate(void* p, std::size_t bytes, std::size_t /*alignment*/) override {
    if (p == nullptr) {
      return;
    }
    pool_->Free(reinterpret_cast<uint8_t*>(p), static_cast<int64_t>(bytes));
  }

  bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override {
    return this == &other;
  }

  arrow::MemoryPool* pool_;
};

}  // namespace

std::size_t HashAggTransformOp::KeyHash::operator()(const NormalizedKey& key) const noexcept {
  // FNV-1a style mixing over key parts (supports 1 or 2 keys).
  uint64_t h = 14695981039346656037ULL;
  h ^= static_cast<uint64_t>(key.key_count);
  h *= 1099511628211ULL;

  for (uint8_t i = 0; i < key.key_count && i < key.parts.size(); ++i) {
    const auto& part = key.parts[i];
    if (part.is_null) {
      h ^= 0x9e3779b97f4a7c15ULL;
      h *= 1099511628211ULL;
      continue;
    }
    const std::size_t part_hash = std::visit(
        [&](const auto& v) -> std::size_t {
          using V = std::decay_t<decltype(v)>;
          if constexpr (std::is_same_v<V, int64_t>) {
            return std::hash<int64_t>{}(v);
          } else if constexpr (std::is_same_v<V, uint64_t>) {
            return std::hash<uint64_t>{}(v);
          } else if constexpr (std::is_same_v<V, Decimal128Bytes> ||
                               std::is_same_v<V, Decimal256Bytes>) {
            return HashBytes(v);
          } else if constexpr (std::is_same_v<V, std::pmr::string>) {
            return HashBytes(reinterpret_cast<const uint8_t*>(v.data()), v.size());
          } else {
            return 0;
          }
        },
        part.value);
    h ^= static_cast<uint64_t>(part_hash);
    h *= 1099511628211ULL;
  }

  return static_cast<std::size_t>(h);
}

bool HashAggTransformOp::KeyEq::operator()(const NormalizedKey& lhs,
                                           const NormalizedKey& rhs) const noexcept {
  if (lhs.key_count != rhs.key_count) {
    return false;
  }
  for (uint8_t i = 0; i < lhs.key_count && i < lhs.parts.size(); ++i) {
    const auto& l = lhs.parts[i];
    const auto& r = rhs.parts[i];
    if (l.is_null != r.is_null) {
      return false;
    }
    if (l.is_null) {
      continue;
    }
    if (l.value != r.value) {
      return false;
    }
  }
  return true;
}

struct HashAggTransformOp::Compiled {
  std::vector<CompiledExpr> key_exprs;
  std::vector<std::optional<CompiledExpr>> agg_arg_exprs;
};

HashAggTransformOp::~HashAggTransformOp() = default;

HashAggTransformOp::HashAggTransformOp(const Engine* engine, std::vector<AggKey> keys,
                                       std::vector<AggFunc> aggs, arrow::MemoryPool* memory_pool)
    : engine_(engine),
      keys_(std::move(keys)),
      memory_pool_(memory_pool != nullptr
                       ? memory_pool
                       : (engine != nullptr ? engine->memory_pool() : arrow::default_memory_pool())),
      exec_context_(memory_pool_, /*executor=*/nullptr,
                   engine != nullptr ? engine->function_registry() : nullptr) {
  pmr_resource_ = std::make_unique<ArrowMemoryPoolResource>(memory_pool_);

  aggs_.reserve(aggs.size());
  for (auto& agg : aggs) {
    if (agg.func == "count_all") {
      aggs_.push_back(AggState{.name = std::move(agg.name),
                               .func = "count_all",
                               .kind = AggState::Kind::kCountAll,
                               .arg = nullptr});
    } else if (agg.func == "count") {
      aggs_.push_back(AggState{.name = std::move(agg.name),
                               .func = "count",
                               .kind = AggState::Kind::kCount,
                               .arg = std::move(agg.arg)});
    } else if (agg.func == "sum" || agg.func == "sum_uint64" || agg.func == "sum_int32") {
      aggs_.push_back(AggState{.name = std::move(agg.name),
                               .func = "sum",
                               .kind = AggState::Kind::kSum,
                               .arg = std::move(agg.arg),
                               .sum_kind = AggState::SumKind::kUnresolved});
    } else if (agg.func == "min") {
      aggs_.push_back(AggState{.name = std::move(agg.name),
                               .func = "min",
                               .kind = AggState::Kind::kMin,
                               .arg = std::move(agg.arg)});
    } else if (agg.func == "max") {
      aggs_.push_back(AggState{.name = std::move(agg.name),
                               .func = "max",
                               .kind = AggState::Kind::kMax,
                               .arg = std::move(agg.arg)});
    } else {
      aggs_.push_back(AggState{.name = std::move(agg.name),
                               .func = std::move(agg.func),
                               .kind = AggState::Kind::kUnsupported,
                               .arg = std::move(agg.arg)});
    }
  }
}

arrow::Result<uint32_t> HashAggTransformOp::GetOrAddGroup(NormalizedKey key, OutputKey output_key) {
  auto it = key_to_group_id_.find(key);
  if (it != key_to_group_id_.end()) {
    return it->second;
  }

  const uint32_t group_id = static_cast<uint32_t>(group_keys_.size());
  key_to_group_id_.emplace(std::move(key), group_id);
  group_keys_.push_back(std::move(output_key));
  for (auto& agg : aggs_) {
    switch (agg.kind) {
      case AggState::Kind::kUnsupported:
        break;
      case AggState::Kind::kCountAll:
        agg.count_all.push_back(0);
        break;
      case AggState::Kind::kCount:
        agg.count.push_back(0);
        break;
      case AggState::Kind::kSum: {
        switch (agg.sum_kind) {
          case AggState::SumKind::kUnresolved:
            return arrow::Status::Invalid("internal error: sum kind must be resolved");
          case AggState::SumKind::kInt64:
            agg.sum_i64.push_back(0);
            agg.sum_has_value.push_back(0);
            break;
          case AggState::SumKind::kUInt64:
            agg.sum_u64.push_back(0);
            agg.sum_has_value.push_back(0);
            break;
        }
        break;
      }
      case AggState::Kind::kMin:
      case AggState::Kind::kMax: {
        KeyPart out;
        out.is_null = true;
        out.value = int64_t{0};
        agg.extreme_out.push_back(out);
        agg.extreme_norm.push_back(std::move(out));
        break;
      }
    }
  }
  return group_id;
}

arrow::Status HashAggTransformOp::ConsumeBatch(const arrow::RecordBatch& input) {
  if (engine_ == nullptr) {
    return arrow::Status::Invalid("hash agg engine must not be null");
  }
  const std::size_t key_count = keys_.size();
  if (key_count > kMaxKeys) {
    return arrow::Status::NotImplemented("hash agg supports up to ", kMaxKeys, " group keys");
  }
  if (key_count != 0) {
    for (std::size_t i = 0; i < key_count; ++i) {
      if (keys_[i].name.empty()) {
        return arrow::Status::Invalid("group key name must not be empty");
      }
      if (keys_[i].expr == nullptr) {
        return arrow::Status::Invalid("group key expr must not be null");
      }
    }
  }

  if (compiled_ == nullptr) {
    auto compiled = std::make_unique<Compiled>();
    compiled->key_exprs.reserve(key_count);
    for (std::size_t i = 0; i < key_count; ++i) {
      ARROW_ASSIGN_OR_RAISE(auto compiled_key,
                            CompileExpr(input.schema(), *keys_[i].expr, engine_, &exec_context_));
      compiled->key_exprs.push_back(std::move(compiled_key));
    }

    compiled->agg_arg_exprs.resize(aggs_.size());
    for (std::size_t i = 0; i < aggs_.size(); ++i) {
      const auto& agg = aggs_[i];
      switch (agg.kind) {
        case AggState::Kind::kCountAll:
          continue;
        case AggState::Kind::kUnsupported:
          continue;
        case AggState::Kind::kCount:
        case AggState::Kind::kSum:
        case AggState::Kind::kMin:
        case AggState::Kind::kMax:
          break;
      }
      if (agg.arg == nullptr) {
        return arrow::Status::Invalid(agg.func, " arg must not be null");
      }
      ARROW_ASSIGN_OR_RAISE(auto compiled_arg,
                            CompileExpr(input.schema(), *agg.arg, engine_, &exec_context_));
      compiled->agg_arg_exprs[i] = std::move(compiled_arg);
    }

    compiled_ = std::move(compiled);
  }
  if (compiled_ == nullptr || compiled_->key_exprs.size() != key_count) {
    return arrow::Status::Invalid("compiled hash agg key expr count mismatch");
  }
  if (compiled_ == nullptr || compiled_->agg_arg_exprs.size() != aggs_.size()) {
    return arrow::Status::Invalid("compiled hash agg agg expr count mismatch");
  }

  std::array<std::shared_ptr<arrow::Array>, kMaxKeys> key_arrays{};
  for (std::size_t i = 0; i < key_count; ++i) {
    ARROW_ASSIGN_OR_RAISE(key_arrays[i],
                          ExecuteExprAsArray(compiled_->key_exprs[i], input, &exec_context_));
    if (key_arrays[i] == nullptr) {
      return arrow::Status::Invalid("group key must not evaluate to null array");
    }
    if (key_arrays[i]->length() != input.num_rows()) {
      return arrow::Status::Invalid("group key length mismatch");
    }
  }

  if (key_count != 0 && output_key_fields_.empty()) {
    auto schema = input.schema();
    output_key_fields_.reserve(key_count);

    for (std::size_t i = 0; i < key_count; ++i) {
      const auto* field_ref = std::get_if<FieldRef>(&keys_[i].expr->node);
      if (schema != nullptr && field_ref != nullptr) {
        int field_index = field_ref->index;
        if (field_index < 0 && !field_ref->name.empty()) {
          field_index = schema->GetFieldIndex(field_ref->name);
        }
        if (field_index >= 0 && field_index < schema->num_fields()) {
          if (const auto& field = schema->field(field_index); field != nullptr) {
            output_key_fields_.push_back(field);
            continue;
          }
        }
      }
      // Fallback: use evaluated key type (metadata may be missing).
      output_key_fields_.push_back(
          arrow::field(keys_[i].name, key_arrays[i]->type(), /*nullable=*/true));
    }
  }
  if (output_key_fields_.size() != key_count) {
    return arrow::Status::Invalid("group key field count mismatch");
  }

  std::array<Collation, kMaxKeys> collations;
  collations.fill(CollationFromId(63));
  for (std::size_t i = 0; i < key_count; ++i) {
    if (key_arrays[i]->type_id() != arrow::Type::BINARY) {
      continue;
    }
    int32_t collation_id = 63;
    if (output_key_fields_[i] != nullptr) {
      ARROW_ASSIGN_OR_RAISE(const auto logical_type, GetLogicalType(*output_key_fields_[i]));
      if (logical_type.id == LogicalTypeId::kString) {
        collation_id = logical_type.collation_id >= 0 ? logical_type.collation_id : 63;
      }
    }
    const auto collation = CollationFromId(collation_id);
    if (collation.kind == CollationKind::kUnsupported) {
      return arrow::Status::NotImplemented("unsupported collation id: ", collation_id);
    }
    collations[i] = collation;
  }

  std::vector<std::shared_ptr<arrow::Array>> agg_args(aggs_.size());
  for (std::size_t i = 0; i < aggs_.size(); ++i) {
    auto& agg = aggs_[i];
    switch (agg.kind) {
      case AggState::Kind::kUnsupported:
        return arrow::Status::NotImplemented("unsupported aggregate func: ", agg.func);
      case AggState::Kind::kCountAll:
        continue;
      case AggState::Kind::kCount:
      case AggState::Kind::kSum:
      case AggState::Kind::kMin:
      case AggState::Kind::kMax:
        break;
    }
    if (compiled_ == nullptr || i >= compiled_->agg_arg_exprs.size() ||
        !compiled_->agg_arg_exprs[i].has_value()) {
      return arrow::Status::Invalid(agg.func, " arg must not be null");
    }

    ARROW_ASSIGN_OR_RAISE(
        auto arg_any,
        ExecuteExprAsArray(*compiled_->agg_arg_exprs[i], input, &exec_context_));
    if (arg_any == nullptr) {
      return arrow::Status::Invalid(agg.func, " arg must not evaluate to null array");
    }
    if (arg_any->length() != input.num_rows()) {
      return arrow::Status::Invalid(agg.func, " arg length mismatch");
    }

    if (agg.kind == AggState::Kind::kSum) {
      switch (arg_any->type_id()) {
        case arrow::Type::BOOL:
        case arrow::Type::INT8:
        case arrow::Type::INT16:
        case arrow::Type::INT32:
        case arrow::Type::INT64:
        case arrow::Type::UINT8:
        case arrow::Type::UINT16:
        case arrow::Type::UINT32:
        case arrow::Type::UINT64:
          break;
        default:
          return arrow::Status::NotImplemented("sum arg type not supported: ",
                                               arg_any->type()->ToString());
      }
    }
    if (agg.kind == AggState::Kind::kMin || agg.kind == AggState::Kind::kMax) {
      switch (arg_any->type_id()) {
        case arrow::Type::INT8:
        case arrow::Type::INT16:
        case arrow::Type::INT32:
        case arrow::Type::INT64:
        case arrow::Type::UINT8:
        case arrow::Type::UINT16:
        case arrow::Type::UINT32:
        case arrow::Type::UINT64:
        case arrow::Type::BINARY:
          break;
        default:
          return arrow::Status::NotImplemented(agg.func, " arg type not supported: ",
                                               arg_any->type()->ToString());
      }
    }

    agg_args[i] = std::move(arg_any);
  }

  if (output_agg_fields_.empty()) {
    auto schema = input.schema();
    output_agg_fields_.reserve(aggs_.size());

    for (std::size_t i = 0; i < aggs_.size(); ++i) {
      const auto& agg = aggs_[i];
      if (agg.name.empty()) {
        return arrow::Status::Invalid("agg output name must not be empty");
      }
      switch (agg.kind) {
        case AggState::Kind::kUnsupported:
          return arrow::Status::NotImplemented("unsupported aggregate func: ", agg.func);
        case AggState::Kind::kCountAll:
        case AggState::Kind::kCount:
          output_agg_fields_.push_back(arrow::field(agg.name, arrow::uint64(), /*nullable=*/false));
          break;
        case AggState::Kind::kSum: {
          const auto& arg_any = agg_args[i];
          if (arg_any == nullptr) {
            return arrow::Status::Invalid("internal error: missing sum arg array");
          }

          AggState::SumKind expected = AggState::SumKind::kUnresolved;
          switch (arg_any->type_id()) {
            case arrow::Type::BOOL:
            case arrow::Type::UINT8:
            case arrow::Type::UINT16:
            case arrow::Type::UINT32:
            case arrow::Type::UINT64:
              expected = AggState::SumKind::kUInt64;
              break;
            case arrow::Type::INT8:
            case arrow::Type::INT16:
            case arrow::Type::INT32:
            case arrow::Type::INT64:
              expected = AggState::SumKind::kInt64;
              break;
            default:
              return arrow::Status::NotImplemented("sum arg type not supported: ",
                                                   arg_any->type()->ToString());
          }

          if (agg.sum_kind == AggState::SumKind::kUnresolved) {
            aggs_[i].sum_kind = expected;
          } else if (agg.sum_kind != expected) {
            return arrow::Status::Invalid("sum arg type mismatch across batches");
          }

          if (expected == AggState::SumKind::kInt64) {
            output_agg_fields_.push_back(arrow::field(agg.name, arrow::int64(), /*nullable=*/true));
          } else {
            output_agg_fields_.push_back(arrow::field(agg.name, arrow::uint64(), /*nullable=*/true));
          }
          break;
        }
        case AggState::Kind::kMin:
        case AggState::Kind::kMax: {
          const auto& arg_any = agg_args[i];
          if (arg_any == nullptr) {
            return arrow::Status::Invalid("internal error: missing ", agg.func, " arg array");
          }

          std::shared_ptr<arrow::Field> out_field;
          const auto* field_ref = agg.arg != nullptr ? std::get_if<FieldRef>(&agg.arg->node) : nullptr;
          if (schema != nullptr && field_ref != nullptr) {
            int field_index = field_ref->index;
            if (field_index < 0 && !field_ref->name.empty()) {
              field_index = schema->GetFieldIndex(field_ref->name);
            }
            if (field_index >= 0 && field_index < schema->num_fields()) {
              if (const auto& field = schema->field(field_index); field != nullptr) {
                out_field = field->WithName(agg.name)->WithNullable(true);
              }
            }
          }
          if (out_field == nullptr) {
            out_field = arrow::field(agg.name, arg_any->type(), /*nullable=*/true);
          }

          output_agg_fields_.push_back(std::move(out_field));
          break;
        }
      }
    }
  }
  if (output_agg_fields_.size() != aggs_.size()) {
    return arrow::Status::Invalid("agg output field count mismatch");
  }

  std::vector<Collation> agg_collations(aggs_.size(), CollationFromId(63));
  for (std::size_t i = 0; i < aggs_.size(); ++i) {
    const auto& agg = aggs_[i];
    if (agg.kind != AggState::Kind::kMin && agg.kind != AggState::Kind::kMax) {
      continue;
    }
    const auto& arg_any = agg_args[i];
    if (arg_any == nullptr || arg_any->type_id() != arrow::Type::BINARY) {
      continue;
    }

    int32_t collation_id = 63;
    if (i < output_agg_fields_.size() && output_agg_fields_[i] != nullptr) {
      ARROW_ASSIGN_OR_RAISE(const auto logical_type, GetLogicalType(*output_agg_fields_[i]));
      if (logical_type.id == LogicalTypeId::kString) {
        collation_id = logical_type.collation_id >= 0 ? logical_type.collation_id : 63;
      }
    }
    const auto collation = CollationFromId(collation_id);
    if (collation.kind == CollationKind::kUnsupported) {
      return arrow::Status::NotImplemented("unsupported collation id: ", collation_id);
    }
    agg_collations[i] = collation;
  }

  if (key_count == 0 && group_keys_.empty()) {
    NormalizedKey norm_key;
    norm_key.key_count = 0;
    OutputKey out_key;
    out_key.key_count = 0;
    ARROW_ASSIGN_OR_RAISE(const auto global_group,
                          GetOrAddGroup(std::move(norm_key), std::move(out_key)));
    if (global_group != 0) {
      return arrow::Status::Invalid("unexpected global agg group id");
    }
  }

  const int64_t rows = input.num_rows();
  for (int64_t row = 0; row < rows; ++row) {
    uint32_t group_id = 0;
    if (key_count != 0) {
      NormalizedKey norm_key;
      norm_key.key_count = static_cast<uint8_t>(key_count);
      OutputKey out_key;
      out_key.key_count = static_cast<uint8_t>(key_count);

      for (std::size_t ki = 0; ki < key_count; ++ki) {
        auto& norm_part = norm_key.parts[ki];
        auto& out_part = out_key.parts[ki];

        const auto& arr_any = key_arrays[ki];
        if (arr_any == nullptr) {
          return arrow::Status::Invalid("internal error: missing key array");
        }

        if (arr_any->IsNull(row)) {
          norm_part.is_null = true;
          out_part.is_null = true;
          continue;
        }

        norm_part.is_null = false;
        out_part.is_null = false;

        switch (arr_any->type_id()) {
          case arrow::Type::BOOL: {
            const auto& arr = static_cast<const arrow::BooleanArray&>(*arr_any);
            const uint64_t v = static_cast<uint64_t>(arr.Value(row) ? 1 : 0);
            norm_part.value = static_cast<uint64_t>(v);
            out_part.value = static_cast<uint64_t>(v);
            break;
          }
          case arrow::Type::INT8: {
            const auto& arr = static_cast<const arrow::Int8Array&>(*arr_any);
            const int64_t v = static_cast<int64_t>(arr.Value(row));
            norm_part.value = static_cast<int64_t>(v);
            out_part.value = static_cast<int64_t>(v);
            break;
          }
          case arrow::Type::INT16: {
            const auto& arr = static_cast<const arrow::Int16Array&>(*arr_any);
            const int64_t v = static_cast<int64_t>(arr.Value(row));
            norm_part.value = static_cast<int64_t>(v);
            out_part.value = static_cast<int64_t>(v);
            break;
          }
          case arrow::Type::INT32: {
            const auto& arr = static_cast<const arrow::Int32Array&>(*arr_any);
            const int64_t v = static_cast<int64_t>(arr.Value(row));
            norm_part.value = static_cast<int64_t>(v);
            out_part.value = static_cast<int64_t>(v);
            break;
          }
          case arrow::Type::INT64: {
            const auto& arr = static_cast<const arrow::Int64Array&>(*arr_any);
            const int64_t v = arr.Value(row);
            norm_part.value = static_cast<int64_t>(v);
            out_part.value = static_cast<int64_t>(v);
            break;
          }
          case arrow::Type::UINT8: {
            const auto& arr = static_cast<const arrow::UInt8Array&>(*arr_any);
            const uint64_t v = static_cast<uint64_t>(arr.Value(row));
            norm_part.value = static_cast<uint64_t>(v);
            out_part.value = static_cast<uint64_t>(v);
            break;
          }
          case arrow::Type::UINT16: {
            const auto& arr = static_cast<const arrow::UInt16Array&>(*arr_any);
            const uint64_t v = static_cast<uint64_t>(arr.Value(row));
            norm_part.value = static_cast<uint64_t>(v);
            out_part.value = static_cast<uint64_t>(v);
            break;
          }
          case arrow::Type::UINT32: {
            const auto& arr = static_cast<const arrow::UInt32Array&>(*arr_any);
            const uint64_t v = static_cast<uint64_t>(arr.Value(row));
            norm_part.value = static_cast<uint64_t>(v);
            out_part.value = static_cast<uint64_t>(v);
            break;
          }
          case arrow::Type::UINT64: {
            const auto& arr = static_cast<const arrow::UInt64Array&>(*arr_any);
            const uint64_t v = arr.Value(row);
            norm_part.value = static_cast<uint64_t>(v);
            out_part.value = static_cast<uint64_t>(v);
            break;
          }
          case arrow::Type::FLOAT: {
            const auto& arr = static_cast<const arrow::FloatArray&>(*arr_any);
            const float v = arr.Value(row);
            out_part.value = static_cast<uint64_t>(Float32ToBits(v));
            norm_part.value = static_cast<uint64_t>(CanonicalizeFloat32Bits(v));
            break;
          }
          case arrow::Type::DOUBLE: {
            const auto& arr = static_cast<const arrow::DoubleArray&>(*arr_any);
            const double v = arr.Value(row);
            out_part.value = static_cast<uint64_t>(Float64ToBits(v));
            norm_part.value = static_cast<uint64_t>(CanonicalizeFloat64Bits(v));
            break;
          }
          case arrow::Type::DECIMAL128: {
            const auto& arr = static_cast<const arrow::FixedSizeBinaryArray&>(*arr_any);
            if (arr.byte_width() != static_cast<int>(Decimal128Bytes{}.size())) {
              return arrow::Status::Invalid("unexpected decimal128 byte width");
            }
            Decimal128Bytes bytes{};
            std::memcpy(bytes.data(), arr.GetValue(row), bytes.size());
            norm_part.value = bytes;
            out_part.value = bytes;
            break;
          }
          case arrow::Type::DECIMAL256: {
            const auto& arr = static_cast<const arrow::FixedSizeBinaryArray&>(*arr_any);
            if (arr.byte_width() != static_cast<int>(Decimal256Bytes{}.size())) {
              return arrow::Status::Invalid("unexpected decimal256 byte width");
            }
            Decimal256Bytes bytes{};
            std::memcpy(bytes.data(), arr.GetValue(row), bytes.size());
            norm_part.value = bytes;
            out_part.value = bytes;
            break;
          }
          case arrow::Type::BINARY: {
            const auto& arr = static_cast<const arrow::BinaryArray&>(*arr_any);
            std::string_view view = arr.GetView(row);
            if (pmr_resource_ == nullptr) {
              return arrow::Status::Invalid("internal error: pmr resource must not be null");
            }
            std::pmr::string out_value(pmr_resource_.get());
            out_value.assign(view.data(), view.size());
            out_part.value = std::move(out_value);

            std::pmr::string norm_value(pmr_resource_.get());
            SortKeyStringTo(collations[ki], view, &norm_value);
            norm_part.value = std::move(norm_value);
            break;
          }
          default:
            return arrow::Status::NotImplemented("unsupported group key type: ",
                                                 arr_any->type()->ToString());
        }
      }

      ARROW_ASSIGN_OR_RAISE(group_id, GetOrAddGroup(std::move(norm_key), std::move(out_key)));
    }

    for (std::size_t i = 0; i < aggs_.size(); ++i) {
      auto& agg = aggs_[i];
      switch (agg.kind) {
        case AggState::Kind::kUnsupported:
          return arrow::Status::NotImplemented("unsupported aggregate func: ", agg.func);
        case AggState::Kind::kCountAll:
          ++agg.count_all[group_id];
          break;
        case AggState::Kind::kCount: {
          const auto& arg_any = agg_args[i];
          if (arg_any == nullptr) {
            return arrow::Status::Invalid("internal error: missing count arg array");
          }
          if (!arg_any->IsNull(row)) {
            ++agg.count[group_id];
          }
          break;
        }
        case AggState::Kind::kSum: {
          const auto& arg_any = agg_args[i];
          if (arg_any == nullptr) {
            return arrow::Status::Invalid("internal error: missing sum arg array");
          }
          if (arg_any->IsNull(row)) {
            break;
          }
          if (group_id >= agg.sum_has_value.size()) {
            return arrow::Status::Invalid("internal error: sum group id out of range");
          }
          agg.sum_has_value[group_id] = 1;

          switch (agg.sum_kind) {
            case AggState::SumKind::kUnresolved:
              return arrow::Status::Invalid("internal error: sum kind must be resolved");
            case AggState::SumKind::kInt64: {
              if (group_id >= agg.sum_i64.size()) {
                return arrow::Status::Invalid("internal error: sum group id out of range");
              }
              int64_t add = 0;
              switch (arg_any->type_id()) {
                case arrow::Type::INT8: {
                  const auto& arr = static_cast<const arrow::Int8Array&>(*arg_any);
                  add = static_cast<int64_t>(arr.Value(row));
                  break;
                }
                case arrow::Type::INT16: {
                  const auto& arr = static_cast<const arrow::Int16Array&>(*arg_any);
                  add = static_cast<int64_t>(arr.Value(row));
                  break;
                }
                case arrow::Type::INT32: {
                  const auto& arr = static_cast<const arrow::Int32Array&>(*arg_any);
                  add = static_cast<int64_t>(arr.Value(row));
                  break;
                }
                case arrow::Type::INT64: {
                  const auto& arr = static_cast<const arrow::Int64Array&>(*arg_any);
                  add = arr.Value(row);
                  break;
                }
                default:
                  return arrow::Status::NotImplemented("sum(int64) arg type not supported: ",
                                                       arg_any->type()->ToString());
              }
              agg.sum_i64[group_id] += add;
              break;
            }
            case AggState::SumKind::kUInt64: {
              if (group_id >= agg.sum_u64.size()) {
                return arrow::Status::Invalid("internal error: sum group id out of range");
              }
              uint64_t add = 0;
              switch (arg_any->type_id()) {
                case arrow::Type::BOOL: {
                  const auto& arr = static_cast<const arrow::BooleanArray&>(*arg_any);
                  add = static_cast<uint64_t>(arr.Value(row) ? 1 : 0);
                  break;
                }
                case arrow::Type::UINT8: {
                  const auto& arr = static_cast<const arrow::UInt8Array&>(*arg_any);
                  add = static_cast<uint64_t>(arr.Value(row));
                  break;
                }
                case arrow::Type::UINT16: {
                  const auto& arr = static_cast<const arrow::UInt16Array&>(*arg_any);
                  add = static_cast<uint64_t>(arr.Value(row));
                  break;
                }
                case arrow::Type::UINT32: {
                  const auto& arr = static_cast<const arrow::UInt32Array&>(*arg_any);
                  add = static_cast<uint64_t>(arr.Value(row));
                  break;
                }
                case arrow::Type::UINT64: {
                  const auto& arr = static_cast<const arrow::UInt64Array&>(*arg_any);
                  add = arr.Value(row);
                  break;
                }
                default:
                  return arrow::Status::NotImplemented("sum(uint64) arg type not supported: ",
                                                       arg_any->type()->ToString());
              }
              agg.sum_u64[group_id] += add;
              break;
            }
          }
          break;
        }
        case AggState::Kind::kMin:
        case AggState::Kind::kMax: {
          const auto& arg_any = agg_args[i];
          if (arg_any == nullptr) {
            return arrow::Status::Invalid("internal error: missing ", agg.func, " arg array");
          }
          if (arg_any->IsNull(row)) {
            break;
          }
          if (group_id >= agg.extreme_norm.size() || group_id >= agg.extreme_out.size()) {
            return arrow::Status::Invalid("internal error: group id out of range");
          }

          KeyPart out;
          out.is_null = false;
          KeyPart norm;
          norm.is_null = false;

          switch (arg_any->type_id()) {
            case arrow::Type::INT8: {
              const auto& arr = static_cast<const arrow::Int8Array&>(*arg_any);
              const int64_t v = static_cast<int64_t>(arr.Value(row));
              out.value = static_cast<int64_t>(v);
              norm.value = static_cast<int64_t>(v);
              break;
            }
            case arrow::Type::INT16: {
              const auto& arr = static_cast<const arrow::Int16Array&>(*arg_any);
              const int64_t v = static_cast<int64_t>(arr.Value(row));
              out.value = static_cast<int64_t>(v);
              norm.value = static_cast<int64_t>(v);
              break;
            }
            case arrow::Type::INT32: {
              const auto& arr = static_cast<const arrow::Int32Array&>(*arg_any);
              const int64_t v = static_cast<int64_t>(arr.Value(row));
              out.value = static_cast<int64_t>(v);
              norm.value = static_cast<int64_t>(v);
              break;
            }
            case arrow::Type::INT64: {
              const auto& arr = static_cast<const arrow::Int64Array&>(*arg_any);
              const int64_t v = arr.Value(row);
              out.value = static_cast<int64_t>(v);
              norm.value = static_cast<int64_t>(v);
              break;
            }
            case arrow::Type::UINT8: {
              const auto& arr = static_cast<const arrow::UInt8Array&>(*arg_any);
              const uint64_t v = static_cast<uint64_t>(arr.Value(row));
              out.value = static_cast<uint64_t>(v);
              norm.value = static_cast<uint64_t>(v);
              break;
            }
            case arrow::Type::UINT16: {
              const auto& arr = static_cast<const arrow::UInt16Array&>(*arg_any);
              const uint64_t v = static_cast<uint64_t>(arr.Value(row));
              out.value = static_cast<uint64_t>(v);
              norm.value = static_cast<uint64_t>(v);
              break;
            }
            case arrow::Type::UINT32: {
              const auto& arr = static_cast<const arrow::UInt32Array&>(*arg_any);
              const uint64_t v = static_cast<uint64_t>(arr.Value(row));
              out.value = static_cast<uint64_t>(v);
              norm.value = static_cast<uint64_t>(v);
              break;
            }
            case arrow::Type::UINT64: {
              const auto& arr = static_cast<const arrow::UInt64Array&>(*arg_any);
              const uint64_t v = arr.Value(row);
              out.value = static_cast<uint64_t>(v);
              norm.value = static_cast<uint64_t>(v);
              break;
            }
            case arrow::Type::BINARY: {
              const auto& arr = static_cast<const arrow::BinaryArray&>(*arg_any);
              std::string_view view = arr.GetView(row);
              if (pmr_resource_ == nullptr) {
                return arrow::Status::Invalid("internal error: pmr resource must not be null");
              }
              std::pmr::string out_value(pmr_resource_.get());
              out_value.assign(view.data(), view.size());
              out.value = std::move(out_value);

              std::pmr::string norm_value(pmr_resource_.get());
              SortKeyStringTo(agg_collations[i], view, &norm_value);
              norm.value = std::move(norm_value);
              break;
            }
            default:
              return arrow::Status::NotImplemented(agg.func, " arg type not supported: ",
                                                   arg_any->type()->ToString());
          }

          auto& cur_norm = agg.extreme_norm[group_id];
          auto& cur_out = agg.extreme_out[group_id];
          if (cur_norm.is_null) {
            cur_norm = std::move(norm);
            cur_out = std::move(out);
            break;
          }

          bool take = false;
          switch (arg_any->type_id()) {
            case arrow::Type::INT8:
            case arrow::Type::INT16:
            case arrow::Type::INT32:
            case arrow::Type::INT64: {
              const int64_t cand = std::get<int64_t>(norm.value);
              const int64_t cur = std::get<int64_t>(cur_norm.value);
              take = (agg.kind == AggState::Kind::kMin) ? (cand < cur) : (cand > cur);
              break;
            }
            case arrow::Type::UINT8:
            case arrow::Type::UINT16:
            case arrow::Type::UINT32:
            case arrow::Type::UINT64: {
              const uint64_t cand = std::get<uint64_t>(norm.value);
              const uint64_t cur = std::get<uint64_t>(cur_norm.value);
              take = (agg.kind == AggState::Kind::kMin) ? (cand < cur) : (cand > cur);
              break;
            }
            case arrow::Type::BINARY: {
              const auto& cand = std::get<std::pmr::string>(norm.value);
              const auto& cur = std::get<std::pmr::string>(cur_norm.value);
              const int cmp = cand.compare(cur);
              take = (agg.kind == AggState::Kind::kMin) ? (cmp < 0) : (cmp > 0);
              break;
            }
            default:
              return arrow::Status::NotImplemented(agg.func, " arg type not supported: ",
                                                   arg_any->type()->ToString());
          }

          if (take) {
            cur_norm = std::move(norm);
            cur_out = std::move(out);
          }
          break;
        }
      }
    }
  }

  return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::Schema>> HashAggTransformOp::BuildOutputSchema() const {
  const std::size_t key_count = keys_.size();
  if (key_count > kMaxKeys) {
    return arrow::Status::NotImplemented("hash agg supports up to ", kMaxKeys, " group keys");
  }
  if (output_key_fields_.size() != key_count) {
    return arrow::Status::Invalid("group key field count mismatch");
  }
  if (output_agg_fields_.size() != aggs_.size()) {
    return arrow::Status::Invalid("agg output field count mismatch");
  }

  std::vector<std::shared_ptr<arrow::Field>> fields;
  fields.reserve(aggs_.size() + key_count);

  for (std::size_t i = 0; i < output_agg_fields_.size(); ++i) {
    const auto& field = output_agg_fields_[i];
    if (field == nullptr) {
      return arrow::Status::Invalid("agg output field must not be null");
    }
    fields.push_back(field);
  }

  for (std::size_t i = 0; i < key_count; ++i) {
    if (keys_[i].name.empty()) {
      return arrow::Status::Invalid("group key name must not be empty");
    }
    if (output_key_fields_[i] == nullptr) {
      return arrow::Status::Invalid("group key field must not be null");
    }
    fields.push_back(output_key_fields_[i]->WithName(keys_[i].name));
  }

  return arrow::schema(std::move(fields));
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> HashAggTransformOp::FinalizeOutput() {
  if (output_schema_ == nullptr) {
    ARROW_ASSIGN_OR_RAISE(output_schema_, BuildOutputSchema());
  }

  const std::size_t key_count = keys_.size();
  const std::size_t group_count = group_keys_.size();

  if (output_key_fields_.size() != key_count) {
    return arrow::Status::Invalid("group key field count mismatch");
  }
  if (output_agg_fields_.size() != aggs_.size()) {
    return arrow::Status::Invalid("agg output field count mismatch");
  }

  std::vector<std::shared_ptr<arrow::Array>> columns;
  columns.reserve(aggs_.size() + key_count);

  for (std::size_t ai = 0; ai < aggs_.size(); ++ai) {
    const auto& agg = aggs_[ai];
    const auto& field = output_agg_fields_[ai];
    if (field == nullptr) {
      return arrow::Status::Invalid("agg output field must not be null");
    }
    switch (agg.kind) {
      case AggState::Kind::kUnsupported:
        return arrow::Status::NotImplemented("unsupported aggregate func: ", agg.func);
      case AggState::Kind::kCountAll: {
        arrow::UInt64Builder builder(memory_pool_);
        ARROW_RETURN_NOT_OK(builder.AppendValues(agg.count_all));
        std::shared_ptr<arrow::Array> out;
        ARROW_RETURN_NOT_OK(builder.Finish(&out));
        columns.push_back(std::move(out));
        break;
      }
      case AggState::Kind::kCount: {
        arrow::UInt64Builder builder(memory_pool_);
        ARROW_RETURN_NOT_OK(builder.AppendValues(agg.count));
        std::shared_ptr<arrow::Array> out;
        ARROW_RETURN_NOT_OK(builder.Finish(&out));
        columns.push_back(std::move(out));
        break;
      }
      case AggState::Kind::kSum: {
        if (agg.sum_has_value.size() != group_count) {
          return arrow::Status::Invalid("agg state size mismatch");
        }
        std::shared_ptr<arrow::Array> out;
        switch (agg.sum_kind) {
          case AggState::SumKind::kUnresolved:
            return arrow::Status::Invalid("internal error: sum kind must be resolved");
          case AggState::SumKind::kInt64: {
            if (agg.sum_i64.size() != group_count) {
              return arrow::Status::Invalid("agg state size mismatch");
            }
            arrow::Int64Builder builder(memory_pool_);
            ARROW_RETURN_NOT_OK(builder.Reserve(static_cast<int64_t>(group_count)));
            for (std::size_t i = 0; i < group_count; ++i) {
              if (agg.sum_has_value[i] == 0) {
                ARROW_RETURN_NOT_OK(builder.AppendNull());
              } else {
                ARROW_RETURN_NOT_OK(builder.Append(agg.sum_i64[i]));
              }
            }
            ARROW_RETURN_NOT_OK(builder.Finish(&out));
            break;
          }
          case AggState::SumKind::kUInt64: {
            if (agg.sum_u64.size() != group_count) {
              return arrow::Status::Invalid("agg state size mismatch");
            }
            arrow::UInt64Builder builder(memory_pool_);
            ARROW_RETURN_NOT_OK(builder.Reserve(static_cast<int64_t>(group_count)));
            for (std::size_t i = 0; i < group_count; ++i) {
              if (agg.sum_has_value[i] == 0) {
                ARROW_RETURN_NOT_OK(builder.AppendNull());
              } else {
                ARROW_RETURN_NOT_OK(builder.Append(agg.sum_u64[i]));
              }
            }
            ARROW_RETURN_NOT_OK(builder.Finish(&out));
            break;
          }
        }
        columns.push_back(std::move(out));
        break;
      }
      case AggState::Kind::kMin:
      case AggState::Kind::kMax: {
        if (agg.extreme_out.size() != group_count) {
          return arrow::Status::Invalid("agg state size mismatch");
        }
        std::shared_ptr<arrow::Array> out;
        switch (field->type()->id()) {
          case arrow::Type::INT8: {
            arrow::Int8Builder builder(memory_pool_);
            for (const auto& part : agg.extreme_out) {
              if (part.is_null) {
                ARROW_RETURN_NOT_OK(builder.AppendNull());
              } else {
                ARROW_RETURN_NOT_OK(
                    builder.Append(static_cast<int8_t>(std::get<int64_t>(part.value))));
              }
            }
            ARROW_RETURN_NOT_OK(builder.Finish(&out));
            break;
          }
          case arrow::Type::INT16: {
            arrow::Int16Builder builder(memory_pool_);
            for (const auto& part : agg.extreme_out) {
              if (part.is_null) {
                ARROW_RETURN_NOT_OK(builder.AppendNull());
              } else {
                ARROW_RETURN_NOT_OK(
                    builder.Append(static_cast<int16_t>(std::get<int64_t>(part.value))));
              }
            }
            ARROW_RETURN_NOT_OK(builder.Finish(&out));
            break;
          }
          case arrow::Type::INT32: {
            arrow::Int32Builder builder(memory_pool_);
            for (const auto& part : agg.extreme_out) {
              if (part.is_null) {
                ARROW_RETURN_NOT_OK(builder.AppendNull());
              } else {
                ARROW_RETURN_NOT_OK(
                    builder.Append(static_cast<int32_t>(std::get<int64_t>(part.value))));
              }
            }
            ARROW_RETURN_NOT_OK(builder.Finish(&out));
            break;
          }
          case arrow::Type::INT64: {
            arrow::Int64Builder builder(memory_pool_);
            for (const auto& part : agg.extreme_out) {
              if (part.is_null) {
                ARROW_RETURN_NOT_OK(builder.AppendNull());
              } else {
                ARROW_RETURN_NOT_OK(builder.Append(std::get<int64_t>(part.value)));
              }
            }
            ARROW_RETURN_NOT_OK(builder.Finish(&out));
            break;
          }
          case arrow::Type::UINT8: {
            arrow::UInt8Builder builder(memory_pool_);
            for (const auto& part : agg.extreme_out) {
              if (part.is_null) {
                ARROW_RETURN_NOT_OK(builder.AppendNull());
              } else {
                ARROW_RETURN_NOT_OK(
                    builder.Append(static_cast<uint8_t>(std::get<uint64_t>(part.value))));
              }
            }
            ARROW_RETURN_NOT_OK(builder.Finish(&out));
            break;
          }
          case arrow::Type::UINT16: {
            arrow::UInt16Builder builder(memory_pool_);
            for (const auto& part : agg.extreme_out) {
              if (part.is_null) {
                ARROW_RETURN_NOT_OK(builder.AppendNull());
              } else {
                ARROW_RETURN_NOT_OK(
                    builder.Append(static_cast<uint16_t>(std::get<uint64_t>(part.value))));
              }
            }
            ARROW_RETURN_NOT_OK(builder.Finish(&out));
            break;
          }
          case arrow::Type::UINT32: {
            arrow::UInt32Builder builder(memory_pool_);
            for (const auto& part : agg.extreme_out) {
              if (part.is_null) {
                ARROW_RETURN_NOT_OK(builder.AppendNull());
              } else {
                ARROW_RETURN_NOT_OK(
                    builder.Append(static_cast<uint32_t>(std::get<uint64_t>(part.value))));
              }
            }
            ARROW_RETURN_NOT_OK(builder.Finish(&out));
            break;
          }
          case arrow::Type::UINT64: {
            arrow::UInt64Builder builder(memory_pool_);
            for (const auto& part : agg.extreme_out) {
              if (part.is_null) {
                ARROW_RETURN_NOT_OK(builder.AppendNull());
              } else {
                ARROW_RETURN_NOT_OK(builder.Append(std::get<uint64_t>(part.value)));
              }
            }
            ARROW_RETURN_NOT_OK(builder.Finish(&out));
            break;
          }
          case arrow::Type::BINARY: {
            arrow::BinaryBuilder builder(memory_pool_);
            for (const auto& part : agg.extreme_out) {
              if (part.is_null) {
                ARROW_RETURN_NOT_OK(builder.AppendNull());
              } else {
                const auto& bytes = std::get<std::pmr::string>(part.value);
                ARROW_RETURN_NOT_OK(builder.Append(reinterpret_cast<const uint8_t*>(bytes.data()),
                                                   static_cast<int32_t>(bytes.size())));
              }
            }
            ARROW_RETURN_NOT_OK(builder.Finish(&out));
            break;
          }
          default:
            return arrow::Status::NotImplemented("unsupported min/max output type: ",
                                                 field->type()->ToString());
        }
        columns.push_back(std::move(out));
        break;
      }
    }
  }

  for (std::size_t ki = 0; ki < key_count; ++ki) {
    const auto& field = output_key_fields_[ki];
    if (field == nullptr) {
      return arrow::Status::Invalid("group key field must not be null");
    }

    std::shared_ptr<arrow::Array> out_key;
    switch (field->type()->id()) {
      case arrow::Type::BOOL: {
        arrow::BooleanBuilder builder(memory_pool_);
        for (const auto& key : group_keys_) {
          if (key.key_count != key_count) {
            return arrow::Status::Invalid("group key arity mismatch");
          }
          const auto& part = key.parts[ki];
          if (part.is_null) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
          } else {
            ARROW_RETURN_NOT_OK(builder.Append(std::get<uint64_t>(part.value) != 0));
          }
        }
        ARROW_RETURN_NOT_OK(builder.Finish(&out_key));
        break;
      }
      case arrow::Type::INT8: {
        arrow::Int8Builder builder(memory_pool_);
        for (const auto& key : group_keys_) {
          if (key.key_count != key_count) {
            return arrow::Status::Invalid("group key arity mismatch");
          }
          const auto& part = key.parts[ki];
          if (part.is_null) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
          } else {
            ARROW_RETURN_NOT_OK(builder.Append(static_cast<int8_t>(std::get<int64_t>(part.value))));
          }
        }
        ARROW_RETURN_NOT_OK(builder.Finish(&out_key));
        break;
      }
      case arrow::Type::INT16: {
        arrow::Int16Builder builder(memory_pool_);
        for (const auto& key : group_keys_) {
          if (key.key_count != key_count) {
            return arrow::Status::Invalid("group key arity mismatch");
          }
          const auto& part = key.parts[ki];
          if (part.is_null) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
          } else {
            ARROW_RETURN_NOT_OK(
                builder.Append(static_cast<int16_t>(std::get<int64_t>(part.value))));
          }
        }
        ARROW_RETURN_NOT_OK(builder.Finish(&out_key));
        break;
      }
      case arrow::Type::INT32: {
        arrow::Int32Builder builder(memory_pool_);
        for (const auto& key : group_keys_) {
          if (key.key_count != key_count) {
            return arrow::Status::Invalid("group key arity mismatch");
          }
          const auto& part = key.parts[ki];
          if (part.is_null) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
          } else {
            ARROW_RETURN_NOT_OK(
                builder.Append(static_cast<int32_t>(std::get<int64_t>(part.value))));
          }
        }
        ARROW_RETURN_NOT_OK(builder.Finish(&out_key));
        break;
      }
      case arrow::Type::INT64: {
        arrow::Int64Builder builder(memory_pool_);
        for (const auto& key : group_keys_) {
          if (key.key_count != key_count) {
            return arrow::Status::Invalid("group key arity mismatch");
          }
          const auto& part = key.parts[ki];
          if (part.is_null) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
          } else {
            ARROW_RETURN_NOT_OK(builder.Append(std::get<int64_t>(part.value)));
          }
        }
        ARROW_RETURN_NOT_OK(builder.Finish(&out_key));
        break;
      }
      case arrow::Type::UINT8: {
        arrow::UInt8Builder builder(memory_pool_);
        for (const auto& key : group_keys_) {
          if (key.key_count != key_count) {
            return arrow::Status::Invalid("group key arity mismatch");
          }
          const auto& part = key.parts[ki];
          if (part.is_null) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
          } else {
            ARROW_RETURN_NOT_OK(
                builder.Append(static_cast<uint8_t>(std::get<uint64_t>(part.value))));
          }
        }
        ARROW_RETURN_NOT_OK(builder.Finish(&out_key));
        break;
      }
      case arrow::Type::UINT16: {
        arrow::UInt16Builder builder(memory_pool_);
        for (const auto& key : group_keys_) {
          if (key.key_count != key_count) {
            return arrow::Status::Invalid("group key arity mismatch");
          }
          const auto& part = key.parts[ki];
          if (part.is_null) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
          } else {
            ARROW_RETURN_NOT_OK(
                builder.Append(static_cast<uint16_t>(std::get<uint64_t>(part.value))));
          }
        }
        ARROW_RETURN_NOT_OK(builder.Finish(&out_key));
        break;
      }
      case arrow::Type::UINT32: {
        arrow::UInt32Builder builder(memory_pool_);
        for (const auto& key : group_keys_) {
          if (key.key_count != key_count) {
            return arrow::Status::Invalid("group key arity mismatch");
          }
          const auto& part = key.parts[ki];
          if (part.is_null) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
          } else {
            ARROW_RETURN_NOT_OK(
                builder.Append(static_cast<uint32_t>(std::get<uint64_t>(part.value))));
          }
        }
        ARROW_RETURN_NOT_OK(builder.Finish(&out_key));
        break;
      }
      case arrow::Type::UINT64: {
        arrow::UInt64Builder builder(memory_pool_);
        for (const auto& key : group_keys_) {
          if (key.key_count != key_count) {
            return arrow::Status::Invalid("group key arity mismatch");
          }
          const auto& part = key.parts[ki];
          if (part.is_null) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
          } else {
            ARROW_RETURN_NOT_OK(builder.Append(std::get<uint64_t>(part.value)));
          }
        }
        ARROW_RETURN_NOT_OK(builder.Finish(&out_key));
        break;
      }
      case arrow::Type::FLOAT: {
        arrow::FloatBuilder builder(memory_pool_);
        for (const auto& key : group_keys_) {
          if (key.key_count != key_count) {
            return arrow::Status::Invalid("group key arity mismatch");
          }
          const auto& part = key.parts[ki];
          if (part.is_null) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
          } else {
            ARROW_RETURN_NOT_OK(builder.Append(BitsToFloat32(std::get<uint64_t>(part.value))));
          }
        }
        ARROW_RETURN_NOT_OK(builder.Finish(&out_key));
        break;
      }
      case arrow::Type::DOUBLE: {
        arrow::DoubleBuilder builder(memory_pool_);
        for (const auto& key : group_keys_) {
          if (key.key_count != key_count) {
            return arrow::Status::Invalid("group key arity mismatch");
          }
          const auto& part = key.parts[ki];
          if (part.is_null) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
          } else {
            ARROW_RETURN_NOT_OK(builder.Append(BitsToFloat64(std::get<uint64_t>(part.value))));
          }
        }
        ARROW_RETURN_NOT_OK(builder.Finish(&out_key));
        break;
      }
      case arrow::Type::DECIMAL128: {
        arrow::Decimal128Builder builder(field->type(), memory_pool_);
        for (const auto& key : group_keys_) {
          if (key.key_count != key_count) {
            return arrow::Status::Invalid("group key arity mismatch");
          }
          const auto& part = key.parts[ki];
          if (part.is_null) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
          } else {
            const auto& bytes = std::get<Decimal128Bytes>(part.value);
            ARROW_RETURN_NOT_OK(builder.Append(std::string_view(
                reinterpret_cast<const char*>(bytes.data()), bytes.size())));
          }
        }
        ARROW_RETURN_NOT_OK(builder.Finish(&out_key));
        break;
      }
      case arrow::Type::DECIMAL256: {
        arrow::Decimal256Builder builder(field->type(), memory_pool_);
        for (const auto& key : group_keys_) {
          if (key.key_count != key_count) {
            return arrow::Status::Invalid("group key arity mismatch");
          }
          const auto& part = key.parts[ki];
          if (part.is_null) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
          } else {
            const auto& bytes = std::get<Decimal256Bytes>(part.value);
            ARROW_RETURN_NOT_OK(builder.Append(std::string_view(
                reinterpret_cast<const char*>(bytes.data()), bytes.size())));
          }
        }
        ARROW_RETURN_NOT_OK(builder.Finish(&out_key));
        break;
      }
      case arrow::Type::BINARY: {
        arrow::BinaryBuilder builder(memory_pool_);
        for (const auto& key : group_keys_) {
          if (key.key_count != key_count) {
            return arrow::Status::Invalid("group key arity mismatch");
          }
          const auto& part = key.parts[ki];
          if (part.is_null) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
          } else {
            const auto& bytes = std::get<std::pmr::string>(part.value);
            ARROW_RETURN_NOT_OK(builder.Append(reinterpret_cast<const uint8_t*>(bytes.data()),
                                               static_cast<int32_t>(bytes.size())));
          }
        }
        ARROW_RETURN_NOT_OK(builder.Finish(&out_key));
        break;
      }
      default:
        return arrow::Status::NotImplemented("unsupported group key output type: ",
                                             field->type()->ToString());
    }

    columns.push_back(std::move(out_key));
  }

  return arrow::RecordBatch::Make(output_schema_, static_cast<int64_t>(group_count), std::move(columns));
}

arrow::Result<OperatorStatus> HashAggTransformOp::TransformImpl(
    std::shared_ptr<arrow::RecordBatch>* batch) {
  if (*batch == nullptr) {
    if (!finalized_) {
      ARROW_ASSIGN_OR_RAISE(*batch, FinalizeOutput());
      finalized_ = true;
      return OperatorStatus::kHasOutput;
    }
    if (!eos_forwarded_) {
      eos_forwarded_ = true;
      batch->reset();
      return OperatorStatus::kHasOutput;
    }
    batch->reset();
    return OperatorStatus::kHasOutput;
  }

  if (finalized_) {
    return arrow::Status::Invalid("hash agg received input after finalization");
  }

  const auto& input = **batch;
  ARROW_RETURN_NOT_OK(ConsumeBatch(input));
  batch->reset();
  return OperatorStatus::kNeedInput;
}

}  // namespace tiforth
