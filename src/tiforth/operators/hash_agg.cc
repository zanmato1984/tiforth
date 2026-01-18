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
#include <arrow/util/logging.h>

#include "tiforth/compiled_expr.h"
#include "tiforth/engine.h"
#include "tiforth/collation.h"
#include "tiforth/type_metadata.h"

namespace tiforth {

namespace {

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

class AggCountAll final : public detail::AggregateFunction {
 public:
  struct State {
    uint64_t count = 0;
  };

  int64_t state_size() const override { return static_cast<int64_t>(sizeof(State)); }
  int64_t state_alignment() const override { return static_cast<int64_t>(alignof(State)); }

  void Create(uint8_t* state) const override { new (state) State(); }
  void Destroy(uint8_t* state) const override { reinterpret_cast<State*>(state)->~State(); }

  arrow::Status Add(uint8_t* state, const arrow::Array* arg, int64_t row) const override {
    (void)arg;
    (void)row;
    ++reinterpret_cast<State*>(state)->count;
    return arrow::Status::OK();
  }

  arrow::Status Finalize(const uint8_t* state, arrow::ArrayBuilder* out) const override {
    if (out == nullptr || out->type()->id() != arrow::Type::UINT64) {
      return arrow::Status::Invalid("count_all expects uint64 output builder");
    }
    auto* builder = static_cast<arrow::UInt64Builder*>(out);
    return builder->Append(reinterpret_cast<const State*>(state)->count);
  }
};

class AggCount final : public detail::AggregateFunction {
 public:
  struct State {
    uint64_t count = 0;
  };

  int64_t state_size() const override { return static_cast<int64_t>(sizeof(State)); }
  int64_t state_alignment() const override { return static_cast<int64_t>(alignof(State)); }

  void Create(uint8_t* state) const override { new (state) State(); }
  void Destroy(uint8_t* state) const override { reinterpret_cast<State*>(state)->~State(); }

  arrow::Status Add(uint8_t* state, const arrow::Array* arg, int64_t row) const override {
    if (arg == nullptr) {
      return arrow::Status::Invalid("count expects non-null arg array");
    }
    if (!arg->IsNull(row)) {
      ++reinterpret_cast<State*>(state)->count;
    }
    return arrow::Status::OK();
  }

  arrow::Status Finalize(const uint8_t* state, arrow::ArrayBuilder* out) const override {
    if (out == nullptr || out->type()->id() != arrow::Type::UINT64) {
      return arrow::Status::Invalid("count expects uint64 output builder");
    }
    auto* builder = static_cast<arrow::UInt64Builder*>(out);
    return builder->Append(reinterpret_cast<const State*>(state)->count);
  }
};

class AggSumInt64 final : public detail::AggregateFunction {
 public:
  struct State {
    int64_t sum = 0;
    uint8_t has_value = 0;
  };

  int64_t state_size() const override { return static_cast<int64_t>(sizeof(State)); }
  int64_t state_alignment() const override { return static_cast<int64_t>(alignof(State)); }

  void Create(uint8_t* state) const override { new (state) State(); }
  void Destroy(uint8_t* state) const override { reinterpret_cast<State*>(state)->~State(); }

  arrow::Status Add(uint8_t* state, const arrow::Array* arg, int64_t row) const override {
    if (arg == nullptr) {
      return arrow::Status::Invalid("sum(int64) expects non-null arg array");
    }
    if (arg->IsNull(row)) {
      return arrow::Status::OK();
    }

    int64_t add = 0;
    switch (arg->type_id()) {
      case arrow::Type::INT8: {
        const auto& arr = static_cast<const arrow::Int8Array&>(*arg);
        add = static_cast<int64_t>(arr.Value(row));
        break;
      }
      case arrow::Type::INT16: {
        const auto& arr = static_cast<const arrow::Int16Array&>(*arg);
        add = static_cast<int64_t>(arr.Value(row));
        break;
      }
      case arrow::Type::INT32: {
        const auto& arr = static_cast<const arrow::Int32Array&>(*arg);
        add = static_cast<int64_t>(arr.Value(row));
        break;
      }
      case arrow::Type::INT64: {
        const auto& arr = static_cast<const arrow::Int64Array&>(*arg);
        add = arr.Value(row);
        break;
      }
      default:
        return arrow::Status::NotImplemented("sum(int64) arg type not supported: ",
                                             arg->type()->ToString());
    }

    auto* s = reinterpret_cast<State*>(state);
    s->has_value = 1;
    s->sum += add;
    return arrow::Status::OK();
  }

  arrow::Status Finalize(const uint8_t* state, arrow::ArrayBuilder* out) const override {
    if (out == nullptr || out->type()->id() != arrow::Type::INT64) {
      return arrow::Status::Invalid("sum(int64) expects int64 output builder");
    }
    auto* builder = static_cast<arrow::Int64Builder*>(out);
    const auto* s = reinterpret_cast<const State*>(state);
    if (s->has_value == 0) {
      return builder->AppendNull();
    }
    return builder->Append(s->sum);
  }
};

class AggSumUInt64 final : public detail::AggregateFunction {
 public:
  struct State {
    uint64_t sum = 0;
    uint8_t has_value = 0;
  };

  int64_t state_size() const override { return static_cast<int64_t>(sizeof(State)); }
  int64_t state_alignment() const override { return static_cast<int64_t>(alignof(State)); }

  void Create(uint8_t* state) const override { new (state) State(); }
  void Destroy(uint8_t* state) const override { reinterpret_cast<State*>(state)->~State(); }

  arrow::Status Add(uint8_t* state, const arrow::Array* arg, int64_t row) const override {
    if (arg == nullptr) {
      return arrow::Status::Invalid("sum(uint64) expects non-null arg array");
    }
    if (arg->IsNull(row)) {
      return arrow::Status::OK();
    }

    uint64_t add = 0;
    switch (arg->type_id()) {
      case arrow::Type::BOOL: {
        const auto& arr = static_cast<const arrow::BooleanArray&>(*arg);
        add = static_cast<uint64_t>(arr.Value(row) ? 1 : 0);
        break;
      }
      case arrow::Type::UINT8: {
        const auto& arr = static_cast<const arrow::UInt8Array&>(*arg);
        add = static_cast<uint64_t>(arr.Value(row));
        break;
      }
      case arrow::Type::UINT16: {
        const auto& arr = static_cast<const arrow::UInt16Array&>(*arg);
        add = static_cast<uint64_t>(arr.Value(row));
        break;
      }
      case arrow::Type::UINT32: {
        const auto& arr = static_cast<const arrow::UInt32Array&>(*arg);
        add = static_cast<uint64_t>(arr.Value(row));
        break;
      }
      case arrow::Type::UINT64: {
        const auto& arr = static_cast<const arrow::UInt64Array&>(*arg);
        add = arr.Value(row);
        break;
      }
      default:
        return arrow::Status::NotImplemented("sum(uint64) arg type not supported: ",
                                             arg->type()->ToString());
    }

    auto* s = reinterpret_cast<State*>(state);
    s->has_value = 1;
    s->sum += add;
    return arrow::Status::OK();
  }

  arrow::Status Finalize(const uint8_t* state, arrow::ArrayBuilder* out) const override {
    if (out == nullptr || out->type()->id() != arrow::Type::UINT64) {
      return arrow::Status::Invalid("sum(uint64) expects uint64 output builder");
    }
    auto* builder = static_cast<arrow::UInt64Builder*>(out);
    const auto* s = reinterpret_cast<const State*>(state);
    if (s->has_value == 0) {
      return builder->AppendNull();
    }
    return builder->Append(s->sum);
  }
};

class AggMinMaxSigned final : public detail::AggregateFunction {
 public:
  struct State {
    int64_t value = 0;
    uint8_t has_value = 0;
  };

  AggMinMaxSigned(bool is_min, arrow::Type::type output_type_id)
      : is_min_(is_min), output_type_id_(output_type_id) {}

  int64_t state_size() const override { return static_cast<int64_t>(sizeof(State)); }
  int64_t state_alignment() const override { return static_cast<int64_t>(alignof(State)); }

  void Create(uint8_t* state) const override { new (state) State(); }
  void Destroy(uint8_t* state) const override { reinterpret_cast<State*>(state)->~State(); }

  arrow::Status Add(uint8_t* state, const arrow::Array* arg, int64_t row) const override {
    if (arg == nullptr) {
      return arrow::Status::Invalid("min/max expects non-null arg array");
    }
    if (arg->IsNull(row)) {
      return arrow::Status::OK();
    }

    int64_t cand = 0;
    switch (arg->type_id()) {
      case arrow::Type::INT8: {
        const auto& arr = static_cast<const arrow::Int8Array&>(*arg);
        cand = static_cast<int64_t>(arr.Value(row));
        break;
      }
      case arrow::Type::INT16: {
        const auto& arr = static_cast<const arrow::Int16Array&>(*arg);
        cand = static_cast<int64_t>(arr.Value(row));
        break;
      }
      case arrow::Type::INT32: {
        const auto& arr = static_cast<const arrow::Int32Array&>(*arg);
        cand = static_cast<int64_t>(arr.Value(row));
        break;
      }
      case arrow::Type::INT64: {
        const auto& arr = static_cast<const arrow::Int64Array&>(*arg);
        cand = arr.Value(row);
        break;
      }
      default:
        return arrow::Status::NotImplemented("unsupported min/max arg type: ",
                                             arg->type()->ToString());
    }

    auto* s = reinterpret_cast<State*>(state);
    if (s->has_value == 0) {
      s->has_value = 1;
      s->value = cand;
      return arrow::Status::OK();
    }
    if ((is_min_ && cand < s->value) || (!is_min_ && cand > s->value)) {
      s->value = cand;
    }
    return arrow::Status::OK();
  }

  arrow::Status Finalize(const uint8_t* state, arrow::ArrayBuilder* out) const override {
    if (out == nullptr || out->type()->id() != output_type_id_) {
      return arrow::Status::Invalid("min/max output builder type mismatch");
    }
    const auto* s = reinterpret_cast<const State*>(state);
    if (s->has_value == 0) {
      return out->AppendNull();
    }

    switch (output_type_id_) {
      case arrow::Type::INT8:
        return static_cast<arrow::Int8Builder*>(out)->Append(static_cast<int8_t>(s->value));
      case arrow::Type::INT16:
        return static_cast<arrow::Int16Builder*>(out)->Append(static_cast<int16_t>(s->value));
      case arrow::Type::INT32:
        return static_cast<arrow::Int32Builder*>(out)->Append(static_cast<int32_t>(s->value));
      case arrow::Type::INT64:
        return static_cast<arrow::Int64Builder*>(out)->Append(static_cast<int64_t>(s->value));
      default:
        break;
    }
    return arrow::Status::NotImplemented("unsupported min/max output type");
  }

 private:
  bool is_min_ = true;
  arrow::Type::type output_type_id_;
};

class AggMinMaxUnsigned final : public detail::AggregateFunction {
 public:
  struct State {
    uint64_t value = 0;
    uint8_t has_value = 0;
  };

  AggMinMaxUnsigned(bool is_min, arrow::Type::type output_type_id)
      : is_min_(is_min), output_type_id_(output_type_id) {}

  int64_t state_size() const override { return static_cast<int64_t>(sizeof(State)); }
  int64_t state_alignment() const override { return static_cast<int64_t>(alignof(State)); }

  void Create(uint8_t* state) const override { new (state) State(); }
  void Destroy(uint8_t* state) const override { reinterpret_cast<State*>(state)->~State(); }

  arrow::Status Add(uint8_t* state, const arrow::Array* arg, int64_t row) const override {
    if (arg == nullptr) {
      return arrow::Status::Invalid("min/max expects non-null arg array");
    }
    if (arg->IsNull(row)) {
      return arrow::Status::OK();
    }

    uint64_t cand = 0;
    switch (arg->type_id()) {
      case arrow::Type::UINT8: {
        const auto& arr = static_cast<const arrow::UInt8Array&>(*arg);
        cand = static_cast<uint64_t>(arr.Value(row));
        break;
      }
      case arrow::Type::UINT16: {
        const auto& arr = static_cast<const arrow::UInt16Array&>(*arg);
        cand = static_cast<uint64_t>(arr.Value(row));
        break;
      }
      case arrow::Type::UINT32: {
        const auto& arr = static_cast<const arrow::UInt32Array&>(*arg);
        cand = static_cast<uint64_t>(arr.Value(row));
        break;
      }
      case arrow::Type::UINT64: {
        const auto& arr = static_cast<const arrow::UInt64Array&>(*arg);
        cand = arr.Value(row);
        break;
      }
      default:
        return arrow::Status::NotImplemented("unsupported min/max arg type: ",
                                             arg->type()->ToString());
    }

    auto* s = reinterpret_cast<State*>(state);
    if (s->has_value == 0) {
      s->has_value = 1;
      s->value = cand;
      return arrow::Status::OK();
    }
    if ((is_min_ && cand < s->value) || (!is_min_ && cand > s->value)) {
      s->value = cand;
    }
    return arrow::Status::OK();
  }

  arrow::Status Finalize(const uint8_t* state, arrow::ArrayBuilder* out) const override {
    if (out == nullptr || out->type()->id() != output_type_id_) {
      return arrow::Status::Invalid("min/max output builder type mismatch");
    }
    const auto* s = reinterpret_cast<const State*>(state);
    if (s->has_value == 0) {
      return out->AppendNull();
    }

    switch (output_type_id_) {
      case arrow::Type::UINT8:
        return static_cast<arrow::UInt8Builder*>(out)->Append(static_cast<uint8_t>(s->value));
      case arrow::Type::UINT16:
        return static_cast<arrow::UInt16Builder*>(out)->Append(static_cast<uint16_t>(s->value));
      case arrow::Type::UINT32:
        return static_cast<arrow::UInt32Builder*>(out)->Append(static_cast<uint32_t>(s->value));
      case arrow::Type::UINT64:
        return static_cast<arrow::UInt64Builder*>(out)->Append(static_cast<uint64_t>(s->value));
      default:
        break;
    }
    return arrow::Status::NotImplemented("unsupported min/max output type");
  }

 private:
  bool is_min_ = true;
  arrow::Type::type output_type_id_;
};

class AggMinMaxBinary final : public detail::AggregateFunction {
 public:
  struct State {
    explicit State(std::pmr::memory_resource* resource) : out(resource), norm(resource) {}
    bool has_value = false;
    std::pmr::string out;
    std::pmr::string norm;
  };

  AggMinMaxBinary(bool is_min, Collation collation, std::pmr::memory_resource* resource)
      : is_min_(is_min), collation_(collation), resource_(resource), scratch_norm_(resource) {}

  int64_t state_size() const override { return static_cast<int64_t>(sizeof(State)); }
  int64_t state_alignment() const override { return static_cast<int64_t>(alignof(State)); }

  void Create(uint8_t* state) const override { new (state) State(resource_); }
  void Destroy(uint8_t* state) const override { reinterpret_cast<State*>(state)->~State(); }

  arrow::Status Add(uint8_t* state, const arrow::Array* arg, int64_t row) const override {
    if (arg == nullptr) {
      return arrow::Status::Invalid("min/max expects non-null arg array");
    }
    if (arg->type_id() != arrow::Type::BINARY) {
      return arrow::Status::Invalid("min/max(binary) expects binary arg array");
    }
    if (arg->IsNull(row)) {
      return arrow::Status::OK();
    }

    const auto& arr = static_cast<const arrow::BinaryArray&>(*arg);
    const std::string_view view = arr.GetView(row);
    auto* s = reinterpret_cast<State*>(state);

    scratch_norm_.clear();
    SortKeyStringTo(collation_, view, &scratch_norm_);

    if (!s->has_value) {
      s->has_value = true;
      s->out.assign(view.data(), view.size());
      s->norm = scratch_norm_;
      return arrow::Status::OK();
    }

    const int cmp = scratch_norm_.compare(s->norm);
    const bool take = is_min_ ? (cmp < 0) : (cmp > 0);
    if (take) {
      s->out.assign(view.data(), view.size());
      s->norm = scratch_norm_;
    }
    return arrow::Status::OK();
  }

  arrow::Status Finalize(const uint8_t* state, arrow::ArrayBuilder* out) const override {
    if (out == nullptr || out->type()->id() != arrow::Type::BINARY) {
      return arrow::Status::Invalid("min/max(binary) expects binary output builder");
    }
    auto* builder = static_cast<arrow::BinaryBuilder*>(out);
    const auto* s = reinterpret_cast<const State*>(state);
    if (!s->has_value) {
      return builder->AppendNull();
    }
    return builder->Append(reinterpret_cast<const uint8_t*>(s->out.data()),
                           static_cast<int32_t>(s->out.size()));
  }

 private:
  bool is_min_ = true;
  Collation collation_;
  std::pmr::memory_resource* resource_ = nullptr;
  mutable std::pmr::string scratch_norm_;
};

}  // namespace

struct HashAggContext::Compiled {
  std::vector<CompiledExpr> key_exprs;
  std::vector<std::optional<CompiledExpr>> agg_arg_exprs;
};

HashAggContext::~HashAggContext() {
  // Aggregate states live in an arena (bulk-freed), but some states can own nested resources
  // (e.g. pmr strings). Destroy them explicitly.
  if (group_agg_states_.empty()) {
    return;
  }
  ARROW_DCHECK(group_agg_states_.size() == group_keys_.size());

  for (auto* row_state : group_agg_states_) {
    if (row_state == nullptr) {
      continue;
    }
    for (const auto& agg : aggs_) {
      if (agg.fn == nullptr) {
        continue;
      }
      agg.fn->Destroy(row_state + agg.state_offset);
    }
  }
}

HashAggContext::HashAggContext(const Engine* engine, std::vector<AggKey> keys,
                               std::vector<AggFunc> aggs, arrow::MemoryPool* memory_pool)
    : engine_(engine),
      keys_(std::move(keys)),
      memory_pool_(memory_pool != nullptr
                       ? memory_pool
                       : (engine != nullptr ? engine->memory_pool() : arrow::default_memory_pool())),
      group_key_arena_(memory_pool_),
      agg_state_arena_(memory_pool_),
      key_to_group_id_(memory_pool_, &group_key_arena_),
      pmr_resource_(std::make_unique<ArrowMemoryPoolResource>(memory_pool_)),
      exec_context_(memory_pool_, /*executor=*/nullptr,
                   engine != nullptr ? engine->function_registry() : nullptr),
      group_keys_(std::pmr::polymorphic_allocator<OutputKey>(pmr_resource_.get())),
      group_agg_states_(std::pmr::polymorphic_allocator<uint8_t*>(pmr_resource_.get())) {

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

arrow::Result<uint32_t> HashAggContext::InsertGroup(std::string_view normalized_key_bytes,
                                                    uint64_t hash, OutputKey output_key) {
  if (normalized_key_bytes.size() >
      static_cast<std::size_t>(std::numeric_limits<int32_t>::max())) {
    return arrow::Status::Invalid("normalized key too large");
  }
  if (group_keys_.size() >
      static_cast<std::size_t>(std::numeric_limits<uint32_t>::max())) {
    return arrow::Status::Invalid("too many groups");
  }

  const uint32_t candidate_group_id = static_cast<uint32_t>(group_keys_.size());
  const auto* key_data =
      reinterpret_cast<const uint8_t*>(normalized_key_bytes.data());
  const auto key_size = static_cast<int32_t>(normalized_key_bytes.size());

  ARROW_ASSIGN_OR_RAISE(
      auto res, key_to_group_id_.FindOrInsert(key_data, key_size, hash, candidate_group_id));
  const auto group_id = res.first;
  const bool inserted = res.second;
  if (!inserted) {
    return group_id;
  }
  if (group_id != candidate_group_id) {
    return arrow::Status::Invalid("unexpected group id assignment");
  }

  group_keys_.push_back(std::move(output_key));

  if (!agg_state_layout_ready_) {
    return arrow::Status::Invalid("internal error: aggregate state layout must be initialized");
  }
  ARROW_ASSIGN_OR_RAISE(
      auto* row_state, agg_state_arena_.Allocate(agg_state_row_size_, agg_state_row_alignment_));
  group_agg_states_.push_back(row_state);
  ARROW_DCHECK(group_agg_states_.size() == group_keys_.size());
  for (auto& agg : aggs_) {
    if (agg.fn == nullptr) {
      return arrow::Status::Invalid("internal error: aggregate function must not be null");
    }
    agg.fn->Create(row_state + agg.state_offset);
  }
  return candidate_group_id;
}

arrow::Status HashAggContext::ConsumeBatch(const arrow::RecordBatch& input) {
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

  const auto append_u32_le = [](std::string& out, uint32_t v) {
    out.push_back(static_cast<char>(v));
    out.push_back(static_cast<char>(v >> 8));
    out.push_back(static_cast<char>(v >> 16));
    out.push_back(static_cast<char>(v >> 24));
  };
  const auto append_u64_le = [](std::string& out, uint64_t v) {
    out.push_back(static_cast<char>(v));
    out.push_back(static_cast<char>(v >> 8));
    out.push_back(static_cast<char>(v >> 16));
    out.push_back(static_cast<char>(v >> 24));
    out.push_back(static_cast<char>(v >> 32));
    out.push_back(static_cast<char>(v >> 40));
    out.push_back(static_cast<char>(v >> 48));
    out.push_back(static_cast<char>(v >> 56));
  };

  if (!agg_state_layout_ready_) {
    if (pmr_resource_ == nullptr) {
      return arrow::Status::Invalid("internal error: pmr resource must not be null");
    }

    // Create aggregate function instances (once) after arg types/collations are known.
    for (std::size_t i = 0; i < aggs_.size(); ++i) {
      auto& agg = aggs_[i];
      if (agg.fn != nullptr) {
        continue;
      }
      const auto& arg_any = i < agg_args.size() ? agg_args[i] : nullptr;

      switch (agg.kind) {
        case AggState::Kind::kUnsupported:
          return arrow::Status::NotImplemented("unsupported aggregate func: ", agg.func);
        case AggState::Kind::kCountAll:
          agg.fn = std::make_unique<AggCountAll>();
          break;
        case AggState::Kind::kCount:
          agg.fn = std::make_unique<AggCount>();
          break;
        case AggState::Kind::kSum: {
          switch (agg.sum_kind) {
            case AggState::SumKind::kUnresolved:
              return arrow::Status::Invalid("internal error: sum kind must be resolved");
            case AggState::SumKind::kInt64:
              agg.fn = std::make_unique<AggSumInt64>();
              break;
            case AggState::SumKind::kUInt64:
              agg.fn = std::make_unique<AggSumUInt64>();
              break;
          }
          break;
        }
        case AggState::Kind::kMin:
        case AggState::Kind::kMax: {
          if (arg_any == nullptr) {
            return arrow::Status::Invalid("internal error: missing ", agg.func, " arg array");
          }
          const bool is_min = (agg.kind == AggState::Kind::kMin);
          switch (arg_any->type_id()) {
            case arrow::Type::INT8:
            case arrow::Type::INT16:
            case arrow::Type::INT32:
            case arrow::Type::INT64:
              agg.fn = std::make_unique<AggMinMaxSigned>(is_min, arg_any->type_id());
              break;
            case arrow::Type::UINT8:
            case arrow::Type::UINT16:
            case arrow::Type::UINT32:
            case arrow::Type::UINT64:
              agg.fn = std::make_unique<AggMinMaxUnsigned>(is_min, arg_any->type_id());
              break;
            case arrow::Type::BINARY:
              agg.fn = std::make_unique<AggMinMaxBinary>(is_min, agg_collations[i],
                                                         pmr_resource_.get());
              break;
            default:
              return arrow::Status::NotImplemented(agg.func, " arg type not supported: ",
                                                   arg_any->type()->ToString());
          }
          break;
        }
      }
    }

    std::vector<const detail::AggregateFunction*> fns;
    fns.reserve(aggs_.size());
    for (const auto& agg : aggs_) {
      if (agg.fn == nullptr) {
        return arrow::Status::Invalid("internal error: aggregate function must not be null");
      }
      fns.push_back(agg.fn.get());
    }
    ARROW_ASSIGN_OR_RAISE(const auto layout, detail::ComputeAggStateLayout(fns));
    agg_state_row_size_ = layout.row_size;
    agg_state_row_alignment_ = layout.row_alignment;
    for (std::size_t i = 0; i < aggs_.size(); ++i) {
      aggs_[i].state_offset = layout.offsets[i];
    }
    agg_state_layout_ready_ = true;
  }

  if (key_count == 0 && group_keys_.empty()) {
    OutputKey out_key;
    out_key.key_count = 0;
    group_keys_.push_back(std::move(out_key));
    ARROW_DCHECK(group_agg_states_.empty());
    ARROW_DCHECK(agg_state_layout_ready_);
    ARROW_ASSIGN_OR_RAISE(auto* row_state,
                          agg_state_arena_.Allocate(agg_state_row_size_, agg_state_row_alignment_));
    group_agg_states_.push_back(row_state);
    for (auto& agg : aggs_) {
      if (agg.fn == nullptr) {
        return arrow::Status::Invalid("internal error: aggregate function must not be null");
      }
      agg.fn->Create(row_state + agg.state_offset);
    }
  }

  std::string normalized_key;
  normalized_key.reserve(key_count * 16);
  std::string sort_key;

  const int64_t rows = input.num_rows();
  for (int64_t row = 0; row < rows; ++row) {
    uint32_t group_id = 0;
    if (key_count != 0) {
      normalized_key.clear();

      for (std::size_t ki = 0; ki < key_count; ++ki) {
        const auto& arr_any = key_arrays[ki];
        if (arr_any == nullptr) {
          return arrow::Status::Invalid("internal error: missing key array");
        }

        if (arr_any->IsNull(row)) {
          normalized_key.push_back(static_cast<char>(0));
          continue;
        }
        normalized_key.push_back(static_cast<char>(1));

        switch (arr_any->type_id()) {
          case arrow::Type::BOOL: {
            const auto& arr = static_cast<const arrow::BooleanArray&>(*arr_any);
            const uint64_t v = static_cast<uint64_t>(arr.Value(row) ? 1 : 0);
            append_u64_le(normalized_key, v);
            break;
          }
          case arrow::Type::INT8: {
            const auto& arr = static_cast<const arrow::Int8Array&>(*arr_any);
            const int64_t v = static_cast<int64_t>(arr.Value(row));
            append_u64_le(normalized_key, static_cast<uint64_t>(v));
            break;
          }
          case arrow::Type::INT16: {
            const auto& arr = static_cast<const arrow::Int16Array&>(*arr_any);
            const int64_t v = static_cast<int64_t>(arr.Value(row));
            append_u64_le(normalized_key, static_cast<uint64_t>(v));
            break;
          }
          case arrow::Type::INT32: {
            const auto& arr = static_cast<const arrow::Int32Array&>(*arr_any);
            const int64_t v = static_cast<int64_t>(arr.Value(row));
            append_u64_le(normalized_key, static_cast<uint64_t>(v));
            break;
          }
          case arrow::Type::INT64: {
            const auto& arr = static_cast<const arrow::Int64Array&>(*arr_any);
            const int64_t v = arr.Value(row);
            append_u64_le(normalized_key, static_cast<uint64_t>(v));
            break;
          }
          case arrow::Type::UINT8: {
            const auto& arr = static_cast<const arrow::UInt8Array&>(*arr_any);
            const uint64_t v = static_cast<uint64_t>(arr.Value(row));
            append_u64_le(normalized_key, v);
            break;
          }
          case arrow::Type::UINT16: {
            const auto& arr = static_cast<const arrow::UInt16Array&>(*arr_any);
            const uint64_t v = static_cast<uint64_t>(arr.Value(row));
            append_u64_le(normalized_key, v);
            break;
          }
          case arrow::Type::UINT32: {
            const auto& arr = static_cast<const arrow::UInt32Array&>(*arr_any);
            const uint64_t v = static_cast<uint64_t>(arr.Value(row));
            append_u64_le(normalized_key, v);
            break;
          }
          case arrow::Type::UINT64: {
            const auto& arr = static_cast<const arrow::UInt64Array&>(*arr_any);
            const uint64_t v = arr.Value(row);
            append_u64_le(normalized_key, v);
            break;
          }
          case arrow::Type::FLOAT: {
            const auto& arr = static_cast<const arrow::FloatArray&>(*arr_any);
            const float v = arr.Value(row);
            append_u64_le(normalized_key, CanonicalizeFloat32Bits(v));
            break;
          }
          case arrow::Type::DOUBLE: {
            const auto& arr = static_cast<const arrow::DoubleArray&>(*arr_any);
            const double v = arr.Value(row);
            append_u64_le(normalized_key, CanonicalizeFloat64Bits(v));
            break;
          }
          case arrow::Type::DECIMAL128: {
            const auto& arr = static_cast<const arrow::FixedSizeBinaryArray&>(*arr_any);
            if (arr.byte_width() != static_cast<int>(Decimal128Bytes{}.size())) {
              return arrow::Status::Invalid("unexpected decimal128 byte width");
            }
            normalized_key.append(reinterpret_cast<const char*>(arr.GetValue(row)),
                                  Decimal128Bytes{}.size());
            break;
          }
          case arrow::Type::DECIMAL256: {
            const auto& arr = static_cast<const arrow::FixedSizeBinaryArray&>(*arr_any);
            if (arr.byte_width() != static_cast<int>(Decimal256Bytes{}.size())) {
              return arrow::Status::Invalid("unexpected decimal256 byte width");
            }
            normalized_key.append(reinterpret_cast<const char*>(arr.GetValue(row)),
                                  Decimal256Bytes{}.size());
            break;
          }
          case arrow::Type::BINARY: {
            const auto& arr = static_cast<const arrow::BinaryArray&>(*arr_any);
            std::string_view view = arr.GetView(row);
            sort_key.clear();
            SortKeyStringTo(collations[ki], view, &sort_key);
            if (sort_key.size() >
                static_cast<std::size_t>(std::numeric_limits<uint32_t>::max())) {
              return arrow::Status::Invalid("normalized string key too large");
            }
            append_u32_le(normalized_key, static_cast<uint32_t>(sort_key.size()));
            normalized_key.append(sort_key.data(), sort_key.size());
            break;
          }
          default:
            return arrow::Status::NotImplemented("unsupported group key type: ",
                                                 arr_any->type()->ToString());
        }
      }

      if (normalized_key.size() >
          static_cast<std::size_t>(std::numeric_limits<int32_t>::max())) {
        return arrow::Status::Invalid("normalized key too large");
      }
      const auto* key_data =
          reinterpret_cast<const uint8_t*>(normalized_key.data());
      const auto key_size = static_cast<int32_t>(normalized_key.size());
      const uint64_t key_hash = detail::HashBytes(key_data, key_size);

      ARROW_ASSIGN_OR_RAISE(auto found, key_to_group_id_.Find(key_data, key_size, key_hash));
      if (found.has_value()) {
        group_id = *found;
      } else {
        if (pmr_resource_ == nullptr) {
          return arrow::Status::Invalid("internal error: pmr resource must not be null");
        }

        OutputKey out_key;
        out_key.key_count = static_cast<uint8_t>(key_count);
        for (std::size_t ki = 0; ki < key_count; ++ki) {
          auto& out_part = out_key.parts[ki];
          const auto& arr_any = key_arrays[ki];
          if (arr_any == nullptr) {
            return arrow::Status::Invalid("internal error: missing key array");
          }
          if (arr_any->IsNull(row)) {
            out_part.is_null = true;
            continue;
          }
          out_part.is_null = false;

          switch (arr_any->type_id()) {
            case arrow::Type::BOOL: {
              const auto& arr = static_cast<const arrow::BooleanArray&>(*arr_any);
              const uint64_t v = static_cast<uint64_t>(arr.Value(row) ? 1 : 0);
              out_part.value = static_cast<uint64_t>(v);
              break;
            }
            case arrow::Type::INT8: {
              const auto& arr = static_cast<const arrow::Int8Array&>(*arr_any);
              out_part.value = static_cast<int64_t>(arr.Value(row));
              break;
            }
            case arrow::Type::INT16: {
              const auto& arr = static_cast<const arrow::Int16Array&>(*arr_any);
              out_part.value = static_cast<int64_t>(arr.Value(row));
              break;
            }
            case arrow::Type::INT32: {
              const auto& arr = static_cast<const arrow::Int32Array&>(*arr_any);
              out_part.value = static_cast<int64_t>(arr.Value(row));
              break;
            }
            case arrow::Type::INT64: {
              const auto& arr = static_cast<const arrow::Int64Array&>(*arr_any);
              out_part.value = static_cast<int64_t>(arr.Value(row));
              break;
            }
            case arrow::Type::UINT8: {
              const auto& arr = static_cast<const arrow::UInt8Array&>(*arr_any);
              out_part.value = static_cast<uint64_t>(arr.Value(row));
              break;
            }
            case arrow::Type::UINT16: {
              const auto& arr = static_cast<const arrow::UInt16Array&>(*arr_any);
              out_part.value = static_cast<uint64_t>(arr.Value(row));
              break;
            }
            case arrow::Type::UINT32: {
              const auto& arr = static_cast<const arrow::UInt32Array&>(*arr_any);
              out_part.value = static_cast<uint64_t>(arr.Value(row));
              break;
            }
            case arrow::Type::UINT64: {
              const auto& arr = static_cast<const arrow::UInt64Array&>(*arr_any);
              out_part.value = static_cast<uint64_t>(arr.Value(row));
              break;
            }
            case arrow::Type::FLOAT: {
              const auto& arr = static_cast<const arrow::FloatArray&>(*arr_any);
              out_part.value = static_cast<uint64_t>(Float32ToBits(arr.Value(row)));
              break;
            }
            case arrow::Type::DOUBLE: {
              const auto& arr = static_cast<const arrow::DoubleArray&>(*arr_any);
              out_part.value = static_cast<uint64_t>(Float64ToBits(arr.Value(row)));
              break;
            }
            case arrow::Type::DECIMAL128: {
              const auto& arr = static_cast<const arrow::FixedSizeBinaryArray&>(*arr_any);
              if (arr.byte_width() != static_cast<int>(Decimal128Bytes{}.size())) {
                return arrow::Status::Invalid("unexpected decimal128 byte width");
              }
              Decimal128Bytes bytes{};
              std::memcpy(bytes.data(), arr.GetValue(row), bytes.size());
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
              out_part.value = bytes;
              break;
            }
            case arrow::Type::BINARY: {
              const auto& arr = static_cast<const arrow::BinaryArray&>(*arr_any);
              std::string_view view = arr.GetView(row);
              std::pmr::string out_value(pmr_resource_.get());
              out_value.assign(view.data(), view.size());
              out_part.value = std::move(out_value);
              break;
            }
            default:
              return arrow::Status::NotImplemented("unsupported group key type: ",
                                                   arr_any->type()->ToString());
          }
        }

        ARROW_ASSIGN_OR_RAISE(group_id, InsertGroup(normalized_key, key_hash, std::move(out_key)));
      }
    }

    if (group_id >= group_agg_states_.size()) {
      return arrow::Status::Invalid("internal error: agg state group id out of range");
    }
    auto* row_state = group_agg_states_[group_id];
    if (row_state == nullptr) {
      return arrow::Status::Invalid("internal error: missing aggregate state row");
    }
    for (std::size_t i = 0; i < aggs_.size(); ++i) {
      auto& agg = aggs_[i];
      if (agg.fn == nullptr) {
        return arrow::Status::Invalid("internal error: aggregate function must not be null");
      }
      const auto* arg_any = i < agg_args.size() ? agg_args[i].get() : nullptr;
      ARROW_RETURN_NOT_OK(agg.fn->Add(row_state + agg.state_offset, arg_any, row));
    }
  }

  return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::Schema>> HashAggContext::BuildOutputSchema() const {
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

arrow::Result<std::shared_ptr<arrow::RecordBatch>> HashAggContext::FinalizeOutput() {
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

  if (group_agg_states_.size() != group_count) {
    return arrow::Status::Invalid("aggregate state group count mismatch");
  }

  for (std::size_t ai = 0; ai < aggs_.size(); ++ai) {
    const auto& agg = aggs_[ai];
    const auto& field = output_agg_fields_[ai];
    if (field == nullptr) {
      return arrow::Status::Invalid("agg output field must not be null");
    }
    if (agg.fn == nullptr) {
      return arrow::Status::Invalid("internal error: aggregate function must not be null");
    }

    std::unique_ptr<arrow::ArrayBuilder> builder;
    ARROW_RETURN_NOT_OK(arrow::MakeBuilder(memory_pool_, field->type(), &builder));
    ARROW_RETURN_NOT_OK(builder->Reserve(static_cast<int64_t>(group_count)));

    for (std::size_t gi = 0; gi < group_count; ++gi) {
      auto* row_state = group_agg_states_[gi];
      if (row_state == nullptr) {
        return arrow::Status::Invalid("internal error: missing aggregate state row");
      }
      ARROW_RETURN_NOT_OK(agg.fn->Finalize(row_state + agg.state_offset, builder.get()));
    }

    std::shared_ptr<arrow::Array> out;
    ARROW_RETURN_NOT_OK(builder->Finish(&out));
    columns.push_back(std::move(out));
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

arrow::Status HashAggContext::FinishBuild() {
  if (build_finished_) {
    return arrow::Status::OK();
  }
  build_finished_ = true;
  next_output_row_ = 0;
  if (output_all_ == nullptr) {
    ARROW_ASSIGN_OR_RAISE(output_all_, FinalizeOutput());
  }
  return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> HashAggContext::ReadNextOutputBatch(int64_t max_rows) {
  if (!build_finished_) {
    return arrow::Status::Invalid("hash agg build is not finished");
  }
  if (max_rows <= 0) {
    return arrow::Status::Invalid("max_rows must be positive");
  }
  if (output_all_ == nullptr) {
    ARROW_ASSIGN_OR_RAISE(output_all_, FinalizeOutput());
  }
  if (output_all_ == nullptr) {
    return arrow::Status::Invalid("hash agg output batch must not be null");
  }

  const int64_t total_rows = output_all_->num_rows();
  if (total_rows == 0) {
    if (next_output_row_ == 0) {
      next_output_row_ = 1;
      return output_all_;
    }
    return std::shared_ptr<arrow::RecordBatch>();
  }

  if (next_output_row_ >= total_rows) {
    return std::shared_ptr<arrow::RecordBatch>();
  }

  const int64_t remaining = total_rows - next_output_row_;
  const int64_t length = remaining < max_rows ? remaining : max_rows;
  auto out = output_all_->Slice(next_output_row_, length);
  next_output_row_ += length;
  return out;
}

HashAggBuildSinkOp::HashAggBuildSinkOp(std::shared_ptr<HashAggContext> context)
    : context_(std::move(context)) {}

arrow::Result<OperatorStatus> HashAggBuildSinkOp::WriteImpl(std::shared_ptr<arrow::RecordBatch> batch) {
  if (context_ == nullptr) {
    return arrow::Status::Invalid("hash agg context must not be null");
  }
  if (batch == nullptr) {
    ARROW_RETURN_NOT_OK(context_->FinishBuild());
    return OperatorStatus::kFinished;
  }
  ARROW_RETURN_NOT_OK(context_->ConsumeBatch(*batch));
  return OperatorStatus::kNeedInput;
}

HashAggConvergentSourceOp::HashAggConvergentSourceOp(std::shared_ptr<HashAggContext> context,
                                                     int64_t max_output_rows)
    : context_(std::move(context)), max_output_rows_(max_output_rows) {}

arrow::Result<OperatorStatus> HashAggConvergentSourceOp::ReadImpl(
    std::shared_ptr<arrow::RecordBatch>* batch) {
  if (batch == nullptr) {
    return arrow::Status::Invalid("batch output must not be null");
  }
  if (context_ == nullptr) {
    return arrow::Status::Invalid("hash agg context must not be null");
  }

  ARROW_ASSIGN_OR_RAISE(auto out, context_->ReadNextOutputBatch(max_output_rows_));
  if (out == nullptr) {
    batch->reset();
    return OperatorStatus::kFinished;
  }
  *batch = std::move(out);
  return OperatorStatus::kHasOutput;
}

HashAggTransformOp::~HashAggTransformOp() = default;

HashAggTransformOp::HashAggTransformOp(const Engine* engine, std::vector<AggKey> keys,
                                       std::vector<AggFunc> aggs, arrow::MemoryPool* memory_pool)
    : context_(std::make_shared<HashAggContext>(engine, std::move(keys), std::move(aggs), memory_pool)) {}

arrow::Result<OperatorStatus> HashAggTransformOp::TransformImpl(
    std::shared_ptr<arrow::RecordBatch>* batch) {
  if (context_ == nullptr) {
    return arrow::Status::Invalid("hash agg context must not be null");
  }
  if (*batch == nullptr) {
    if (!finalized_) {
      ARROW_RETURN_NOT_OK(context_->FinishBuild());
      ARROW_ASSIGN_OR_RAISE(*batch, context_->ReadNextOutputBatch(std::numeric_limits<int64_t>::max()));
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
  ARROW_RETURN_NOT_OK(context_->ConsumeBatch(input));
  batch->reset();
  return OperatorStatus::kNeedInput;
}

}  // namespace tiforth
