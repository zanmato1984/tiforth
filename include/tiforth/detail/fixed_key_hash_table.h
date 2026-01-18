#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>

#include <arrow/buffer.h>
#include <arrow/memory_pool.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/util/logging.h>

namespace tiforth::detail {

// Open-addressing hash table mapping a fixed-size trivially-copyable key to a value.
//
// This is a small building block intended for TiFlash-shaped aggregation methods (one-number /
// fixed-keys). Hashing is supplied by the caller (saved hash per entry).
//
// Empty slot sentinel: occupied == 0.
template <typename Key, typename Value>
class FixedKeyHashTable {
 public:
  FixedKeyHashTable(arrow::MemoryPool* pool)
      : pool_(pool != nullptr ? pool : arrow::default_memory_pool()) {}

  FixedKeyHashTable(const FixedKeyHashTable&) = delete;
  FixedKeyHashTable& operator=(const FixedKeyHashTable&) = delete;
  FixedKeyHashTable(FixedKeyHashTable&&) = default;
  FixedKeyHashTable& operator=(FixedKeyHashTable&&) = default;

  int64_t size() const { return size_; }

  arrow::Result<std::optional<Value>> Find(const Key& key, uint64_t hash) const {
    if (capacity_ == 0) {
      return std::optional<Value>{};
    }
    const int64_t mask = capacity_ - 1;
    int64_t idx = static_cast<int64_t>(hash) & mask;
    while (true) {
      const auto& entry = Entries()[idx];
      if (entry.occupied == 0) {
        return std::optional<Value>{};
      }
      if (entry.hash == hash && entry.key == key) {
        return std::optional<Value>{entry.value};
      }
      idx = (idx + 1) & mask;
    }
  }

  // Find existing key; if not found, insert the given {key, value}.
  // Returns {value, inserted}.
  arrow::Result<std::pair<Value, bool>> FindOrInsert(const Key& key, uint64_t hash, Value value) {
    if (capacity_ == 0) {
      ARROW_RETURN_NOT_OK(Init(/*capacity=*/16));
    }

    // Retry loop for rehash-on-insert.
    while (true) {
      const int64_t mask = capacity_ - 1;
      int64_t idx = static_cast<int64_t>(hash) & mask;
      while (true) {
        auto& entry = Entries()[idx];
        if (entry.occupied == 0) {
          break;
        }
        if (entry.hash == hash && entry.key == key) {
          return std::pair<Value, bool>{entry.value, false};
        }
        idx = (idx + 1) & mask;
      }

      if (size_ + 1 > max_load_) {
        ARROW_RETURN_NOT_OK(Rehash(capacity_ * 2));
        continue;
      }

      // Re-probe to find the (possibly different) empty slot after growth.
      const int64_t new_mask = capacity_ - 1;
      int64_t ins_idx = static_cast<int64_t>(hash) & new_mask;
      while (true) {
        auto& entry = Entries()[ins_idx];
        if (entry.occupied == 0) {
          break;
        }
        if (entry.hash == hash && entry.key == key) {
          return std::pair<Value, bool>{entry.value, false};
        }
        ins_idx = (ins_idx + 1) & new_mask;
      }

      auto& dst = Entries()[ins_idx];
      dst.key = key;
      dst.value = value;
      dst.hash = hash;
      dst.occupied = 1;
      ++size_;
      return std::pair<Value, bool>{value, true};
    }
  }

 private:
  struct Entry {
    Key key{};
    Value value{};
    uint64_t hash = 0;
    uint8_t occupied = 0;
  };
  static_assert(std::is_trivially_copyable_v<Entry>);

  Entry* Entries() { return reinterpret_cast<Entry*>(buffer_->mutable_data()); }
  const Entry* Entries() const { return reinterpret_cast<const Entry*>(buffer_->data()); }

  static int64_t RoundUpToPowerOfTwo(int64_t v) {
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

  arrow::Status Init(int64_t capacity) {
    capacity = std::max<int64_t>(capacity, 16);
    capacity = RoundUpToPowerOfTwo(capacity);
    if (capacity <= 0) {
      return arrow::Status::Invalid("invalid hash table capacity");
    }
    const int64_t bytes = capacity * static_cast<int64_t>(sizeof(Entry));
    if (bytes / static_cast<int64_t>(sizeof(Entry)) != capacity) {
      return arrow::Status::Invalid("hash table size overflow");
    }
    ARROW_ASSIGN_OR_RAISE(buffer_, arrow::AllocateBuffer(bytes, pool_));
    std::memset(buffer_->mutable_data(), 0, static_cast<std::size_t>(bytes));
    capacity_ = capacity;
    size_ = 0;
    max_load_ = static_cast<int64_t>(static_cast<double>(capacity_) * 0.7);
    if (max_load_ < 1) {
      max_load_ = 1;
    }
    return arrow::Status::OK();
  }

  arrow::Status Rehash(int64_t new_capacity) {
    if (new_capacity <= capacity_) {
      return arrow::Status::OK();
    }

    FixedKeyHashTable next(pool_);
    ARROW_RETURN_NOT_OK(next.Init(new_capacity));

    for (int64_t i = 0; i < capacity_; ++i) {
      const auto& e = Entries()[i];
      if (e.occupied == 0) {
        continue;
      }
      const int64_t mask = next.capacity_ - 1;
      int64_t idx = static_cast<int64_t>(e.hash) & mask;
      while (true) {
        auto& dst = next.Entries()[idx];
        if (dst.occupied == 0) {
          dst = e;
          ++next.size_;
          break;
        }
        idx = (idx + 1) & mask;
      }
    }

    *this = std::move(next);
    return arrow::Status::OK();
  }

  arrow::MemoryPool* pool_ = nullptr;
  std::unique_ptr<arrow::Buffer> buffer_;
  int64_t capacity_ = 0;
  int64_t size_ = 0;
  int64_t max_load_ = 0;
};

}  // namespace tiforth::detail

