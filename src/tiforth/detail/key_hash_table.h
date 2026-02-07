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
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <string_view>
#include <utility>

#include <arrow/buffer.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/memory_pool.h>

#include "tiforth/detail/arena.h"

#ifndef XXH_INLINE_ALL
#define XXH_INLINE_ALL
#define TIFORTH_DETAIL_XXH_INLINE_ALL 1
#endif
#include <arrow/vendored/xxhash/xxhash.h>
#ifdef TIFORTH_DETAIL_XXH_INLINE_ALL
#undef TIFORTH_DETAIL_XXH_INLINE_ALL
#undef XXH_INLINE_ALL
#endif

namespace tiforth::detail {

struct ByteSlice {
  const uint8_t* data = nullptr;
  int32_t size = 0;
};

inline bool ByteSliceEquals(ByteSlice lhs, const uint8_t* rhs_data, int32_t rhs_size) noexcept {
  if (lhs.size != rhs_size) {
    return false;
  }
  if (lhs.size == 0) {
    return true;
  }
  return std::memcmp(lhs.data, rhs_data, static_cast<std::size_t>(rhs_size)) == 0;
}

inline uint64_t HashBytes(const uint8_t* data, int32_t size) noexcept {
  if (size <= 0) {
    return 1;
  }
  return static_cast<uint64_t>(XXH3_64bits(data, static_cast<std::size_t>(size)));
}

// Open-addressing hash table mapping an arena-owned normalized key byte slice to a 32-bit value.
// Empty slot sentinel: key.data == nullptr.
class KeyHashTable {
 public:
  KeyHashTable(arrow::MemoryPool* pool, Arena* arena)
      : pool_(pool != nullptr ? pool : arrow::default_memory_pool()), arena_(arena) {}

  KeyHashTable(const KeyHashTable&) = delete;
  KeyHashTable& operator=(const KeyHashTable&) = delete;
  KeyHashTable(KeyHashTable&&) = default;
  KeyHashTable& operator=(KeyHashTable&&) = default;

  int64_t size() const { return size_; }

  arrow::Result<std::optional<uint32_t>> Find(const uint8_t* key_data, int32_t key_size,
                                              uint64_t hash) const {
    if (key_size < 0) {
      return arrow::Status::Invalid("key size must be non-negative");
    }
    if (key_size > 0 && key_data == nullptr) {
      return arrow::Status::Invalid("key data must not be null for non-zero size");
    }
    if (capacity_ == 0) {
      return std::optional<uint32_t>{};
    }

    const int64_t mask = capacity_ - 1;
    int64_t idx = static_cast<int64_t>(hash) & mask;
    while (true) {
      const auto& entry = Entries()[idx];
      if (entry.key.data == nullptr) {
        return std::optional<uint32_t>{};
      }
      if (entry.hash == hash && ByteSliceEquals(entry.key, key_data, key_size)) {
        return std::optional<uint32_t>{entry.value};
      }
      idx = (idx + 1) & mask;
    }
  }

  // Find existing key; if not found, insert a copy into the arena with the given value.
  // Returns {value, inserted}.
  arrow::Result<std::pair<uint32_t, bool>> FindOrInsert(const uint8_t* key_data, int32_t key_size,
                                                        uint64_t hash, uint32_t value) {
    if (key_size < 0) {
      return arrow::Status::Invalid("key size must be non-negative");
    }
    if (key_size > 0 && key_data == nullptr) {
      return arrow::Status::Invalid("key data must not be null for non-zero size");
    }
    if (arena_ == nullptr) {
      return arrow::Status::Invalid("arena must not be null");
    }
    if (capacity_ == 0) {
      ARROW_RETURN_NOT_OK(Init(/*capacity=*/16));
    }

    // Retry loop for rehash-on-insert.
    while (true) {
      const int64_t mask = capacity_ - 1;
      int64_t idx = static_cast<int64_t>(hash) & mask;
      while (true) {
        auto& entry = Entries()[idx];
        if (entry.key.data == nullptr) {
          break;
        }
        if (entry.hash == hash && ByteSliceEquals(entry.key, key_data, key_size)) {
          return std::pair<uint32_t, bool>{entry.value, false};
        }
        idx = (idx + 1) & mask;
      }

      // Need to insert: grow first if needed.
      if (size_ + 1 > max_load_) {
        ARROW_RETURN_NOT_OK(Rehash(capacity_ * 2));
        continue;
      }

      // Re-probe to find the (possibly different) empty slot after growth.
      const int64_t new_mask = capacity_ - 1;
      int64_t ins_idx = static_cast<int64_t>(hash) & new_mask;
      while (true) {
        auto& entry = Entries()[ins_idx];
        if (entry.key.data == nullptr) {
          break;
        }
        // Key might have been inserted by a previous probe path (shouldn't happen in single-threaded use),
        // but keep this check for safety.
        if (entry.hash == hash && ByteSliceEquals(entry.key, key_data, key_size)) {
          return std::pair<uint32_t, bool>{entry.value, false};
        }
        ins_idx = (ins_idx + 1) & new_mask;
      }

      ARROW_ASSIGN_OR_RAISE(const auto* stored, arena_->Append(key_data, key_size));
      auto& dst = Entries()[ins_idx];
      dst.key.data = stored;
      dst.key.size = key_size;
      dst.hash = hash;
      dst.value = value;
      ++size_;
      return std::pair<uint32_t, bool>{value, true};
    }
  }

  arrow::Result<std::pair<uint32_t, bool>> FindOrInsert(std::string_view key_bytes,
                                                        uint32_t value) {
    const auto* data = reinterpret_cast<const uint8_t*>(key_bytes.data());
    const auto size = static_cast<int32_t>(key_bytes.size());
    const uint64_t h = HashBytes(data, size);
    return FindOrInsert(data, size, h, value);
  }

 private:
  struct Entry {
    ByteSlice key;
    uint32_t value = 0;
    uint64_t hash = 0;
  };
  static_assert(std::is_trivially_copyable_v<Entry>);

  Entry* Entries() {
    return reinterpret_cast<Entry*>(buffer_->mutable_data());
  }
  const Entry* Entries() const {
    return reinterpret_cast<const Entry*>(buffer_->data());
  }

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

    KeyHashTable next(pool_, arena_);
    ARROW_RETURN_NOT_OK(next.Init(new_capacity));

    for (int64_t i = 0; i < capacity_; ++i) {
      const auto& e = Entries()[i];
      if (e.key.data == nullptr) {
        continue;
      }
      // Insert into next (key bytes are already arena-owned, so no copying needed).
      const int64_t mask = next.capacity_ - 1;
      int64_t idx = static_cast<int64_t>(e.hash) & mask;
      while (true) {
        auto& dst = next.Entries()[idx];
        if (dst.key.data == nullptr) {
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
  Arena* arena_ = nullptr;
  std::unique_ptr<arrow::Buffer> buffer_;
  int64_t capacity_ = 0;
  int64_t size_ = 0;
  int64_t max_load_ = 0;
};

}  // namespace tiforth::detail
