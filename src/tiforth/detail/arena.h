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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <utility>
#include <vector>

#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/memory_pool.h>

#include <arrow/util/logging.h>

namespace tiforth::detail {

// Simple arena that allocates big chunks from an Arrow MemoryPool and returns stable pointers.
// This is the TiForth counterpart of TiFlash/ClickHouse Arena, but uses Arrow Status/Result.
class Arena {
 public:
  explicit Arena(arrow::MemoryPool* pool, int64_t initial_chunk_bytes = 4096)
      : pool_(pool != nullptr ? pool : arrow::default_memory_pool()),
        next_chunk_bytes_(std::max<int64_t>(initial_chunk_bytes, 1024)) {}

  Arena(const Arena&) = delete;
  Arena& operator=(const Arena&) = delete;

  ~Arena() {
    for (const auto& chunk : chunks_) {
      if (chunk.data != nullptr && chunk.capacity > 0) {
        pool_->Free(chunk.data, chunk.capacity);
      }
    }
  }

  arrow::Result<uint8_t*> Allocate(int64_t size, int64_t alignment = alignof(std::max_align_t)) {
    if (size < 0) {
      return arrow::Status::Invalid("arena allocate size must be non-negative");
    }
    if (size == 0) {
      return const_cast<uint8_t*>(&kEmptyByte);
    }
    if (alignment <= 0) {
      return arrow::Status::Invalid("arena allocate alignment must be positive");
    }

    // We only expect power-of-two alignments here.
    ARROW_DCHECK((alignment & (alignment - 1)) == 0);

    if (chunks_.empty()) {
      ARROW_RETURN_NOT_OK(AddChunk(/*min_bytes=*/size + alignment));
    }

    while (true) {
      auto& chunk = chunks_.back();
      ARROW_DCHECK(chunk.data != nullptr);
      const int64_t aligned_offset = AlignUp(chunk.offset, alignment);
      if (aligned_offset <= chunk.capacity && size <= chunk.capacity - aligned_offset) {
        uint8_t* out = chunk.data + aligned_offset;
        chunk.offset = aligned_offset + size;
        return out;
      }
      ARROW_RETURN_NOT_OK(AddChunk(/*min_bytes=*/size + alignment));
    }
  }

  arrow::Result<const uint8_t*> Append(const uint8_t* data, int64_t size) {
    if (size < 0) {
      return arrow::Status::Invalid("arena append size must be non-negative");
    }
    if (size == 0) {
      return &kEmptyByte;
    }
    if (data == nullptr) {
      return arrow::Status::Invalid("arena append data must not be null for non-zero size");
    }
    ARROW_ASSIGN_OR_RAISE(auto* out, Allocate(size, /*alignment=*/1));
    std::memcpy(out, data, static_cast<std::size_t>(size));
    return out;
  }

 private:
  struct Chunk {
    uint8_t* data = nullptr;
    int64_t capacity = 0;
    int64_t offset = 0;
  };

  static int64_t AlignUp(int64_t value, int64_t alignment) {
    const int64_t mask = alignment - 1;
    if ((value & mask) == 0) {
      return value;
    }
    // Guard overflow.
    if (value > std::numeric_limits<int64_t>::max() - mask) {
      return std::numeric_limits<int64_t>::max();
    }
    return (value + mask) & ~mask;
  }

  static int64_t RoundUpToPage(int64_t value) {
    constexpr int64_t kPage = 4096;
    return AlignUp(value, kPage);
  }

  arrow::Status AddChunk(int64_t min_bytes) {
    if (min_bytes <= 0) {
      return arrow::Status::Invalid("arena chunk size must be positive");
    }

    int64_t alloc_bytes = std::max(next_chunk_bytes_, min_bytes);
    alloc_bytes = RoundUpToPage(alloc_bytes);
    if (alloc_bytes <= 0) {
      return arrow::Status::Invalid("arena chunk size overflow");
    }

    uint8_t* out = nullptr;
    ARROW_RETURN_NOT_OK(pool_->Allocate(alloc_bytes, &out));
    chunks_.push_back(Chunk{.data = out, .capacity = alloc_bytes, .offset = 0});

    // Exponential growth until it gets large enough (good enough for now).
    if (next_chunk_bytes_ < (1LL << 30)) {
      next_chunk_bytes_ = std::min<int64_t>(next_chunk_bytes_ * 2, 1LL << 30);
    }
    return arrow::Status::OK();
  }

  static inline const uint8_t kEmptyByte = 0;

  arrow::MemoryPool* pool_ = nullptr;
  std::vector<Chunk> chunks_;
  int64_t next_chunk_bytes_ = 0;
};

}  // namespace tiforth::detail
