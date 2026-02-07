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
#include <cstdint>
#include <limits>
#include <vector>

#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/util/logging.h>

namespace arrow {
class Array;
class ArrayBuilder;
}  // namespace arrow

namespace tiforth::detail {

// Minimal aggregate function interface in the TiFlash/ClickHouse shape:
// each function owns a fixed-size state blob per group and exposes lifecycle hooks.
//
// Notes:
// - Implementations are expected to be used by a single aggregation operator instance.
// - State memory is owned by an arena; Destroy() must release any nested resources (e.g. pmr strings).
class AggregateFunction {
 public:
  virtual ~AggregateFunction() = default;

  virtual int64_t state_size() const = 0;
  virtual int64_t state_alignment() const = 0;

  virtual void Create(uint8_t* state) const = 0;
  virtual void Destroy(uint8_t* state) const = 0;

  // arg may be null for no-arg aggregates (e.g. count_all).
  virtual arrow::Status Add(uint8_t* state, const arrow::Array* arg, int64_t row) const = 0;
  virtual arrow::Status Finalize(const uint8_t* state, arrow::ArrayBuilder* out) const = 0;
};

struct AggStateLayout {
  int64_t row_size = 0;
  int64_t row_alignment = 1;
  std::vector<int64_t> offsets;  // per-aggregate offset in the state row
};

inline bool IsPowerOfTwo(int64_t v) noexcept { return v > 0 && ((v & (v - 1)) == 0); }

inline int64_t AlignUp(int64_t value, int64_t alignment) noexcept {
  ARROW_DCHECK(IsPowerOfTwo(alignment));
  const int64_t mask = alignment - 1;
  if ((value & mask) == 0) {
    return value;
  }
  if (value > std::numeric_limits<int64_t>::max() - mask) {
    return std::numeric_limits<int64_t>::max();
  }
  return (value + mask) & ~mask;
}

inline arrow::Result<AggStateLayout> ComputeAggStateLayout(
    const std::vector<const AggregateFunction*>& fns) {
  AggStateLayout out;
  out.offsets.resize(fns.size(), 0);

  int64_t offset = 0;
  int64_t max_alignment = 1;
  for (std::size_t i = 0; i < fns.size(); ++i) {
    const auto* fn = fns[i];
    if (fn == nullptr) {
      return arrow::Status::Invalid("aggregate function must not be null");
    }
    const int64_t size = fn->state_size();
    const int64_t alignment = fn->state_alignment();
    if (size < 0) {
      return arrow::Status::Invalid("aggregate state size must be non-negative");
    }
    if (!IsPowerOfTwo(alignment)) {
      return arrow::Status::Invalid("aggregate state alignment must be a power of two");
    }

    offset = AlignUp(offset, alignment);
    if (offset == std::numeric_limits<int64_t>::max()) {
      return arrow::Status::Invalid("aggregate state layout overflow");
    }
    out.offsets[i] = offset;

    if (size > 0 && offset > std::numeric_limits<int64_t>::max() - size) {
      return arrow::Status::Invalid("aggregate state layout overflow");
    }
    offset += size;
    max_alignment = std::max<int64_t>(max_alignment, alignment);
  }

  if (!IsPowerOfTwo(max_alignment)) {
    return arrow::Status::Invalid("internal error: invalid aggregate row alignment");
  }
  out.row_alignment = max_alignment;
  out.row_size = AlignUp(offset, max_alignment);
  if (out.row_size == std::numeric_limits<int64_t>::max()) {
    return arrow::Status::Invalid("aggregate state layout overflow");
  }
  return out;
}

}  // namespace tiforth::detail
