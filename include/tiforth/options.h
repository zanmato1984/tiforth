#pragma once

#include <cstddef>

namespace tiforth {

struct Options {
  // Keep the defaults aligned with Arrow (22.0.0):
  // - arrow::acero::ExecPlan::kMaxBatchSize == 1<<15
  // - arrow::util::MiniBatch::kMiniBatchLength == 1<<10
  static constexpr std::size_t kDefaultMaxBatchLength = 1U << 15;
  static constexpr std::size_t kDefaultMiniBatchLength = 1U << 10;

  std::size_t source_max_batch_length = kDefaultMaxBatchLength;
  std::size_t pipe_max_batch_length = kDefaultMaxBatchLength;
  std::size_t mini_batch_length = kDefaultMiniBatchLength;
};

}  // namespace tiforth
