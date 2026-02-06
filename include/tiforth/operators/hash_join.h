#pragma once

#include <memory>
#include <string>
#include <vector>

#include "tiforth/traits.h"

namespace arrow {
class MemoryPool;
class RecordBatch;
}  // namespace arrow

namespace tiforth {
class Engine;
}  // namespace tiforth

namespace tiforth::op {
struct JoinKey {
  std::vector<std::string> left;
  std::vector<std::string> right;
};

class HashJoinPipeOp final : public PipeOp {
 public:
  HashJoinPipeOp(const Engine* engine,
                 std::vector<std::shared_ptr<arrow::RecordBatch>> build_batches, JoinKey key,
                 arrow::MemoryPool* memory_pool = nullptr);
  ~HashJoinPipeOp() override;

  PipelinePipe Pipe() override;
  PipelineDrain Drain() override;
  std::unique_ptr<SourceOp> ImplicitSource() override;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace tiforth::op
