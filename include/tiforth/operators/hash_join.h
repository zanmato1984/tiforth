#pragma once

#include <memory>
#include <string>
#include <vector>

#include "tiforth/pipeline/op/op.h"

namespace arrow {
class MemoryPool;
class RecordBatch;
}  // namespace arrow

namespace tiforth {

class Engine;

struct JoinKey {
  std::vector<std::string> left;
  std::vector<std::string> right;
};

class HashJoinPipeOp final : public pipeline::PipeOp {
 public:
  HashJoinPipeOp(const Engine* engine,
                 std::vector<std::shared_ptr<arrow::RecordBatch>> build_batches, JoinKey key,
                 arrow::MemoryPool* memory_pool = nullptr);
  ~HashJoinPipeOp() override;

  pipeline::PipelinePipe Pipe(const pipeline::PipelineContext&) override;
  pipeline::PipelineDrain Drain(const pipeline::PipelineContext&) override;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace tiforth
