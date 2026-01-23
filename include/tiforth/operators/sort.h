#pragma once

#include <memory>
#include <string>
#include <vector>

#include <arrow/compute/exec.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>

#include "tiforth/pipeline/op/op.h"

namespace arrow {
class Array;
class MemoryPool;
class Schema;
}  // namespace arrow

namespace tiforth {

class Engine;

struct SortKey {
  std::string name;
  bool ascending = true;
  bool nulls_first = false;
};

class SortPipeOp final : public pipeline::PipeOp {
 public:
  SortPipeOp(const Engine* engine, std::vector<SortKey> keys, arrow::MemoryPool* memory_pool = nullptr);

  pipeline::PipelinePipe Pipe(const pipeline::PipelineContext&) override;
  pipeline::PipelineDrain Drain(const pipeline::PipelineContext&) override;

 private:
  arrow::Result<std::shared_ptr<arrow::RecordBatch>> SortAll();

  std::vector<SortKey> keys_;
  std::shared_ptr<arrow::Schema> output_schema_;
  std::vector<std::shared_ptr<arrow::RecordBatch>> buffered_;

  bool drained_ = false;

  arrow::compute::ExecContext exec_context_;
};

}  // namespace tiforth
