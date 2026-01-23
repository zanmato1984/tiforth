#pragma once

#include <memory>

#include <arrow/compute/exec.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>

#include "tiforth/expr.h"
#include "tiforth/pipeline/op/op.h"

namespace arrow {
class Array;
class MemoryPool;
class Schema;
}  // namespace arrow

namespace tiforth {

class FilterPipeOp final : public pipeline::PipeOp {
 public:
  FilterPipeOp(const Engine* engine, ExprPtr predicate, arrow::MemoryPool* memory_pool = nullptr);
  ~FilterPipeOp() override;

  pipeline::PipelinePipe Pipe(const pipeline::PipelineContext&) override;
  pipeline::PipelineDrain Drain(const pipeline::PipelineContext&) override;

 private:
  arrow::Result<std::shared_ptr<arrow::RecordBatch>> Filter(const arrow::RecordBatch& input);

  struct Compiled;

  const Engine* engine_ = nullptr;
  ExprPtr predicate_;
  std::shared_ptr<arrow::Schema> output_schema_;
  std::unique_ptr<Compiled> compiled_;
  arrow::compute::ExecContext exec_context_;
};

}  // namespace tiforth
