#pragma once

#include <memory>

#include <arrow/compute/exec.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>

#include "tiforth/expr.h"
#include "tiforth/traits.h"

namespace arrow {
class Array;
class MemoryPool;
class Schema;
}  // namespace arrow

namespace tiforth::op {

class FilterPipeOp final : public PipeOp {
 public:
  FilterPipeOp(const Engine* engine, ExprPtr predicate, arrow::MemoryPool* memory_pool = nullptr);
  ~FilterPipeOp() override;

  PipelinePipe Pipe() override;
  PipelineDrain Drain() override;
  std::unique_ptr<SourceOp> ImplicitSource() override;

 private:
  arrow::Result<std::shared_ptr<arrow::RecordBatch>> Filter(const arrow::RecordBatch& input);

  struct Compiled;

  const Engine* engine_ = nullptr;
  ExprPtr predicate_;
  std::shared_ptr<arrow::Schema> output_schema_;
  std::unique_ptr<Compiled> compiled_;
  arrow::compute::ExecContext exec_context_;
};

}  // namespace tiforth::op
