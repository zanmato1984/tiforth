#pragma once

#include <memory>
#include <string>
#include <vector>

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

struct ProjectionExpr {
  std::string name;
  ExprPtr expr;
};

class ProjectionPipeOp final : public PipeOp {
 public:
  ProjectionPipeOp(const Engine* engine, std::vector<ProjectionExpr> exprs,
                   arrow::MemoryPool* memory_pool = nullptr);
  ~ProjectionPipeOp() override;

  PipelinePipe Pipe() override;
  PipelineDrain Drain() override;
  std::unique_ptr<SourceOp> ImplicitSource() override;

 private:
  arrow::Result<std::shared_ptr<arrow::RecordBatch>> Project(const arrow::RecordBatch& input);

  struct Compiled;

  arrow::Result<std::shared_ptr<arrow::Schema>> ComputeOutputSchema(
      const arrow::RecordBatch& input, const std::vector<std::shared_ptr<arrow::Array>>& arrays) const;

  const Engine* engine_ = nullptr;
  std::vector<ProjectionExpr> exprs_;
  std::shared_ptr<arrow::Schema> output_schema_;
  std::unique_ptr<Compiled> compiled_;
  arrow::compute::ExecContext exec_context_;
};

}  // namespace tiforth::op
