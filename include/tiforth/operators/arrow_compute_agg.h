#pragma once

#include <memory>
#include <string>
#include <vector>

#include <arrow/compute/exec.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>

#include "tiforth/operators.h"
#include "tiforth/operators/agg_defs.h"

namespace arrow {
class MemoryPool;
}  // namespace arrow

namespace tiforth {

class Engine;

class ArrowComputeAggTransformOp final : public TransformOp {
 public:
  ArrowComputeAggTransformOp(const Engine* engine, std::vector<AggKey> keys,
                             std::vector<AggFunc> aggs, arrow::MemoryPool* memory_pool = nullptr);
 ~ArrowComputeAggTransformOp() override;

 protected:
  arrow::Result<OperatorStatus> TransformImpl(std::shared_ptr<arrow::RecordBatch>* batch) override;

 private:
  arrow::Result<std::shared_ptr<arrow::RecordBatch>> NextOutputBatch();

  struct ExecState;

  const Engine* engine_ = nullptr;
  std::vector<AggKey> keys_;
  std::vector<AggFunc> aggs_;

  std::shared_ptr<arrow::Schema> input_schema_;
  arrow::compute::ExecContext exec_context_;

  std::unique_ptr<ExecState> exec_state_;

  bool finalized_ = false;
  bool eos_forwarded_ = false;
};

}  // namespace tiforth
