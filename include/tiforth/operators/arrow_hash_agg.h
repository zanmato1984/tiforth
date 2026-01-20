#pragma once

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include <arrow/compute/api_aggregate.h>
#include <arrow/compute/exec.h>
#include <arrow/compute/kernel.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/type_fwd.h>

#include "tiforth/operators.h"
#include "tiforth/operators/agg_defs.h"

namespace arrow {
class MemoryPool;
namespace compute {
class Grouper;
}  // namespace compute
}  // namespace arrow

namespace tiforth {

class Engine;

// A TiForth-native hash aggregation operator driven by Arrow's Grouper + grouped hash_* kernels.
//
// This intentionally does not depend on Arrow Acero ExecPlan. It is the building block for:
// - pluggable Grouper implementations (collation / short-string optimization),
// - grouped hash_* parity tests against TiFlash native aggregation.
class ArrowHashAggTransformOp final : public TransformOp {
 public:
  using GrouperFactory =
      std::function<arrow::Result<std::unique_ptr<arrow::compute::Grouper>>(
          const std::vector<arrow::TypeHolder>& key_types,
          arrow::compute::ExecContext* exec_context)>;

  ArrowHashAggTransformOp(const Engine* engine, std::vector<AggKey> keys, std::vector<AggFunc> aggs,
                          GrouperFactory grouper_factory = {},
                          arrow::MemoryPool* memory_pool = nullptr);
  ~ArrowHashAggTransformOp() override;

 protected:
  arrow::Result<OperatorStatus> TransformImpl(std::shared_ptr<arrow::RecordBatch>* batch) override;

 private:
  arrow::Result<std::shared_ptr<arrow::RecordBatch>> NextOutputBatch();
  arrow::Status InitIfNeededAndConsume(const arrow::RecordBatch& batch);
  arrow::Status ConsumeBatch(const arrow::RecordBatch& batch);

  const Engine* engine_ = nullptr;
  std::vector<AggKey> keys_;
  std::vector<AggFunc> aggs_;
  GrouperFactory grouper_factory_;

  std::shared_ptr<arrow::Schema> input_schema_;
  arrow::compute::ExecContext exec_context_;

  struct Compiled;
  std::unique_ptr<Compiled> compiled_;

  std::unique_ptr<arrow::compute::Grouper> grouper_;
  std::vector<arrow::compute::Aggregate> aggregates_;
  std::vector<std::vector<arrow::TypeHolder>> agg_in_types_;
  std::vector<const arrow::compute::HashAggregateKernel*> agg_kernels_;
  std::vector<std::unique_ptr<arrow::compute::KernelState>> agg_states_;

  std::shared_ptr<arrow::Schema> output_schema_;
  std::shared_ptr<arrow::RecordBatch> output_batch_;
  int64_t output_offset_ = 0;
  bool output_started_ = false;

  bool finalized_ = false;
  bool eos_forwarded_ = false;
};

}  // namespace tiforth
