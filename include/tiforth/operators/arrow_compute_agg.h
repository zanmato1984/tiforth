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

struct ArrowComputeAggOptions {
  // When enabled, for binary/string group keys, encode keys into stable int32 dictionary codes
  // (dictionary grows incrementally across batches), aggregate on the fixed-width codes, and
  // decode output keys back to the original binary/string type.
  //
  // This is a performance-oriented option for varlen keys; it trades memory/latency for throughput.
  bool stable_dictionary_encode_binary_keys = false;
};

class ArrowComputeAggTransformOp final : public TransformOp {
 public:
  ArrowComputeAggTransformOp(const Engine* engine, std::vector<AggKey> keys,
                             std::vector<AggFunc> aggs, ArrowComputeAggOptions options = {},
                             arrow::MemoryPool* memory_pool = nullptr);
 ~ArrowComputeAggTransformOp() override;

 protected:
  arrow::Result<OperatorStatus> TransformImpl(std::shared_ptr<arrow::RecordBatch>* batch) override;

 private:
  arrow::Result<std::shared_ptr<arrow::RecordBatch>> NextOutputBatch();
  arrow::Result<OperatorStatus> TransformImplStableDictionary(std::shared_ptr<arrow::RecordBatch>* batch);

  struct ExecState;
  struct DictState;

  const Engine* engine_ = nullptr;
  std::vector<AggKey> keys_;
  std::vector<AggFunc> aggs_;
  ArrowComputeAggOptions options_;

  std::shared_ptr<arrow::Schema> input_schema_;
  arrow::compute::ExecContext exec_context_;

  std::unique_ptr<ExecState> exec_state_;
  std::unique_ptr<DictState> dict_state_;

  bool finalized_ = false;
  bool eos_forwarded_ = false;
};

}  // namespace tiforth
