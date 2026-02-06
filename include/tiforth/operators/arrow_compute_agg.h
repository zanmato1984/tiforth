#pragma once

#include <memory>
#include <string>
#include <vector>

#include <arrow/compute/exec.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>

#include "tiforth/operators/agg_defs.h"
#include "tiforth/traits.h"

namespace arrow {
class MemoryPool;
}  // namespace arrow

namespace tiforth {
class Engine;
}  // namespace tiforth

namespace tiforth::op {

struct ArrowComputeAggOptions {
  // When enabled, for binary/string group keys, encode keys into stable int32 dictionary codes
  // (dictionary grows incrementally across batches), aggregate on the fixed-width codes, and
  // decode output keys back to the original binary/string type.
  //
  // This is a performance-oriented option for varlen keys; it trades memory/latency for throughput.
  bool stable_dictionary_encode_binary_keys = false;
};

class ArrowComputeAggPipeOp final : public PipeOp {
 public:
  ArrowComputeAggPipeOp(const Engine* engine, std::vector<AggKey> keys, std::vector<AggFunc> aggs,
                        ArrowComputeAggOptions options = {},
                        arrow::MemoryPool* memory_pool = nullptr);
  ~ArrowComputeAggPipeOp() override;

  PipelinePipe Pipe() override;
  PipelineDrain Drain() override;
  std::unique_ptr<SourceOp> ImplicitSource() override;

 private:
  arrow::Status ConsumeBatch(std::shared_ptr<arrow::RecordBatch> batch);
  arrow::Status ConsumeBatchStableDictionary(std::shared_ptr<arrow::RecordBatch> batch);
  arrow::Result<std::shared_ptr<arrow::RecordBatch>> NextOutputBatch();
  arrow::Status FinalizeIfNeeded();

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
};

}  // namespace tiforth::op
