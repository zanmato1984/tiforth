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

#include "tiforth/operators/agg_defs.h"
#include "tiforth/pipeline/op/op.h"

namespace arrow {
class MemoryPool;
namespace compute {
class Grouper;
}  // namespace compute
}  // namespace arrow

namespace tiforth {

class Engine;

class HashAggContext;

struct HashAggPartialState {
  HashAggPartialState() = default;
  HashAggPartialState(const HashAggPartialState&) = delete;
  HashAggPartialState& operator=(const HashAggPartialState&) = delete;
  HashAggPartialState(HashAggPartialState&&);
  HashAggPartialState& operator=(HashAggPartialState&&);
  ~HashAggPartialState();

  // Keepalive for Arrow compute objects (Grouper / KernelState) which may store a raw pointer to
  // ExecContext.
  std::shared_ptr<arrow::compute::ExecContext> exec_context;

  std::shared_ptr<arrow::Schema> input_schema;
  std::vector<arrow::TypeHolder> key_types;
  std::unique_ptr<arrow::compute::Grouper> grouper;
  std::vector<std::vector<arrow::TypeHolder>> agg_in_types;
  std::vector<std::unique_ptr<arrow::compute::KernelState>> agg_states;
};

// A TiForth-native hash aggregation operator driven by Arrow's Grouper + grouped hash_* kernels.
//
// This intentionally does not depend on Arrow Acero ExecPlan. It is the building block for:
// - pluggable Grouper implementations (collation / short-string optimization),
// - grouped hash_* parity tests against TiFlash native aggregation.
//
// MS20+: implemented as a breaker-style hash aggregation (partial TransformOp + merge SinkOp + result SourceOp).
class HashAggTransformOp final : public pipeline::PipeOp {
 public:
  using GrouperFactory =
      std::function<arrow::Result<std::unique_ptr<arrow::compute::Grouper>>(
          const std::vector<arrow::TypeHolder>& key_types,
          arrow::compute::ExecContext* exec_context)>;

  explicit HashAggTransformOp(std::shared_ptr<HashAggContext> context);
  HashAggTransformOp(std::shared_ptr<HashAggContext> context,
                     std::function<arrow::Status(HashAggPartialState, pipeline::ThreadId)>
                         on_partial_sealed);
  ~HashAggTransformOp() override;

  pipeline::PipelinePipe Pipe(const pipeline::PipelineContext&) override;
  pipeline::PipelineDrain Drain(const pipeline::PipelineContext&) override;

 private:
  arrow::Status SealPartial(pipeline::ThreadId thread_id);
  arrow::Status InitIfNeededAndConsume(const arrow::RecordBatch& batch);
  arrow::Status ConsumeBatch(const arrow::RecordBatch& batch);

  std::shared_ptr<HashAggContext> context_;
  const Engine* engine_ = nullptr;

  std::shared_ptr<arrow::Schema> input_schema_;
  std::shared_ptr<arrow::compute::ExecContext> exec_context_;

  struct Compiled;
  std::unique_ptr<Compiled> compiled_;

  std::vector<arrow::TypeHolder> key_types_;
  std::unique_ptr<arrow::compute::Grouper> grouper_;
  std::vector<arrow::compute::Aggregate> aggregates_;
  std::vector<std::vector<arrow::TypeHolder>> agg_in_types_;
  std::vector<const arrow::compute::HashAggregateKernel*> agg_kernels_;
  std::vector<std::unique_ptr<arrow::compute::KernelState>> agg_states_;

  std::function<arrow::Status(HashAggPartialState, pipeline::ThreadId)> on_partial_sealed_;
  bool sealed_ = false;
};

class HashAggContext final {
 public:
  using GrouperFactory = HashAggTransformOp::GrouperFactory;
  using PartialState = HashAggPartialState;

  HashAggContext(const Engine* engine, std::vector<AggKey> keys, std::vector<AggFunc> aggs,
                 GrouperFactory grouper_factory = {}, arrow::MemoryPool* memory_pool = nullptr);

  HashAggContext(const HashAggContext&) = delete;
  HashAggContext& operator=(const HashAggContext&) = delete;

  ~HashAggContext();

  const Engine* engine() const { return engine_; }
  const std::vector<AggKey>& keys() const { return keys_; }
  const std::vector<AggFunc>& aggs() const { return aggs_; }
  const GrouperFactory& grouper_factory() const { return grouper_factory_; }

  arrow::MemoryPool* memory_pool() const { return exec_context_.memory_pool(); }

  arrow::Status MergePartial(PartialState partial);
  arrow::Status Finalize();
  arrow::Result<std::shared_ptr<arrow::RecordBatch>> OutputBatch();

 private:
  arrow::Status InitIfNeeded(const PartialState& partial);

  const Engine* engine_ = nullptr;
  std::vector<AggKey> keys_;
  std::vector<AggFunc> aggs_;
  GrouperFactory grouper_factory_;

  std::shared_ptr<arrow::Schema> input_schema_;
  arrow::compute::ExecContext exec_context_;

  std::vector<arrow::compute::Aggregate> aggregates_;
  std::vector<std::vector<arrow::TypeHolder>> agg_in_types_;
  std::vector<const arrow::compute::HashAggregateKernel*> agg_kernels_;
  std::vector<std::unique_ptr<arrow::compute::KernelState>> agg_states_;

  std::unique_ptr<arrow::compute::Grouper> grouper_;

  std::shared_ptr<arrow::Schema> output_schema_;
  std::shared_ptr<arrow::RecordBatch> output_batch_;
  bool finalized_ = false;
};

class HashAggMergeSinkOp final : public pipeline::SinkOp {
 public:
  explicit HashAggMergeSinkOp(std::shared_ptr<HashAggContext> context);

  pipeline::PipelineSink Sink(const pipeline::PipelineContext&) override;
  std::optional<task::TaskGroup> Backend(const pipeline::PipelineContext&) override;

 private:
  std::shared_ptr<HashAggContext> context_;
  bool finalized_ = false;
};

class HashAggResultSourceOp final : public pipeline::SourceOp {
 public:
  HashAggResultSourceOp(std::shared_ptr<HashAggContext> context, int64_t max_output_rows = 65536);
  HashAggResultSourceOp(std::shared_ptr<HashAggContext> context, int64_t start_row, int64_t end_row,
                        int64_t max_output_rows = 65536);

  pipeline::PipelineSource Source(const pipeline::PipelineContext&) override;

 private:
  std::shared_ptr<HashAggContext> context_;
  int64_t start_row_ = 0;
  int64_t end_row_ = -1;
  int64_t next_row_ = 0;
  bool emitted_empty_ = false;
  int64_t max_output_rows_ = 65536;
};

}  // namespace tiforth
