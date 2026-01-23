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

class HashAggState;

class HashAggSinkOp final : public pipeline::SinkOp {
 public:
  explicit HashAggSinkOp(std::shared_ptr<HashAggState> state);

  pipeline::PipelineSink Sink(const pipeline::PipelineContext&) override;
  std::optional<task::TaskGroup> Backend(const pipeline::PipelineContext&) override;
  std::unique_ptr<pipeline::SourceOp> ImplicitSource(const pipeline::PipelineContext&) override;

 private:
  std::shared_ptr<HashAggState> state_;
  bool backend_done_ = false;
};

class HashAggResultSourceOp final : public pipeline::SourceOp {
 public:
  HashAggResultSourceOp(std::shared_ptr<HashAggState> state, int64_t max_output_rows = 65536);
  HashAggResultSourceOp(std::shared_ptr<HashAggState> state, int64_t start_row, int64_t end_row,
                        int64_t max_output_rows = 65536);

  pipeline::PipelineSource Source(const pipeline::PipelineContext&) override;

 private:
  std::shared_ptr<HashAggState> state_;
  int64_t start_row_ = 0;
  int64_t end_row_ = -1;
  int64_t next_row_ = 0;
  bool emitted_empty_ = false;
  int64_t max_output_rows_ = 65536;
};

class HashAggState final {
 public:
  using GrouperFactory =
      std::function<arrow::Result<std::unique_ptr<arrow::compute::Grouper>>(
          const std::vector<arrow::TypeHolder>& key_types,
          arrow::compute::ExecContext* exec_context)>;

  HashAggState(const Engine* engine, std::vector<AggKey> keys, std::vector<AggFunc> aggs,
               GrouperFactory grouper_factory = {}, arrow::MemoryPool* memory_pool = nullptr,
               std::size_t dop = 1);

  HashAggState(const HashAggState&) = delete;
  HashAggState& operator=(const HashAggState&) = delete;

  ~HashAggState();

  const Engine* engine() const { return engine_; }
  const std::vector<AggKey>& keys() const { return keys_; }
  const std::vector<AggFunc>& aggs() const { return aggs_; }
  const GrouperFactory& grouper_factory() const { return grouper_factory_; }

  arrow::MemoryPool* memory_pool() const { return exec_context_.memory_pool(); }

  arrow::Status Consume(pipeline::ThreadId thread_id, const arrow::RecordBatch& batch);
  arrow::Status MergeAndFinalize();
  arrow::Result<std::shared_ptr<arrow::RecordBatch>> OutputBatch();

 private:
  struct Compiled;
  struct ThreadLocal;

  arrow::Status InitIfNeeded(const arrow::RecordBatch& batch);
  arrow::Status ConsumeBatch(pipeline::ThreadId thread_id, const arrow::RecordBatch& batch);
  arrow::Status Merge();
  arrow::Status Finalize();

  const Engine* engine_ = nullptr;
  std::vector<AggKey> keys_;
  std::vector<AggFunc> aggs_;
  GrouperFactory grouper_factory_;

  std::shared_ptr<arrow::Schema> input_schema_;
  arrow::compute::ExecContext exec_context_;
  std::size_t dop_ = 1;

  std::unique_ptr<Compiled> compiled_;

  std::vector<arrow::TypeHolder> key_types_;
  std::vector<arrow::compute::Aggregate> aggregates_;
  std::vector<std::vector<arrow::TypeHolder>> agg_in_types_;
  std::vector<const arrow::compute::HashAggregateKernel*> agg_kernels_;

  std::vector<ThreadLocal> thread_locals_;

  std::shared_ptr<arrow::Schema> output_schema_;
  std::shared_ptr<arrow::RecordBatch> output_batch_;
  bool finalized_ = false;
};

}  // namespace tiforth

