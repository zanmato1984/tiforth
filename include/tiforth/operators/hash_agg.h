#pragma once

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include <arrow/result.h>
#include <arrow/type_fwd.h>

#include "tiforth/operators/agg_defs.h"
#include "tiforth/pipeline/op/op.h"

namespace arrow {
class MemoryPool;
class RecordBatch;
class Schema;
namespace compute {
class ExecContext;
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
  task::TaskGroups Frontend(const pipeline::PipelineContext&) override;
  std::unique_ptr<pipeline::SourceOp> ImplicitSource(const pipeline::PipelineContext&) override;

 private:
  std::shared_ptr<HashAggState> state_;
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

  const Engine* engine() const;
  const std::vector<AggKey>& keys() const;
  const std::vector<AggFunc>& aggs() const;
  const GrouperFactory& grouper_factory() const;

  arrow::MemoryPool* memory_pool() const;

  arrow::Status Consume(pipeline::ThreadId thread_id, const arrow::RecordBatch& batch);
  arrow::Status MergeAndFinalize();
  arrow::Result<std::shared_ptr<arrow::RecordBatch>> OutputBatch();

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace tiforth
