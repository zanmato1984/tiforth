#pragma once

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include <arrow/result.h>
#include <arrow/type_fwd.h>

#include "tiforth/operators/agg_defs.h"
#include "tiforth/broken_pipeline_traits.h"

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
}  // namespace tiforth

namespace tiforth::op {
class HashAggState;

class HashAggSinkOp final : public SinkOp {
 public:
  explicit HashAggSinkOp(std::shared_ptr<HashAggState> state);

  PipelineSink Sink() override;
  TaskGroups Frontend() override;
  std::optional<TaskGroup> Backend() override;
  std::unique_ptr<SourceOp> ImplicitSource() override;

 private:
  std::shared_ptr<HashAggState> state_;
};

class HashAggResultSourceOp final : public SourceOp {
 public:
  HashAggResultSourceOp(std::shared_ptr<HashAggState> state, int64_t max_output_rows = 65536);
  HashAggResultSourceOp(std::shared_ptr<HashAggState> state, int64_t start_row, int64_t end_row,
                        int64_t max_output_rows = 65536);

  PipelineSource Source() override;
  TaskGroups Frontend() override;
  std::optional<TaskGroup> Backend() override;

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

  arrow::Status Consume(ThreadId thread_id, const arrow::RecordBatch& batch);
  arrow::Status MergeAndFinalize();
  arrow::Result<std::shared_ptr<arrow::RecordBatch>> OutputBatch();

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace tiforth::op
