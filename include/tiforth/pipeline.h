#pragma once

#include <functional>
#include <memory>
#include <vector>

#include <arrow/result.h>
#include <arrow/record_batch.h>

#include "tiforth/engine.h"
#include "tiforth/pipeline/op/op.h"
#include "tiforth/task.h"

namespace tiforth {

class Pipeline;

class PipelineBuilder {
 public:
  static arrow::Result<std::unique_ptr<PipelineBuilder>> Create(const Engine* engine);

  PipelineBuilder(const PipelineBuilder&) = delete;
  PipelineBuilder& operator=(const PipelineBuilder&) = delete;

  ~PipelineBuilder();

  using PipeFactory = std::function<arrow::Result<std::unique_ptr<pipeline::PipeOp>>()>;

  arrow::Status AppendPipe(PipeFactory factory);

  arrow::Result<std::unique_ptr<Pipeline>> Finalize();

 private:
  explicit PipelineBuilder(const Engine* engine);

  const Engine* engine_;
  std::vector<PipeFactory> pipe_factories_;
};

class Pipeline {
 public:
  Pipeline(const Pipeline&) = delete;
  Pipeline& operator=(const Pipeline&) = delete;

  arrow::Result<std::unique_ptr<Task>> CreateTask() const;
  arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> MakeReader(
      std::shared_ptr<arrow::RecordBatchReader> input) const;

 private:
  Pipeline(const Engine* engine, std::vector<PipelineBuilder::PipeFactory> pipe_factories);

  const Engine* engine_;
  std::vector<PipelineBuilder::PipeFactory> pipe_factories_;

  friend class PipelineBuilder;
};

}  // namespace tiforth
