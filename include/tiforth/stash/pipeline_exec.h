#pragma once

#include <cstddef>
#include <memory>
#include <vector>

#include <arrow/record_batch.h>
#include <arrow/result.h>

#include "tiforth/stash/operators.h"

namespace tiforth {

class PipelineExec {
 public:
  PipelineExec(SourceOpPtr source, TransformOps transforms, SinkOpPtr sink);

  PipelineExec(const PipelineExec&) = delete;
  PipelineExec& operator=(const PipelineExec&) = delete;

  ~PipelineExec();

  arrow::Result<OperatorStatus> Execute();
  arrow::Result<OperatorStatus> ExecuteIO();
  arrow::Result<OperatorStatus> Await();
  arrow::Status Notify();

 private:
  arrow::Result<OperatorStatus> FetchBatch(std::shared_ptr<arrow::RecordBatch>* batch,
                                          std::size_t* start_transform_op_index);

  SourceOpPtr source_;
  TransformOps transforms_;
  SinkOpPtr sink_;

  Operator* awaitable_ = nullptr;
  Operator* io_op_ = nullptr;
  Operator* waiting_for_notify_ = nullptr;
};

struct PipelineExecBuilder {
  SourceOpPtr source_op;
  TransformOps transform_ops;
  SinkOpPtr sink_op;

  void SetSourceOp(SourceOpPtr source);
  void AppendTransformOp(TransformOpPtr transform);
  void SetSinkOp(SinkOpPtr sink);

  arrow::Result<std::unique_ptr<PipelineExec>> Build();
};

}  // namespace tiforth
