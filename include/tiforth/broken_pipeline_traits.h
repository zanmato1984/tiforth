#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/status.h>

#include <broken_pipeline/broken_pipeline.h>

#include "tiforth/query_context.h"

namespace tiforth {

// TiForth adopts Broken Pipeline as its pipeline/task protocol implementation.
//
// This Traits type binds Broken Pipeline's generic APIs to Arrow-native TiForth types:
// - Batch: shared_ptr<RecordBatch>
// - Status/Result: Arrow Status/Result
// - Context: QueryContext (optional per-query state; may be null in TaskContext)
struct BrokenPipelineTraits {
  using Batch = std::shared_ptr<arrow::RecordBatch>;
  using Context = ::tiforth::QueryContext;
  using Status = arrow::Status;

  template <class T>
  using Result = arrow::Result<T>;
};

// Convenience aliases (modeled after broken-pipeline's tests/examples).
using Batch = BrokenPipelineTraits::Batch;
using Status = BrokenPipelineTraits::Status;

template <class T>
using Result = BrokenPipelineTraits::Result<T>;

using TaskContext = bp::TaskContext<BrokenPipelineTraits>;
using TaskGroup = bp::TaskGroup<BrokenPipelineTraits>;
using TaskGroups = std::vector<TaskGroup>;

using Task = bp::Task<BrokenPipelineTraits>;
using Continuation = bp::Continuation<BrokenPipelineTraits>;
using TaskId = bp::TaskId;
using ThreadId = bp::ThreadId;
using TaskResult = bp::TaskResult<BrokenPipelineTraits>;
using TaskStatus = bp::TaskStatus;
using TaskHint = bp::TaskHint;

using Resumer = bp::Resumer;
using ResumerPtr = std::shared_ptr<Resumer>;
using Resumers = std::vector<ResumerPtr>;
using ResumerFactory = bp::ResumerFactory<BrokenPipelineTraits>;

using Awaiter = bp::Awaiter;
using AwaiterPtr = std::shared_ptr<Awaiter>;
using AwaiterFactory = bp::AwaiterFactory<BrokenPipelineTraits>;

using OpOutput = bp::OpOutput<BrokenPipelineTraits>;
using OpResult = bp::OpResult<BrokenPipelineTraits>;
using PipelineSource = bp::PipelineSource<BrokenPipelineTraits>;
using PipelineDrain = bp::PipelineDrain<BrokenPipelineTraits>;
using PipelinePipe = bp::PipelinePipe<BrokenPipelineTraits>;
using PipelineSink = bp::PipelineSink<BrokenPipelineTraits>;

using SourceOp = bp::SourceOp<BrokenPipelineTraits>;
using PipeOp = bp::PipeOp<BrokenPipelineTraits>;
using SinkOp = bp::SinkOp<BrokenPipelineTraits>;

using Pipeline = bp::Pipeline<BrokenPipelineTraits>;
using PipelineChannel = Pipeline::Channel;

using PipelineExec = bp::PipelineExec<BrokenPipelineTraits>;
using Pipelinexe = bp::Pipelinexe<BrokenPipelineTraits>;
using PipeExec = bp::PipeExec<BrokenPipelineTraits>;

}  // namespace tiforth
