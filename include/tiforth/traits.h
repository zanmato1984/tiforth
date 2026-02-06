#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include <broken_pipeline/broken_pipeline.h>
#include <broken_pipeline/traits/arrow.h>

namespace tiforth {

using Traits = bp::traits::arrow::Traits;

// Convenience aliases (modeled after broken-pipeline's schedule/traits.h).
using Batch = Traits::Batch;
using Status = Traits::Status;

template <class T>
using Result = Traits::template Result<T>;

using TaskContext = bp::TaskContext<Traits>;
using TaskGroup = bp::TaskGroup<Traits>;
using TaskGroups = std::vector<TaskGroup>;

using Task = bp::Task<Traits>;
using Continuation = bp::Continuation<Traits>;
using TaskId = bp::TaskId;
using ThreadId = bp::ThreadId;
using TaskResult = bp::TaskResult<Traits>;
using TaskStatus = bp::TaskStatus;
using TaskHint = bp::TaskHint;

using Resumer = bp::Resumer;
using ResumerPtr = std::shared_ptr<Resumer>;
using Resumers = std::vector<ResumerPtr>;
using ResumerFactory = bp::ResumerFactory<Traits>;

using Awaiter = bp::Awaiter;
using AwaiterPtr = std::shared_ptr<Awaiter>;
using AwaiterFactory = bp::AwaiterFactory<Traits>;

using OpOutput = bp::OpOutput<Traits>;
using OpResult = bp::OpResult<Traits>;
using PipelineSource = bp::PipelineSource<Traits>;
using PipelineDrain = bp::PipelineDrain<Traits>;
using PipelinePipe = bp::PipelinePipe<Traits>;
using PipelineSink = bp::PipelineSink<Traits>;

using SourceOp = bp::SourceOp<Traits>;
using PipeOp = bp::PipeOp<Traits>;
using SinkOp = bp::SinkOp<Traits>;

using Pipeline = bp::Pipeline<Traits>;
using PipelineChannel = Pipeline::Channel;

using PipelineExec = bp::PipelineExec<Traits>;
using Pipelinexe = bp::Pipelinexe<Traits>;
using PipeExec = bp::PipeExec<Traits>;

using bp::Compile;

}  // namespace tiforth
