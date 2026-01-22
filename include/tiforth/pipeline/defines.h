#pragma once

#include <arrow/record_batch.h>
#include <arrow/result.h>

#include <cstddef>
#include <memory>

namespace tiforth::pipeline {

using ThreadId = std::size_t;
using Batch = std::shared_ptr<arrow::RecordBatch>;

class OpOutput;
using OpResult = arrow::Result<OpOutput>;

}  // namespace tiforth::pipeline

