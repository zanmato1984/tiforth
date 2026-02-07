// Copyright 2026 TiForth Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "tiforth/detail/arrow_compute.h"

#include <mutex>

#include <arrow/array/concatenate.h>
#include <arrow/array/util.h>
#include <arrow/chunked_array.h>
#include <arrow/compute/initialize.h>
#include <arrow/datum.h>
#include <arrow/memory_pool.h>

namespace tiforth::detail {

namespace {

std::once_flag arrow_compute_init_once;
arrow::Status arrow_compute_init_status = arrow::Status::OK();

}  // namespace

arrow::Status EnsureArrowComputeInitialized() {
  std::call_once(arrow_compute_init_once,
                 []() { arrow_compute_init_status = arrow::compute::Initialize(); });
  return arrow_compute_init_status;
}

arrow::Result<std::shared_ptr<arrow::Array>> DatumToArray(const arrow::Datum& datum,
                                                          arrow::MemoryPool* pool) {
  if (datum.is_array()) {
    return datum.make_array();
  }
  if (datum.is_chunked_array()) {
    auto chunked = datum.chunked_array();
    if (chunked == nullptr) {
      return arrow::Status::Invalid("expected non-null chunked array datum");
    }
    if (chunked->num_chunks() == 1) {
      return chunked->chunk(0);
    }
    return arrow::Concatenate(chunked->chunks(), pool);
  }
  return arrow::Status::Invalid("expected array or chunked array result");
}

arrow::Result<std::shared_ptr<arrow::Array>> DatumToArray(const arrow::Datum& datum, int64_t length,
                                                          arrow::MemoryPool* pool) {
  if (datum.is_scalar()) {
    return arrow::MakeArrayFromScalar(*datum.scalar(), length, pool);
  }
  return DatumToArray(datum, pool);
}

}  // namespace tiforth::detail
