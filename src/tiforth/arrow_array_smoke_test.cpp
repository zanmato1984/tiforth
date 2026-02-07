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

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/status.h>
#include <arrow/testing/gtest_util.h>

#include <gtest/gtest.h>

namespace tiforth {

namespace {

arrow::Status RunArrowArraySmoke() {
  arrow::Int32Builder builder;
  ARROW_RETURN_NOT_OK(builder.Append(1));
  ARROW_RETURN_NOT_OK(builder.Append(2));
  ARROW_RETURN_NOT_OK(builder.Append(3));

  std::shared_ptr<arrow::Array> array;
  ARROW_RETURN_NOT_OK(builder.Finish(&array));

  if (array == nullptr) {
    return arrow::Status::Invalid("expected non-null array");
  }
  if (array->type_id() != arrow::Type::INT32) {
    return arrow::Status::Invalid("expected int32 array");
  }
  if (array->length() != 3) {
    return arrow::Status::Invalid("expected length=3");
  }

  auto int32_array = std::static_pointer_cast<arrow::Int32Array>(array);
  if (int32_array->Value(0) != 1 || int32_array->Value(1) != 2 ||
      int32_array->Value(2) != 3) {
    return arrow::Status::Invalid("unexpected int32 values");
  }

  auto debug_string = int32_array->ToString();
  if (debug_string.empty()) {
    return arrow::Status::Invalid("expected non-empty ToString()");
  }

  return arrow::Status::OK();
}

}  // namespace

TEST(TiForthArrowSmokeTest, ArrowArray) {
  auto status = RunArrowArraySmoke();
  ASSERT_OK(status);
}

}  // namespace tiforth
