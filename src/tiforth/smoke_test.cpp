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

#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/record_batch.h>
#include <arrow/testing/gtest_util.h>

#include <arrow/array.h>
#include <arrow/builder.h>

#include <gtest/gtest.h>

#include "tiforth/engine.h"
#include "tiforth/traits.h"

#include "tiforth/testing/test_pipeline_ops.h"
#include "tiforth/testing/test_task_group_runner.h"

namespace tiforth {

TIFORTH_SCHEDULER_TEST_SUITE(TiForthSmokeTest);


namespace {

arrow::Status RunSmoke() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));

  auto schema = arrow::schema({arrow::field("x", arrow::int32())});
  arrow::Int32Builder values;
  ARROW_RETURN_NOT_OK(values.AppendValues({1, 2, 3}));
  std::shared_ptr<arrow::Array> array;
  ARROW_RETURN_NOT_OK(values.Finish(&array));

  auto batch = arrow::RecordBatch::Make(schema, /*num_rows=*/3, {array});
  auto source_op = std::make_unique<test::VectorSourceOp>(
      std::vector<std::shared_ptr<arrow::RecordBatch>>{batch});

  test::CollectSinkOp::OutputsByThread outputs_by_thread(1);
  auto sink_op = std::make_unique<test::CollectSinkOp>(&outputs_by_thread);

  Pipeline::Channel channel;
  channel.source_op = source_op.get();
  channel.pipe_ops = {};

  Pipeline logical_pipeline{"Smoke", std::vector<Pipeline::Channel>{std::move(channel)},
                                   sink_op.get()};

  ARROW_ASSIGN_OR_RAISE(auto task_groups,
                        test::CompileToTaskGroups(logical_pipeline, /*dop=*/1));

  auto task_ctx = test::MakeTestTaskContext();
  ARROW_RETURN_NOT_OK(test::RunTaskGroupsToCompletion(task_groups, task_ctx));

  auto outputs = test::FlattenOutputs(std::move(outputs_by_thread));
  if (outputs.size() != 1) {
    return arrow::Status::Invalid("expected exactly 1 output batch");
  }
  if (outputs[0].get() != batch.get()) {
    return arrow::Status::Invalid("expected pass-through output batch");
  }

  return arrow::Status::OK();
}

}  // namespace

TIFORTH_SCHEDULER_TEST(TiForthSmokeTest, Lifecycle) {
  auto status = RunSmoke();
  ASSERT_OK(status);
}

}  // namespace tiforth
