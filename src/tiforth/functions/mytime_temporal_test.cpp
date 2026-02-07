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

#include <cstdint>
#include <vector>

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <gtest/gtest.h>

#include "tiforth/engine.h"
#include "tiforth/expr.h"
#include "tiforth/operators/projection.h"
#include "tiforth/traits.h"
#include "tiforth/type_metadata.h"

#include "tiforth/testing/test_pipeline_ops.h"
#include "tiforth/testing/test_task_group_runner.h"

namespace tiforth {

namespace {

uint64_t PackMyDateTime(uint16_t year, uint8_t month, uint8_t day, uint16_t hour, uint8_t minute,
                        uint8_t second, uint32_t micro_second) {
  const uint64_t ymd = ((static_cast<uint64_t>(year) * 13 + month) << 5) | day;
  const uint64_t hms = (static_cast<uint64_t>(hour) << 12) |
                       (static_cast<uint64_t>(minute) << 6) | static_cast<uint64_t>(second);
  return ((ymd << 17) | hms) << 24 | static_cast<uint64_t>(micro_second);
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> RunProjection(
    std::shared_ptr<arrow::RecordBatch> input, std::vector<op::ProjectionExpr> exprs) {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));
  if (input == nullptr) {
    return arrow::Status::Invalid("input must not be null");
  }

  auto source_op = std::make_unique<test::VectorSourceOp>(
      std::vector<std::shared_ptr<arrow::RecordBatch>>{input});

  std::vector<std::unique_ptr<PipeOp>> pipe_ops;
  pipe_ops.push_back(std::make_unique<op::ProjectionPipeOp>(engine.get(), std::move(exprs)));

  test::CollectSinkOp::OutputsByThread outputs_by_thread(1);
  auto sink_op = std::make_unique<test::CollectSinkOp>(&outputs_by_thread);

  Pipeline::Channel channel;
  channel.source_op = source_op.get();
  channel.pipe_ops.reserve(pipe_ops.size());
  for (auto& op : pipe_ops) {
    channel.pipe_ops.push_back(op.get());
  }

  Pipeline logical_pipeline{
      "MyTimeProjection",
      std::vector<Pipeline::Channel>{std::move(channel)},
      sink_op.get()};

  ARROW_ASSIGN_OR_RAISE(
      auto task_groups,
      test::CompileToTaskGroups(logical_pipeline, /*dop=*/1));

  auto task_ctx = test::MakeTestTaskContext();
  ARROW_RETURN_NOT_OK(test::RunTaskGroupsToCompletion(task_groups, task_ctx));

  auto outputs = test::FlattenOutputs(std::move(outputs_by_thread));
  if (outputs.size() != 1) {
    return arrow::Status::Invalid("expected exactly 1 output batch");
  }
  if (outputs[0] == nullptr) {
    return arrow::Status::Invalid("expected non-null output batch");
  }
  return outputs[0];
}

arrow::Status RunPackedMyTimeExtractSmoke() {
  LogicalType type;
  type.id = LogicalTypeId::kMyDateTime;
  type.datetime_fsp = 6;
  ARROW_ASSIGN_OR_RAISE(auto t_field, WithLogicalTypeMetadata(arrow::field("t", arrow::uint64()), type));
  auto schema = arrow::schema({t_field});

  arrow::UInt64Builder t_builder;
  const uint64_t dt0 = PackMyDateTime(2024, 1, 2, 3, 4, 5, 6);
  const uint64_t dt1 = PackMyDateTime(1999, 12, 31, 23, 59, 58, 123456);
  const uint64_t dt2 = PackMyDateTime(0, 0, 0, 0, 0, 0, 0);
  ARROW_RETURN_NOT_OK(t_builder.AppendValues({dt0, dt1, dt2}));
  std::shared_ptr<arrow::Array> t_array;
  ARROW_RETURN_NOT_OK(t_builder.Finish(&t_array));

  auto input = arrow::RecordBatch::Make(schema, /*num_rows=*/3, {t_array});

  std::vector<op::ProjectionExpr> exprs;
  exprs.push_back({"year", MakeCall("toYear", {MakeFieldRef("t")})});
  exprs.push_back({"month", MakeCall("toMonth", {MakeFieldRef("t")})});
  exprs.push_back({"day", MakeCall("toDayOfMonth", {MakeFieldRef("t")})});
  exprs.push_back({"dow", MakeCall("toDayOfWeek", {MakeFieldRef("t")})});
  exprs.push_back({"week", MakeCall("toWeek", {MakeFieldRef("t")})});
  exprs.push_back({"yearweek", MakeCall("toYearWeek", {MakeFieldRef("t")})});
  exprs.push_back({"hour", MakeCall("hour", {MakeFieldRef("t")})});
  exprs.push_back({"minute", MakeCall("minute", {MakeFieldRef("t")})});
  exprs.push_back({"second", MakeCall("second", {MakeFieldRef("t")})});
  exprs.push_back({"micro", MakeCall("microSecond", {MakeFieldRef("t")})});
  exprs.push_back({"date", MakeCall("toMyDate", {MakeFieldRef("t")})});
  exprs.push_back({"date_year",
                   MakeCall("toYear", {MakeCall("toMyDate", {MakeFieldRef("t")})})});
  exprs.push_back({"tidb_dow", MakeCall("tidbDayOfWeek", {MakeFieldRef("t")})});
  exprs.push_back({"tidb_weekofyear", MakeCall("tidbWeekOfYear", {MakeFieldRef("t")})});
  exprs.push_back({"yearweek_func", MakeCall("yearWeek", {MakeFieldRef("t")})});

  ARROW_ASSIGN_OR_RAISE(auto out, RunProjection(std::move(input), std::move(exprs)));
  if (out->num_columns() != 15 || out->num_rows() != 3) {
    return arrow::Status::Invalid("unexpected output shape");
  }

  {
    arrow::UInt16Builder builder;
    ARROW_RETURN_NOT_OK(builder.AppendValues({2024, 1999, 0}));
    std::shared_ptr<arrow::Array> expect;
    ARROW_RETURN_NOT_OK(builder.Finish(&expect));
    if (!expect->Equals(*out->column(0))) {
      return arrow::Status::Invalid("toYear mismatch");
    }
  }
  {
    arrow::UInt8Builder builder;
    ARROW_RETURN_NOT_OK(builder.AppendValues({1, 12, 0}));
    std::shared_ptr<arrow::Array> expect;
    ARROW_RETURN_NOT_OK(builder.Finish(&expect));
    if (!expect->Equals(*out->column(1))) {
      return arrow::Status::Invalid("toMonth mismatch");
    }
  }
  {
    arrow::UInt8Builder builder;
    ARROW_RETURN_NOT_OK(builder.AppendValues({2, 31, 0}));
    std::shared_ptr<arrow::Array> expect;
    ARROW_RETURN_NOT_OK(builder.Finish(&expect));
    if (!expect->Equals(*out->column(2))) {
      return arrow::Status::Invalid("toDayOfMonth mismatch");
    }
  }
  {
    arrow::UInt8Builder builder;
    ARROW_RETURN_NOT_OK(builder.AppendValues({3, 6, 7}));
    std::shared_ptr<arrow::Array> expect;
    ARROW_RETURN_NOT_OK(builder.Finish(&expect));
    if (!expect->Equals(*out->column(3))) {
      return arrow::Status::Invalid("toDayOfWeek mismatch");
    }
  }
  {
    arrow::UInt8Builder builder;
    ARROW_RETURN_NOT_OK(builder.AppendValues({0, 52, 0}));
    std::shared_ptr<arrow::Array> expect;
    ARROW_RETURN_NOT_OK(builder.Finish(&expect));
    if (!expect->Equals(*out->column(4))) {
      return arrow::Status::Invalid("toWeek mismatch");
    }
  }
  {
    arrow::UInt32Builder builder;
    ARROW_RETURN_NOT_OK(builder.AppendValues({202353, 199952, 0}));
    std::shared_ptr<arrow::Array> expect;
    ARROW_RETURN_NOT_OK(builder.Finish(&expect));
    if (!expect->Equals(*out->column(5))) {
      return arrow::Status::Invalid("toYearWeek mismatch");
    }
  }
  {
    arrow::Int64Builder builder;
    ARROW_RETURN_NOT_OK(builder.AppendValues({3, 23, 0}));
    std::shared_ptr<arrow::Array> expect;
    ARROW_RETURN_NOT_OK(builder.Finish(&expect));
    if (!expect->Equals(*out->column(6))) {
      return arrow::Status::Invalid("hour mismatch");
    }
  }
  {
    arrow::Int64Builder builder;
    ARROW_RETURN_NOT_OK(builder.AppendValues({4, 59, 0}));
    std::shared_ptr<arrow::Array> expect;
    ARROW_RETURN_NOT_OK(builder.Finish(&expect));
    if (!expect->Equals(*out->column(7))) {
      return arrow::Status::Invalid("minute mismatch");
    }
  }
  {
    arrow::Int64Builder builder;
    ARROW_RETURN_NOT_OK(builder.AppendValues({5, 58, 0}));
    std::shared_ptr<arrow::Array> expect;
    ARROW_RETURN_NOT_OK(builder.Finish(&expect));
    if (!expect->Equals(*out->column(8))) {
      return arrow::Status::Invalid("second mismatch");
    }
  }
  {
    arrow::Int64Builder builder;
    ARROW_RETURN_NOT_OK(builder.AppendValues({6, 123456, 0}));
    std::shared_ptr<arrow::Array> expect;
    ARROW_RETURN_NOT_OK(builder.Finish(&expect));
    if (!expect->Equals(*out->column(9))) {
      return arrow::Status::Invalid("microSecond mismatch");
    }
  }
  {
    arrow::UInt64Builder builder;
    const uint64_t ymd_mask = ~((uint64_t{1} << 41) - 1);
    ARROW_RETURN_NOT_OK(builder.Append(dt0 & ymd_mask));
    ARROW_RETURN_NOT_OK(builder.Append(dt1 & ymd_mask));
    ARROW_RETURN_NOT_OK(builder.Append(dt2 & ymd_mask));
    std::shared_ptr<arrow::Array> expect;
    ARROW_RETURN_NOT_OK(builder.Finish(&expect));
    if (!expect->Equals(*out->column(10))) {
      return arrow::Status::Invalid("toMyDate mismatch");
    }

    ARROW_ASSIGN_OR_RAISE(const auto logical_type, GetLogicalType(*out->schema()->field(10)));
    if (logical_type.id != LogicalTypeId::kMyDate) {
      return arrow::Status::Invalid("toMyDate output logical type mismatch");
    }
  }
  {
    arrow::UInt16Builder builder;
    ARROW_RETURN_NOT_OK(builder.AppendValues({2024, 1999, 0}));
    std::shared_ptr<arrow::Array> expect;
    ARROW_RETURN_NOT_OK(builder.Finish(&expect));
    if (!expect->Equals(*out->column(11))) {
      return arrow::Status::Invalid("nested toYear(toMyDate) mismatch");
    }
  }
  {
    arrow::UInt16Builder builder;
    ARROW_RETURN_NOT_OK(builder.Append(3));
    ARROW_RETURN_NOT_OK(builder.Append(6));
    ARROW_RETURN_NOT_OK(builder.AppendNull());
    std::shared_ptr<arrow::Array> expect;
    ARROW_RETURN_NOT_OK(builder.Finish(&expect));
    if (!expect->Equals(*out->column(12))) {
      return arrow::Status::Invalid("tidbDayOfWeek mismatch");
    }
  }
  {
    arrow::UInt16Builder builder;
    ARROW_RETURN_NOT_OK(builder.Append(1));
    ARROW_RETURN_NOT_OK(builder.Append(52));
    ARROW_RETURN_NOT_OK(builder.AppendNull());
    std::shared_ptr<arrow::Array> expect;
    ARROW_RETURN_NOT_OK(builder.Finish(&expect));
    if (!expect->Equals(*out->column(13))) {
      return arrow::Status::Invalid("tidbWeekOfYear mismatch");
    }
  }
  {
    arrow::UInt32Builder builder;
    ARROW_RETURN_NOT_OK(builder.Append(202353));
    ARROW_RETURN_NOT_OK(builder.Append(199952));
    ARROW_RETURN_NOT_OK(builder.AppendNull());
    std::shared_ptr<arrow::Array> expect;
    ARROW_RETURN_NOT_OK(builder.Finish(&expect));
    if (!expect->Equals(*out->column(14))) {
      return arrow::Status::Invalid("yearWeek mismatch");
    }
  }

  return arrow::Status::OK();
}

}  // namespace

TEST(TiForthMyTimeTest, PackedExtract) {
  auto status = RunPackedMyTimeExtractSmoke();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

}  // namespace tiforth
