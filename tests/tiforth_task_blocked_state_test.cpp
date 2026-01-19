#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>
#include <arrow/testing/gtest_util.h>

#include <gtest/gtest.h>

#include "tiforth/engine.h"
#include "tiforth/operators/pilot.h"
#include "tiforth/pipeline.h"

namespace tiforth {

namespace {

arrow::Result<std::shared_ptr<arrow::RecordBatch>> MakeBatch(
    const std::shared_ptr<arrow::Schema>& schema, const std::vector<int32_t>& values) {
  arrow::Int32Builder builder;
  ARROW_RETURN_NOT_OK(builder.AppendValues(values));
  std::shared_ptr<arrow::Array> array;
  ARROW_RETURN_NOT_OK(builder.Finish(&array));
  return arrow::RecordBatch::Make(schema, static_cast<int64_t>(values.size()), {array});
}

arrow::Status RunPilotIOInTaskAPI() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, PipelineBuilder::Create(engine.get()));

  ARROW_RETURN_NOT_OK(builder->AppendTransform([]() -> arrow::Result<TransformOpPtr> {
    PilotAsyncTransformOptions options;
    options.block_kind = PilotBlockKind::kIOIn;
    options.block_cycles = 1;
    return std::make_unique<PilotAsyncTransformOp>(std::move(options));
  }));

  ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());
  ARROW_ASSIGN_OR_RAISE(auto task, pipeline->CreateTask());

  const auto schema = arrow::schema({arrow::field("x", arrow::int32())});
  ARROW_ASSIGN_OR_RAISE(const auto batch0, MakeBatch(schema, {1, 2}));

  ARROW_RETURN_NOT_OK(task->PushInput(batch0));
  ARROW_RETURN_NOT_OK(task->CloseInput());

  ARROW_ASSIGN_OR_RAISE(auto state, task->Step());
  if (state != TaskState::kIOIn) {
    return arrow::Status::Invalid("expected kIOIn, got different state");
  }

  ARROW_ASSIGN_OR_RAISE(state, task->ExecuteIO());
  if (state != TaskState::kNeedInput) {
    return arrow::Status::Invalid("expected kNeedInput after ExecuteIO");
  }

  ARROW_ASSIGN_OR_RAISE(state, task->Step());
  if (state != TaskState::kHasOutput) {
    return arrow::Status::Invalid("expected kHasOutput after IO completes");
  }

  ARROW_ASSIGN_OR_RAISE(auto out0, task->PullOutput());
  if (out0.get() != batch0.get()) {
    return arrow::Status::Invalid("expected pass-through output batch");
  }

  ARROW_ASSIGN_OR_RAISE(state, task->Step());
  if (state != TaskState::kFinished) {
    return arrow::Status::Invalid("expected task to finish");
  }
  return arrow::Status::OK();
}

arrow::Status RunPilotIOInRecordBatchReader() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, PipelineBuilder::Create(engine.get()));

  ARROW_RETURN_NOT_OK(builder->AppendTransform([]() -> arrow::Result<TransformOpPtr> {
    PilotAsyncTransformOptions options;
    options.block_kind = PilotBlockKind::kIOIn;
    options.block_cycles = 1;
    return std::make_unique<PilotAsyncTransformOp>(std::move(options));
  }));

  ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());

  const auto schema = arrow::schema({arrow::field("x", arrow::int32())});
  ARROW_ASSIGN_OR_RAISE(auto batch0, MakeBatch(schema, {1, 2}));
  ARROW_ASSIGN_OR_RAISE(auto batch1, MakeBatch(schema, {3}));

  ARROW_ASSIGN_OR_RAISE(auto input_reader,
                        arrow::RecordBatchReader::Make({batch0, batch1}, schema));
  ARROW_ASSIGN_OR_RAISE(auto output_reader, pipeline->MakeReader(input_reader));

  std::vector<std::shared_ptr<arrow::RecordBatch>> output_batches;
  while (true) {
    std::shared_ptr<arrow::RecordBatch> out;
    ARROW_RETURN_NOT_OK(output_reader->ReadNext(&out));
    if (out == nullptr) {
      break;
    }
    output_batches.push_back(std::move(out));
  }

  if (output_batches.size() != 2) {
    return arrow::Status::Invalid("expected exactly 2 output batches");
  }
  if (output_batches[0].get() != batch0.get() || output_batches[1].get() != batch1.get()) {
    return arrow::Status::Invalid("expected pass-through output batches");
  }
  return arrow::Status::OK();
}

arrow::Status RunPilotErrorPropagation() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, PipelineBuilder::Create(engine.get()));

  ARROW_RETURN_NOT_OK(builder->AppendTransform([]() -> arrow::Result<TransformOpPtr> {
    PilotAsyncTransformOptions options;
    options.block_kind = PilotBlockKind::kIOIn;
    options.error_point = PilotErrorPoint::kTransform;
    options.error_status = arrow::Status::IOError("pilot injected error");
    return std::make_unique<PilotAsyncTransformOp>(std::move(options));
  }));

  ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());
  ARROW_ASSIGN_OR_RAISE(auto task, pipeline->CreateTask());

  const auto schema = arrow::schema({arrow::field("x", arrow::int32())});
  ARROW_ASSIGN_OR_RAISE(const auto batch0, MakeBatch(schema, {1}));

  ARROW_RETURN_NOT_OK(task->PushInput(batch0));
  ARROW_RETURN_NOT_OK(task->CloseInput());

  auto step_res = task->Step();
  if (step_res.ok()) {
    return arrow::Status::Invalid("expected task Step() to return error");
  }
  if (!step_res.status().IsIOError()) {
    return arrow::Status::Invalid("expected IOError from pilot operator");
  }

  ARROW_ASSIGN_OR_RAISE(auto input_reader, arrow::RecordBatchReader::Make({batch0}, schema));
  ARROW_ASSIGN_OR_RAISE(auto output_reader, pipeline->MakeReader(input_reader));
  std::shared_ptr<arrow::RecordBatch> out;
  auto st = output_reader->ReadNext(&out);
  if (st.ok()) {
    return arrow::Status::Invalid("expected reader ReadNext() to return error");
  }
  if (!st.IsIOError()) {
    return arrow::Status::Invalid("expected IOError from reader");
  }

  return arrow::Status::OK();
}

arrow::Status RunPilotWaitForNotifyTaskAPI() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, PipelineBuilder::Create(engine.get()));

  ARROW_RETURN_NOT_OK(builder->AppendTransform([]() -> arrow::Result<TransformOpPtr> {
    PilotAsyncTransformOptions options;
    options.block_kind = PilotBlockKind::kWaitForNotify;
    return std::make_unique<PilotAsyncTransformOp>(std::move(options));
  }));

  ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());
  ARROW_ASSIGN_OR_RAISE(auto task, pipeline->CreateTask());

  const auto schema = arrow::schema({arrow::field("x", arrow::int32())});
  ARROW_ASSIGN_OR_RAISE(const auto batch0, MakeBatch(schema, {1, 2}));

  ARROW_RETURN_NOT_OK(task->PushInput(batch0));
  ARROW_RETURN_NOT_OK(task->CloseInput());

  ARROW_ASSIGN_OR_RAISE(auto state, task->Step());
  if (state != TaskState::kWaitForNotify) {
    return arrow::Status::Invalid("expected kWaitForNotify");
  }
  ARROW_RETURN_NOT_OK(task->Notify());

  ARROW_ASSIGN_OR_RAISE(state, task->Step());
  if (state != TaskState::kHasOutput) {
    return arrow::Status::Invalid("expected output after notify");
  }
  ARROW_ASSIGN_OR_RAISE(auto out0, task->PullOutput());
  if (out0.get() != batch0.get()) {
    return arrow::Status::Invalid("expected pass-through output batch");
  }
  ARROW_ASSIGN_OR_RAISE(state, task->Step());
  if (state != TaskState::kFinished) {
    return arrow::Status::Invalid("expected finished after notify path");
  }
  return arrow::Status::OK();
}

}  // namespace

TEST(TiForthTaskBlockedStateTest, PilotIOInTaskAPI) { ASSERT_OK(RunPilotIOInTaskAPI()); }

TEST(TiForthTaskBlockedStateTest, PilotIOInRecordBatchReader) {
  ASSERT_OK(RunPilotIOInRecordBatchReader());
}

TEST(TiForthTaskBlockedStateTest, PilotErrorPropagation) {
  ASSERT_OK(RunPilotErrorPropagation());
}

TEST(TiForthTaskBlockedStateTest, PilotWaitForNotifyTaskAPI) {
  ASSERT_OK(RunPilotWaitForNotifyTaskAPI());
}

}  // namespace tiforth
