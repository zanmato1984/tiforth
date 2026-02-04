#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>
#include <arrow/testing/gtest_util.h>

#include <gtest/gtest.h>

#include "tiforth/engine.h"
#include "tiforth/broken_pipeline_traits.h"

#include "test_pipeline_ops.h"
#include "test_task_group_runner.h"

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

arrow::Status RunPassThroughReaderSmoke() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));

  auto schema = arrow::schema({arrow::field("x", arrow::int32())});
  ARROW_ASSIGN_OR_RAISE(auto batch0, MakeBatch(schema, {1, 2}));
  ARROW_ASSIGN_OR_RAISE(auto batch1, MakeBatch(schema, {3, 4, 5}));

  ARROW_ASSIGN_OR_RAISE(auto input_reader, arrow::RecordBatchReader::Make({batch0, batch1}, schema));

  class ReaderSourceOp final : public SourceOp {
   public:
    explicit ReaderSourceOp(std::shared_ptr<arrow::RecordBatchReader> reader)
        : reader_(std::move(reader)) {}

    PipelineSource Source() override {
      return [this](const TaskContext&, ThreadId thread_id) -> OpResult {
        if (thread_id != 0) {
          return arrow::Status::Invalid("ReaderSourceOp only supports thread_id=0");
        }
        if (reader_ == nullptr) {
          return arrow::Status::Invalid("reader must not be null");
        }
        std::shared_ptr<arrow::RecordBatch> batch;
        ARROW_RETURN_NOT_OK(reader_->ReadNext(&batch));
        if (batch == nullptr) {
          return OpOutput::Finished();
        }
        return OpOutput::SourcePipeHasMore(std::move(batch));
      };
    }

    TaskGroups Frontend() override { return {}; }
    std::optional<TaskGroup> Backend() override { return std::nullopt; }

   private:
    std::shared_ptr<arrow::RecordBatchReader> reader_;
  };

  auto source_op = std::make_unique<ReaderSourceOp>(input_reader);

  test::CollectSinkOp::OutputsByThread outputs_by_thread(1);
  auto sink_op = std::make_unique<test::CollectSinkOp>(&outputs_by_thread);

  LogicalPipeline::Channel channel;
  channel.source_op = source_op.get();
  channel.pipe_ops = {};

  LogicalPipeline logical_pipeline{
      "PassThroughReader",
      std::vector<LogicalPipeline::Channel>{std::move(channel)},
      sink_op.get()};

  ARROW_ASSIGN_OR_RAISE(
      auto task_groups,
      test::CompileToTaskGroups(logical_pipeline, /*dop=*/1));

  auto task_ctx = test::MakeTestTaskContext();
  ARROW_RETURN_NOT_OK(test::RunTaskGroupsToCompletion(task_groups, task_ctx));

  auto output_batches = test::FlattenOutputs(std::move(outputs_by_thread));

  if (output_batches.size() != 2) {
    return arrow::Status::Invalid("expected exactly 2 output batches");
  }
  if (output_batches[0].get() != batch0.get() || output_batches[1].get() != batch1.get()) {
    return arrow::Status::Invalid("expected pass-through output batches");
  }

  return arrow::Status::OK();
}

}  // namespace

TEST(TiForthPassThroughReaderTest, RecordBatchReader) {
  auto status = RunPassThroughReaderSmoke();
  ASSERT_OK(status);
}

}  // namespace tiforth
