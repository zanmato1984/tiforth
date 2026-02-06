#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>
#include <arrow/testing/gtest_util.h>

#include <broken_pipeline/schedule/detail/conditional_awaiter.h>

#include <gtest/gtest.h>

#include "tiforth/engine.h"
#include "tiforth/traits.h"

#include "test_pilot_op.h"
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

arrow::Result<std::vector<std::shared_ptr<arrow::RecordBatch>>> RunPilotPipeline(
    test::PilotAsyncOptions options, std::vector<std::shared_ptr<arrow::RecordBatch>> inputs) {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));
  (void)engine;

  auto source_op = std::make_unique<test::VectorSourceOp>(std::move(inputs));

  std::vector<std::unique_ptr<PipeOp>> pipe_ops;
  pipe_ops.push_back(std::make_unique<test::PilotAsyncPipeOp>(std::move(options)));

  test::CollectSinkOp::OutputsByThread outputs_by_thread(1);
  auto sink_op = std::make_unique<test::CollectSinkOp>(&outputs_by_thread);

  Pipeline::Channel channel;
  channel.source_op = source_op.get();
  channel.pipe_ops.reserve(pipe_ops.size());
  for (auto& op : pipe_ops) {
    channel.pipe_ops.push_back(op.get());
  }

  Pipeline logical_pipeline{
      "Pilot",
      std::vector<Pipeline::Channel>{std::move(channel)},
      sink_op.get()};

  ARROW_ASSIGN_OR_RAISE(
      auto task_groups,
      test::CompileToTaskGroups(logical_pipeline, /*dop=*/1));

  auto task_ctx = test::MakeTestTaskContext();
  ARROW_RETURN_NOT_OK(test::RunTaskGroupsToCompletion(task_groups, task_ctx));

  return test::FlattenOutputs(std::move(outputs_by_thread));
}

arrow::Status RunPilotIOIn() {
  test::PilotAsyncOptions options;
  options.block_kind = test::PilotBlockKind::kIOIn;
  options.block_cycles = 1;

  const auto schema = arrow::schema({arrow::field("x", arrow::int32())});
  ARROW_ASSIGN_OR_RAISE(const auto batch0, MakeBatch(schema, {1, 2}));

  ARROW_ASSIGN_OR_RAISE(auto outputs, RunPilotPipeline(options, {batch0}));
  if (outputs.size() != 1) {
    return arrow::Status::Invalid("expected exactly 1 output batch");
  }
  if (outputs[0].get() != batch0.get()) {
    return arrow::Status::Invalid("expected pass-through output batch");
  }
  return arrow::Status::OK();
}

arrow::Status RunPilotErrorPropagation() {
  test::PilotAsyncOptions options;
  options.block_kind = test::PilotBlockKind::kIOIn;
  options.error_point = test::PilotErrorPoint::kPipe;
  options.error_status = arrow::Status::IOError("pilot injected error");

  const auto schema = arrow::schema({arrow::field("x", arrow::int32())});
  ARROW_ASSIGN_OR_RAISE(const auto batch0, MakeBatch(schema, {1}));

  auto outputs = RunPilotPipeline(options, {batch0});
  if (outputs.ok()) {
    return arrow::Status::Invalid("expected pipeline to return an error status");
  }
  if (!outputs.status().IsIOError()) {
    return arrow::Status::Invalid("expected IOError from pilot operator");
  }
  return arrow::Status::OK();
}

arrow::Status RunPilotWaitForNotify() {
  test::PilotAsyncOptions options;
  options.block_kind = test::PilotBlockKind::kWaitForNotify;

  const auto schema = arrow::schema({arrow::field("x", arrow::int32())});
  ARROW_ASSIGN_OR_RAISE(const auto batch0, MakeBatch(schema, {1, 2}));

  auto source_op = std::make_unique<test::VectorSourceOp>(
      std::vector<std::shared_ptr<arrow::RecordBatch>>{batch0});

  std::vector<std::unique_ptr<PipeOp>> pipe_ops;
  pipe_ops.push_back(std::make_unique<test::PilotAsyncPipeOp>(std::move(options)));

  test::CollectSinkOp::OutputsByThread outputs_by_thread(1);
  auto sink_op = std::make_unique<test::CollectSinkOp>(&outputs_by_thread);

  Pipeline::Channel channel;
  channel.source_op = source_op.get();
  channel.pipe_ops.reserve(pipe_ops.size());
  for (auto& op : pipe_ops) {
    channel.pipe_ops.push_back(op.get());
  }

  Pipeline logical_pipeline{
      "PilotWaitForNotify",
      std::vector<Pipeline::Channel>{std::move(channel)},
      sink_op.get()};

  ARROW_ASSIGN_OR_RAISE(
      auto task_groups,
      test::CompileToTaskGroups(logical_pipeline, /*dop=*/1));

  if (task_groups.size() != 1) {
    return arrow::Status::Invalid("expected exactly 1 task group");
  }

  auto task_ctx = test::MakeTestTaskContext();
  const auto& group = task_groups[0];

  // Step until blocked on WaitForNotify.
  for (int i = 0; i < 8; ++i) {
    ARROW_ASSIGN_OR_RAISE(auto status, group.Task()(task_ctx, /*task_id=*/0));
    if (status.IsBlocked()) {
      auto awaiter =
          std::dynamic_pointer_cast<bp::schedule::detail::ConditionalAwaiter>(
              status.GetAwaiter());
      if (awaiter == nullptr || awaiter->GetResumers().size() != 1 ||
          awaiter->GetResumers()[0] == nullptr) {
        return arrow::Status::Invalid("expected a single blocked resumer");
      }
      auto blocked = std::dynamic_pointer_cast<BlockedResumer>(awaiter->GetResumers()[0]);
      if (blocked == nullptr) {
        return arrow::Status::Invalid("expected BlockedResumer");
      }
      if (blocked->kind() != BlockedKind::kWaitForNotify) {
        return arrow::Status::Invalid("expected WaitForNotify blocked kind");
      }

      // Ensure it stays blocked if Notify is not called.
      ARROW_ASSIGN_OR_RAISE(auto blocked_again, group.Task()(task_ctx, /*task_id=*/0));
      if (!blocked_again.IsBlocked()) {
        return arrow::Status::Invalid("expected blocked again before Notify()");
      }

      // Notify + resume, then run to completion.
      ARROW_RETURN_NOT_OK(blocked->Notify());
      blocked->Resume();
      ARROW_RETURN_NOT_OK(test::RunTaskGroupToCompletion(group, task_ctx));
      break;
    }
    if (status.IsContinue() || status.IsYield()) {
      continue;
    }
    if (status.IsFinished()) {
      return arrow::Status::Invalid("unexpected finished without blocking");
    }
    if (status.IsCancelled()) {
      return arrow::Status::Cancelled("task cancelled");
    }
    return arrow::Status::Invalid("unexpected task status before blocking");
  }

  auto outputs = test::FlattenOutputs(std::move(outputs_by_thread));
  if (outputs.size() != 1) {
    return arrow::Status::Invalid("expected exactly 1 output batch after Notify()");
  }
  if (outputs[0].get() != batch0.get()) {
    return arrow::Status::Invalid("expected pass-through output batch");
  }
  return arrow::Status::OK();
}

}  // namespace

TEST(TiForthTaskBlockedStateTest, PilotErrorPropagation) {
  ASSERT_OK(RunPilotErrorPropagation());
}

TEST(TiForthTaskBlockedStateTest, PilotIOIn) { ASSERT_OK(RunPilotIOIn()); }

TEST(TiForthTaskBlockedStateTest, PilotWaitForNotify) { ASSERT_OK(RunPilotWaitForNotify()); }

}  // namespace tiforth
