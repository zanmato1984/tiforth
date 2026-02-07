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

#include <memory>
#include <optional>

#include <arrow/result.h>
#include <arrow/status.h>

#include <gtest/gtest.h>

#include "tiforth/engine.h"
#include "tiforth/spill.h"

namespace tiforth {

namespace {

class TestSpillManager final : public SpillManager {
 public:
  arrow::Result<std::optional<SpillHandle>> RequestSpill(int64_t bytes_hint) override {
    ++request_calls;
    last_bytes_hint = bytes_hint;
    return SpillHandle{.id = 42};
  }

  arrow::Status WriteSpill(SpillHandle handle,
                           std::shared_ptr<arrow::RecordBatch> batch) override {
    (void)handle;
    (void)batch;
    return arrow::Status::NotImplemented("test");
  }

  arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> ReadSpill(SpillHandle handle) override {
    (void)handle;
    return arrow::Status::NotImplemented("test");
  }

  int request_calls = 0;
  int64_t last_bytes_hint = 0;
};

arrow::Status RunDefaultDenySmoke() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));
  auto* manager = engine->spill_manager();
  if (manager == nullptr) {
    return arrow::Status::Invalid("expected non-null spill manager");
  }

  ARROW_ASSIGN_OR_RAISE(auto handle, manager->RequestSpill(/*bytes_hint=*/1024));
  if (handle.has_value()) {
    return arrow::Status::Invalid("expected deny spill manager to return no handle");
  }
  return arrow::Status::OK();
}

arrow::Status RunCustomManagerSmoke() {
  auto manager = std::make_shared<TestSpillManager>();
  EngineOptions options;
  options.spill_manager = manager;
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(options));

  if (engine->spill_manager() != manager.get()) {
    return arrow::Status::Invalid("engine did not retain custom spill manager");
  }

  ARROW_ASSIGN_OR_RAISE(auto handle, engine->spill_manager()->RequestSpill(/*bytes_hint=*/7));
  if (!handle.has_value() || handle->id != 42) {
    return arrow::Status::Invalid("unexpected spill handle");
  }
  if (manager->request_calls != 1 || manager->last_bytes_hint != 7) {
    return arrow::Status::Invalid("unexpected spill manager call state");
  }
  return arrow::Status::OK();
}

}  // namespace

TEST(TiForthSpillHooksTest, DefaultDeny) {
  auto status = RunDefaultDenySmoke();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthSpillHooksTest, CustomManagerPlumbed) {
  auto status = RunCustomManagerSmoke();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

}  // namespace tiforth
