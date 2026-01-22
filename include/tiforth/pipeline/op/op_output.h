#pragma once

#include <arrow/util/logging.h>

#include <optional>
#include <string>
#include <utility>
#include <variant>

#include "tiforth/pipeline/defines.h"
#include "tiforth/task/resumer.h"

namespace tiforth::pipeline {

class OpOutput {
 private:
  enum class Code {
    kPipeSinkNeedsMore,
    kPipeEven,
    kSourcePipeHasMore,
    kBlocked,
    kPipeYield,
    kPipeYieldBack,
    kFinished,
    kCancelled,
  };

 public:
  bool IsPipeSinkNeedsMore() const { return code_ == Code::kPipeSinkNeedsMore; }
  bool IsPipeEven() const { return code_ == Code::kPipeEven; }
  bool IsSourcePipeHasMore() const { return code_ == Code::kSourcePipeHasMore; }
  bool IsBlocked() const { return code_ == Code::kBlocked; }
  bool IsPipeYield() const { return code_ == Code::kPipeYield; }
  bool IsPipeYieldBack() const { return code_ == Code::kPipeYieldBack; }
  bool IsFinished() const { return code_ == Code::kFinished; }
  bool IsCancelled() const { return code_ == Code::kCancelled; }

  std::optional<Batch>& GetBatch() {
    ARROW_CHECK(IsPipeEven() || IsSourcePipeHasMore() || IsFinished());
    return std::get<std::optional<Batch>>(payload_);
  }

  const std::optional<Batch>& GetBatch() const {
    ARROW_CHECK(IsPipeEven() || IsSourcePipeHasMore() || IsFinished());
    return std::get<std::optional<Batch>>(payload_);
  }

  task::ResumerPtr& GetResumer() {
    ARROW_CHECK(IsBlocked());
    return std::get<task::ResumerPtr>(payload_);
  }

  const task::ResumerPtr& GetResumer() const {
    ARROW_CHECK(IsBlocked());
    return std::get<task::ResumerPtr>(payload_);
  }

  bool operator==(const OpOutput& other) const { return code_ == other.code_; }

  std::string ToString() const {
    switch (code_) {
      case Code::kPipeSinkNeedsMore:
        return "PIPE_SINK_NEEDS_MORE";
      case Code::kPipeEven:
        return "PIPE_EVEN";
      case Code::kSourcePipeHasMore:
        return "SOURCE_PIPE_HAS_MORE";
      case Code::kBlocked:
        return "BLOCKED";
      case Code::kPipeYield:
        return "PIPE_YIELD";
      case Code::kPipeYieldBack:
        return "PIPE_YIELD_BACK";
      case Code::kFinished:
        return "FINISHED";
      case Code::kCancelled:
        return "CANCELLED";
    }
    return "UNKNOWN";
  }

  static OpOutput PipeSinkNeedsMore() { return OpOutput(Code::kPipeSinkNeedsMore); }
  static OpOutput PipeEven(Batch batch) { return OpOutput(Code::kPipeEven, std::move(batch)); }
  static OpOutput SourcePipeHasMore(Batch batch) {
    return OpOutput(Code::kSourcePipeHasMore, std::move(batch));
  }
  static OpOutput Blocked(task::ResumerPtr resumer) { return OpOutput(std::move(resumer)); }
  static OpOutput PipeYield() { return OpOutput(Code::kPipeYield); }
  static OpOutput PipeYieldBack() { return OpOutput(Code::kPipeYieldBack); }
  static OpOutput Finished(std::optional<Batch> batch = std::nullopt) {
    return OpOutput(Code::kFinished, std::move(batch));
  }
  static OpOutput Cancelled() { return OpOutput(Code::kCancelled); }

 private:
  explicit OpOutput(Code code, std::optional<Batch> batch = std::nullopt)
      : code_(code), payload_(std::move(batch)) {}

  explicit OpOutput(task::ResumerPtr resumer)
      : code_(Code::kBlocked), payload_(std::move(resumer)) {}

  Code code_ = Code::kPipeSinkNeedsMore;
  std::variant<task::ResumerPtr, std::optional<Batch>> payload_;
};

}  // namespace tiforth::pipeline

