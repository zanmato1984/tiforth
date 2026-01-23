#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string_view>

#include <arrow/buffer_builder.h>
#include <arrow/memory_pool.h>
#include <arrow/status.h>

namespace tiforth::detail {

// Reusable byte buffer backed by an Arrow MemoryPool.
//
// This is used for "scratch" key encoding (hash agg / hash join) to avoid
// allocating from the default heap on hot paths.
class ScratchBytes {
 public:
  explicit ScratchBytes(arrow::MemoryPool* pool = nullptr)
      : builder_(pool != nullptr ? pool : arrow::default_memory_pool()) {}

  ScratchBytes(const ScratchBytes&) = delete;
  ScratchBytes& operator=(const ScratchBytes&) = delete;
  ScratchBytes(ScratchBytes&&) = default;
  ScratchBytes& operator=(ScratchBytes&&) = default;

  void Reset() {
    status_ = arrow::Status::OK();
    builder_.Rewind(0);
  }

  // Methods required by tiforth::SortKeyStringTo templates.
  void clear() { builder_.Rewind(0); }

  void reserve(std::size_t desired_capacity) {
    if (!status_.ok()) {
      return;
    }
    if (desired_capacity >
        static_cast<std::size_t>(std::numeric_limits<int64_t>::max())) {
      status_ = arrow::Status::Invalid("scratch reserve overflow");
      return;
    }
    const int64_t desired = static_cast<int64_t>(desired_capacity);
    const int64_t additional = std::max<int64_t>(0, desired - builder_.length());
    status_ = builder_.Reserve(additional);
  }

  void push_back(char c) {
    if (!status_.ok()) {
      return;
    }
    status_ = builder_.Append(/*num_copies=*/1, static_cast<uint8_t>(c));
  }

  void append(const char* data, std::size_t size) {
    if (!status_.ok()) {
      return;
    }
    if (size == 0) {
      return;
    }
    if (data == nullptr) {
      status_ = arrow::Status::Invalid("scratch append data must not be null for non-zero size");
      return;
    }
    if (size > static_cast<std::size_t>(std::numeric_limits<int64_t>::max())) {
      status_ = arrow::Status::Invalid("scratch append overflow");
      return;
    }
    status_ = builder_.Append(data, static_cast<int64_t>(size));
  }

  void append(std::string_view v) { append(v.data(), v.size()); }

  void assign(const char* data, std::size_t size) {
    clear();
    append(data, size);
  }

  void assign(std::string_view v) { assign(v.data(), v.size()); }

  const uint8_t* data() const { return builder_.data(); }
  int64_t size() const { return builder_.length(); }

  std::string_view view() const {
    const auto len = builder_.length();
    if (len <= 0) {
      return std::string_view();
    }
    return std::string_view(reinterpret_cast<const char*>(builder_.data()),
                            static_cast<std::size_t>(len));
  }

  const arrow::Status& status() const { return status_; }

 private:
  arrow::BufferBuilder builder_;
  arrow::Status status_ = arrow::Status::OK();
};

}  // namespace tiforth::detail
