#pragma once

#include <cstddef>
#include <cstdint>
#include <memory_resource>
#include <new>

#include <arrow/memory_pool.h>

namespace tiforth::detail {

// std::pmr::memory_resource adapter backed by an Arrow MemoryPool.
//
// This is used for PMR containers where we want allocations to be tracked by
// the host-provided Arrow pool. Allocation failures throw std::bad_alloc
// (std::pmr contract).
class ArrowMemoryPoolResource final : public std::pmr::memory_resource {
 public:
  explicit ArrowMemoryPoolResource(arrow::MemoryPool* pool)
      : pool_(pool != nullptr ? pool : arrow::default_memory_pool()) {}

 private:
  void* do_allocate(std::size_t bytes, std::size_t /*alignment*/) override {
    uint8_t* out = nullptr;
    const auto st = pool_->Allocate(static_cast<int64_t>(bytes), &out);
    if (!st.ok()) {
      throw std::bad_alloc();
    }
    return out;
  }

  void do_deallocate(void* p, std::size_t bytes, std::size_t /*alignment*/) override {
    if (p == nullptr) {
      return;
    }
    pool_->Free(reinterpret_cast<uint8_t*>(p), static_cast<int64_t>(bytes));
  }

  bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override {
    return this == &other;
  }

  arrow::MemoryPool* pool_ = nullptr;
};

}  // namespace tiforth::detail

