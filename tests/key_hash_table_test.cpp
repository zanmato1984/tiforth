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

#include <arrow/memory_pool.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/testing/gtest_util.h>

#include <array>
#include <cstdint>
#include <limits>

#include <gtest/gtest.h>

#include "tiforth/detail/arena.h"
#include "tiforth/detail/key_hash_table.h"

namespace tiforth::detail {

namespace {

std::array<uint8_t, 8> U64LittleEndian(uint64_t v) {
  std::array<uint8_t, 8> out{};
  out[0] = static_cast<uint8_t>(v);
  out[1] = static_cast<uint8_t>(v >> 8);
  out[2] = static_cast<uint8_t>(v >> 16);
  out[3] = static_cast<uint8_t>(v >> 24);
  out[4] = static_cast<uint8_t>(v >> 32);
  out[5] = static_cast<uint8_t>(v >> 40);
  out[6] = static_cast<uint8_t>(v >> 48);
  out[7] = static_cast<uint8_t>(v >> 56);
  return out;
}

TEST(TiForthKeyHashTableTest, RehashHighCardinality) {
  auto* pool = arrow::default_memory_pool();
  Arena arena(pool);
  KeyHashTable table(pool, &arena);

  constexpr uint32_t kNumKeys = 5000;

  for (uint32_t i = 0; i < kNumKeys; ++i) {
    const auto key_bytes = U64LittleEndian(static_cast<uint64_t>(i));
    const auto hash = HashBytes(key_bytes.data(), static_cast<int32_t>(key_bytes.size()));
    ASSERT_OK_AND_ASSIGN(auto res, table.FindOrInsert(key_bytes.data(),
                                                     static_cast<int32_t>(key_bytes.size()),
                                                     hash, i));
    ASSERT_TRUE(res.second);
    ASSERT_EQ(res.first, i);
  }

  ASSERT_EQ(table.size(), static_cast<int64_t>(kNumKeys));

  for (uint32_t i = 0; i < kNumKeys; ++i) {
    const auto key_bytes = U64LittleEndian(static_cast<uint64_t>(i));
    const auto hash = HashBytes(key_bytes.data(), static_cast<int32_t>(key_bytes.size()));
    ASSERT_OK_AND_ASSIGN(auto found, table.Find(key_bytes.data(),
                                                static_cast<int32_t>(key_bytes.size()), hash));
    ASSERT_TRUE(found.has_value());
    ASSERT_EQ(*found, i);

    ASSERT_OK_AND_ASSIGN(auto res, table.FindOrInsert(key_bytes.data(),
                                                     static_cast<int32_t>(key_bytes.size()),
                                                     hash, /*value=*/123));
    ASSERT_FALSE(res.second);
    ASSERT_EQ(res.first, i);
  }

  {
    const auto missing = U64LittleEndian(static_cast<uint64_t>(kNumKeys + 123));
    const auto hash = HashBytes(missing.data(), static_cast<int32_t>(missing.size()));
    ASSERT_OK_AND_ASSIGN(auto found,
                         table.Find(missing.data(), static_cast<int32_t>(missing.size()), hash));
    ASSERT_FALSE(found.has_value());
  }
}

TEST(TiForthKeyHashTableTest, CollisionHeavyProbing) {
  auto* pool = arrow::default_memory_pool();
  Arena arena(pool);
  KeyHashTable table(pool, &arena);

  constexpr uint32_t kNumKeys = 4096;
  constexpr uint64_t kHash = 0xA5A5A5A5A5A5A5A5ULL;

  for (uint32_t i = 0; i < kNumKeys; ++i) {
    const auto key_bytes = U64LittleEndian(static_cast<uint64_t>(i));
    ASSERT_OK_AND_ASSIGN(auto res,
                         table.FindOrInsert(key_bytes.data(),
                                            static_cast<int32_t>(key_bytes.size()),
                                            kHash, i));
    ASSERT_TRUE(res.second);
    ASSERT_EQ(res.first, i);
  }
  ASSERT_EQ(table.size(), static_cast<int64_t>(kNumKeys));

  for (uint32_t i = 0; i < kNumKeys; ++i) {
    const auto key_bytes = U64LittleEndian(static_cast<uint64_t>(i));
    ASSERT_OK_AND_ASSIGN(auto found,
                         table.Find(key_bytes.data(),
                                    static_cast<int32_t>(key_bytes.size()), kHash));
    ASSERT_TRUE(found.has_value());
    ASSERT_EQ(*found, i);
  }

  {
    const auto missing = U64LittleEndian(0xDEADBEEF);
    ASSERT_OK_AND_ASSIGN(auto found,
                         table.Find(missing.data(), static_cast<int32_t>(missing.size()), kHash));
    ASSERT_FALSE(found.has_value());
  }
}

TEST(TiForthKeyHashTableTest, ZeroLengthKey) {
  auto* pool = arrow::default_memory_pool();
  Arena arena(pool);
  KeyHashTable table(pool, &arena);

  const uint64_t hash = HashBytes(nullptr, /*size=*/0);
  ASSERT_OK_AND_ASSIGN(auto res, table.FindOrInsert(nullptr, /*key_size=*/0, hash, /*value=*/7));
  ASSERT_TRUE(res.second);
  ASSERT_EQ(res.first, 7U);

  ASSERT_OK_AND_ASSIGN(auto again,
                       table.FindOrInsert(nullptr, /*key_size=*/0, hash, /*value=*/9));
  ASSERT_FALSE(again.second);
  ASSERT_EQ(again.first, 7U);

  ASSERT_OK_AND_ASSIGN(auto found, table.Find(nullptr, /*key_size=*/0, hash));
  ASSERT_TRUE(found.has_value());
  ASSERT_EQ(*found, 7U);
}

}  // namespace

}  // namespace tiforth::detail

