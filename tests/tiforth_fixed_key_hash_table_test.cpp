#include <arrow/memory_pool.h>
#include <arrow/testing/gtest_util.h>

#include <array>
#include <cstdint>
#include <cstring>

#include <gtest/gtest.h>

#include "tiforth/detail/fixed_key_hash_table.h"
#include "tiforth/detail/key_hash_table.h"  // HashBytes()

namespace tiforth::detail {

namespace {

TEST(TiForthFixedKeyHashTableTest, RehashHighCardinalityU64) {
  auto* pool = arrow::default_memory_pool();
  FixedKeyHashTable<uint64_t, uint32_t> table(pool);

  constexpr uint32_t kNumKeys = 5000;
  for (uint32_t i = 0; i < kNumKeys; ++i) {
    const uint64_t key = static_cast<uint64_t>(i);
    const uint64_t hash =
        HashBytes(reinterpret_cast<const uint8_t*>(&key), static_cast<int32_t>(sizeof(key)));
    ASSERT_OK_AND_ASSIGN(auto res, table.FindOrInsert(key, hash, i));
    ASSERT_TRUE(res.second);
    ASSERT_EQ(res.first, i);
  }
  ASSERT_EQ(table.size(), static_cast<int64_t>(kNumKeys));

  for (uint32_t i = 0; i < kNumKeys; ++i) {
    const uint64_t key = static_cast<uint64_t>(i);
    const uint64_t hash =
        HashBytes(reinterpret_cast<const uint8_t*>(&key), static_cast<int32_t>(sizeof(key)));
    ASSERT_OK_AND_ASSIGN(auto found, table.Find(key, hash));
    ASSERT_TRUE(found.has_value());
    ASSERT_EQ(*found, i);

    ASSERT_OK_AND_ASSIGN(auto again, table.FindOrInsert(key, hash, /*value=*/123));
    ASSERT_FALSE(again.second);
    ASSERT_EQ(again.first, i);
  }
}

TEST(TiForthFixedKeyHashTableTest, CollisionHeavyProbingU64) {
  auto* pool = arrow::default_memory_pool();
  FixedKeyHashTable<uint64_t, uint32_t> table(pool);

  constexpr uint32_t kNumKeys = 4096;
  constexpr uint64_t kHash = 0xA5A5A5A5A5A5A5A5ULL;
  for (uint32_t i = 0; i < kNumKeys; ++i) {
    const uint64_t key = static_cast<uint64_t>(i);
    ASSERT_OK_AND_ASSIGN(auto res, table.FindOrInsert(key, kHash, i));
    ASSERT_TRUE(res.second);
    ASSERT_EQ(res.first, i);
  }
  ASSERT_EQ(table.size(), static_cast<int64_t>(kNumKeys));

  for (uint32_t i = 0; i < kNumKeys; ++i) {
    const uint64_t key = static_cast<uint64_t>(i);
    ASSERT_OK_AND_ASSIGN(auto found, table.Find(key, kHash));
    ASSERT_TRUE(found.has_value());
    ASSERT_EQ(*found, i);
  }
}

TEST(TiForthFixedKeyHashTableTest, FixedBytes16Key) {
  auto* pool = arrow::default_memory_pool();
  using Key = std::array<uint8_t, 16>;
  FixedKeyHashTable<Key, uint32_t> table(pool);

  constexpr uint32_t kNumKeys = 2048;
  for (uint32_t i = 0; i < kNumKeys; ++i) {
    Key key{};
    const uint64_t lo = static_cast<uint64_t>(i);
    const uint64_t hi = static_cast<uint64_t>(i) * 0x9E3779B97F4A7C15ULL;
    std::memcpy(key.data(), &lo, sizeof(lo));
    std::memcpy(key.data() + sizeof(lo), &hi, sizeof(hi));

    const uint64_t hash = HashBytes(key.data(), static_cast<int32_t>(key.size()));
    ASSERT_OK_AND_ASSIGN(auto res, table.FindOrInsert(key, hash, i));
    ASSERT_TRUE(res.second);
    ASSERT_EQ(res.first, i);
  }
  ASSERT_EQ(table.size(), static_cast<int64_t>(kNumKeys));

  for (uint32_t i = 0; i < kNumKeys; ++i) {
    Key key{};
    const uint64_t lo = static_cast<uint64_t>(i);
    const uint64_t hi = static_cast<uint64_t>(i) * 0x9E3779B97F4A7C15ULL;
    std::memcpy(key.data(), &lo, sizeof(lo));
    std::memcpy(key.data() + sizeof(lo), &hi, sizeof(hi));

    const uint64_t hash = HashBytes(key.data(), static_cast<int32_t>(key.size()));
    ASSERT_OK_AND_ASSIGN(auto found, table.Find(key, hash));
    ASSERT_TRUE(found.has_value());
    ASSERT_EQ(*found, i);
  }
}

}  // namespace

}  // namespace tiforth::detail

