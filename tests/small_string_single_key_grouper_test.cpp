#include <arrow/array/array_binary.h>
#include <arrow/array/array_primitive.h>
#include <arrow/builder.h>
#include <arrow/compute/exec.h>
#include <arrow/memory_pool.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/testing/gtest_util.h>
#include <arrow/type.h>

#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "tiforth/detail/small_string_single_key_grouper.h"

namespace tiforth::detail {

namespace {

arrow::Result<std::shared_ptr<arrow::Array>> MakeBinaryArray(
    const std::vector<std::optional<std::string>>& values) {
  arrow::BinaryBuilder builder;
  ARROW_RETURN_NOT_OK(builder.Reserve(static_cast<int64_t>(values.size())));
  for (const auto& v : values) {
    if (!v.has_value()) {
      ARROW_RETURN_NOT_OK(builder.AppendNull());
      continue;
    }
    ARROW_RETURN_NOT_OK(
        builder.Append(reinterpret_cast<const uint8_t*>(v->data()), static_cast<int32_t>(v->size())));
  }
  std::shared_ptr<arrow::Array> out;
  ARROW_RETURN_NOT_OK(builder.Finish(&out));
  return out;
}

arrow::Result<std::shared_ptr<arrow::Array>> MakeStringArray(
    const std::vector<std::optional<std::string>>& values) {
  arrow::StringBuilder builder;
  ARROW_RETURN_NOT_OK(builder.Reserve(static_cast<int64_t>(values.size())));
  for (const auto& v : values) {
    if (!v.has_value()) {
      ARROW_RETURN_NOT_OK(builder.AppendNull());
      continue;
    }
    ARROW_RETURN_NOT_OK(builder.Append(*v));
  }
  std::shared_ptr<arrow::Array> out;
  ARROW_RETURN_NOT_OK(builder.Finish(&out));
  return out;
}

TEST(TiForthSmallStringSingleKeyGrouperTest, ConsumeAndGetUniquesBinary) {
  arrow::compute::ExecContext ctx(arrow::default_memory_pool());
  SmallStringSingleKeyGrouper grouper(arrow::binary(), &ctx);

  ASSERT_OK_AND_ASSIGN(auto keys, MakeBinaryArray({"a", "b", "a", std::nullopt, "b", std::nullopt, ""}));
  const auto batch = arrow::compute::ExecBatch({arrow::Datum(keys)}, keys->length());
  const arrow::compute::ExecSpan span(batch);

  ASSERT_OK_AND_ASSIGN(auto ids_datum, grouper.Consume(span));
  ASSERT_TRUE(ids_datum.is_array());
  auto ids_arr = std::static_pointer_cast<arrow::UInt32Array>(ids_datum.make_array());
  ASSERT_EQ(ids_arr->length(), 7);
  ASSERT_TRUE(ids_arr->null_count() == 0);
  ASSERT_EQ(ids_arr->Value(0), 0U);
  ASSERT_EQ(ids_arr->Value(1), 1U);
  ASSERT_EQ(ids_arr->Value(2), 0U);
  ASSERT_EQ(ids_arr->Value(3), 2U);
  ASSERT_EQ(ids_arr->Value(4), 1U);
  ASSERT_EQ(ids_arr->Value(5), 2U);
  ASSERT_EQ(ids_arr->Value(6), 3U);

  ASSERT_EQ(grouper.num_groups(), 4U);

  ASSERT_OK_AND_ASSIGN(auto uniques, grouper.GetUniques());
  ASSERT_EQ(uniques.length, 4);
  ASSERT_EQ(uniques.num_values(), 1);
  ASSERT_TRUE(uniques.values[0].is_array());
  auto out_keys = uniques.values[0].make_array();
  auto out_bin = std::static_pointer_cast<arrow::BinaryArray>(out_keys);
  ASSERT_EQ(out_bin->length(), 4);
  ASSERT_EQ(out_bin->null_count(), 1);
  ASSERT_EQ(out_bin->GetView(0), "a");
  ASSERT_EQ(out_bin->GetView(1), "b");
  ASSERT_TRUE(out_bin->IsNull(2));
  ASSERT_EQ(out_bin->GetView(3), "");
}

TEST(TiForthSmallStringSingleKeyGrouperTest, ConsumeMultipleBatchesAndLookup) {
  arrow::compute::ExecContext ctx(arrow::default_memory_pool());
  SmallStringSingleKeyGrouper grouper(arrow::binary(), &ctx);

  ASSERT_OK_AND_ASSIGN(auto batch0_keys, MakeBinaryArray({"a", "b", std::nullopt}));
  {
    const auto batch = arrow::compute::ExecBatch({arrow::Datum(batch0_keys)}, batch0_keys->length());
    const arrow::compute::ExecSpan span(batch);
    ASSERT_OK_AND_ASSIGN(auto ids_datum, grouper.Consume(span));
    auto ids = std::static_pointer_cast<arrow::UInt32Array>(ids_datum.make_array());
    ASSERT_EQ(ids->Value(0), 0U);
    ASSERT_EQ(ids->Value(1), 1U);
    ASSERT_EQ(ids->Value(2), 2U);
    ASSERT_EQ(grouper.num_groups(), 3U);
  }

  ASSERT_OK_AND_ASSIGN(auto batch1_keys, MakeBinaryArray({"b", "c", std::nullopt}));
  {
    const auto batch = arrow::compute::ExecBatch({arrow::Datum(batch1_keys)}, batch1_keys->length());
    const arrow::compute::ExecSpan span(batch);
    ASSERT_OK_AND_ASSIGN(auto ids_datum, grouper.Consume(span));
    auto ids = std::static_pointer_cast<arrow::UInt32Array>(ids_datum.make_array());
    ASSERT_EQ(ids->Value(0), 1U);
    ASSERT_EQ(ids->Value(1), 3U);
    ASSERT_EQ(ids->Value(2), 2U);
    ASSERT_EQ(grouper.num_groups(), 4U);
  }

  ASSERT_OK_AND_ASSIGN(auto lookup_keys, MakeBinaryArray({"a", "d", std::nullopt}));
  {
    const auto batch = arrow::compute::ExecBatch({arrow::Datum(lookup_keys)}, lookup_keys->length());
    const arrow::compute::ExecSpan span(batch);
    ASSERT_OK_AND_ASSIGN(auto ids_datum, grouper.Lookup(span));
    auto ids = std::static_pointer_cast<arrow::UInt32Array>(ids_datum.make_array());
    ASSERT_EQ(ids->length(), 3);
    ASSERT_EQ(ids->null_count(), 1);
    ASSERT_EQ(ids->Value(0), 0U);
    ASSERT_TRUE(ids->IsNull(1));
    ASSERT_EQ(ids->Value(2), 2U);
  }
}

TEST(TiForthSmallStringSingleKeyGrouperTest, ResetClearsGroups) {
  arrow::compute::ExecContext ctx(arrow::default_memory_pool());
  SmallStringSingleKeyGrouper grouper(arrow::binary(), &ctx);

  ASSERT_OK_AND_ASSIGN(auto keys, MakeBinaryArray({"x", "y"}));
  {
    const auto batch = arrow::compute::ExecBatch({arrow::Datum(keys)}, keys->length());
    const arrow::compute::ExecSpan span(batch);
    ASSERT_OK(grouper.Populate(span));
  }
  ASSERT_EQ(grouper.num_groups(), 2U);

  ASSERT_OK(grouper.Reset());
  ASSERT_EQ(grouper.num_groups(), 0U);

  ASSERT_OK_AND_ASSIGN(auto lookup_keys, MakeBinaryArray({"x"}));
  const auto batch = arrow::compute::ExecBatch({arrow::Datum(lookup_keys)}, lookup_keys->length());
  const arrow::compute::ExecSpan span(batch);
  ASSERT_OK_AND_ASSIGN(auto ids_datum, grouper.Lookup(span));
  auto ids = std::static_pointer_cast<arrow::UInt32Array>(ids_datum.make_array());
  ASSERT_EQ(ids->length(), 1);
  ASSERT_TRUE(ids->IsNull(0));
}

TEST(TiForthSmallStringSingleKeyGrouperTest, StringType) {
  arrow::compute::ExecContext ctx(arrow::default_memory_pool());
  SmallStringSingleKeyGrouper grouper(arrow::utf8(), &ctx);

  ASSERT_OK_AND_ASSIGN(auto keys, MakeStringArray({"hi", std::nullopt, "hi", "z"}));
  const auto batch = arrow::compute::ExecBatch({arrow::Datum(keys)}, keys->length());
  const arrow::compute::ExecSpan span(batch);

  ASSERT_OK_AND_ASSIGN(auto ids_datum, grouper.Consume(span));
  auto ids = std::static_pointer_cast<arrow::UInt32Array>(ids_datum.make_array());
  ASSERT_EQ(ids->Value(0), 0U);
  ASSERT_EQ(ids->Value(1), 1U);
  ASSERT_EQ(ids->Value(2), 0U);
  ASSERT_EQ(ids->Value(3), 2U);

  ASSERT_OK_AND_ASSIGN(auto uniques, grouper.GetUniques());
  ASSERT_EQ(uniques.length, 3);
  auto out_keys = uniques.values[0].make_array();
  auto out_str = std::static_pointer_cast<arrow::StringArray>(out_keys);
  ASSERT_EQ(out_str->GetView(0), "hi");
  ASSERT_TRUE(out_str->IsNull(1));
  ASSERT_EQ(out_str->GetView(2), "z");
}

}  // namespace

}  // namespace tiforth::detail
