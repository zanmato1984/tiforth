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

#include <arrow/type.h>

#include <gtest/gtest.h>

#include "tiforth/type_metadata.h"

namespace tiforth {

TEST(TiForthTypeMetadataTest, RoundTripDecimal) {
  auto field = arrow::field("d", arrow::decimal128(/*precision=*/12, /*scale=*/3));

  LogicalType logical;
  logical.id = LogicalTypeId::kDecimal;
  logical.decimal_precision = 12;
  logical.decimal_scale = 3;

  auto with_meta_result = WithLogicalTypeMetadata(field, logical);
  ASSERT_TRUE(with_meta_result.ok()) << with_meta_result.status().ToString();
  auto with_meta = *with_meta_result;
  ASSERT_NE(with_meta, nullptr);

  auto parsed_result = GetLogicalType(*with_meta);
  ASSERT_TRUE(parsed_result.ok()) << parsed_result.status().ToString();
  auto parsed = *parsed_result;
  EXPECT_EQ(parsed.id, LogicalTypeId::kDecimal);
  EXPECT_EQ(parsed.decimal_precision, 12);
  EXPECT_EQ(parsed.decimal_scale, 3);
}

TEST(TiForthTypeMetadataTest, RoundTripMyDateTime) {
  auto field = arrow::field("t", arrow::uint64());

  LogicalType logical;
  logical.id = LogicalTypeId::kMyDateTime;
  logical.datetime_fsp = 6;

  auto with_meta_result = WithLogicalTypeMetadata(field, logical);
  ASSERT_TRUE(with_meta_result.ok()) << with_meta_result.status().ToString();
  auto with_meta = *with_meta_result;
  ASSERT_NE(with_meta, nullptr);

  auto parsed_result = GetLogicalType(*with_meta);
  ASSERT_TRUE(parsed_result.ok()) << parsed_result.status().ToString();
  auto parsed = *parsed_result;
  EXPECT_EQ(parsed.id, LogicalTypeId::kMyDateTime);
  EXPECT_EQ(parsed.datetime_fsp, 6);
}

TEST(TiForthTypeMetadataTest, RoundTripStringCollation) {
  auto field = arrow::field("s", arrow::binary());

  LogicalType logical;
  logical.id = LogicalTypeId::kString;
  logical.collation_id = 63;

  auto with_meta_result = WithLogicalTypeMetadata(field, logical);
  ASSERT_TRUE(with_meta_result.ok()) << with_meta_result.status().ToString();
  auto with_meta = *with_meta_result;
  ASSERT_NE(with_meta, nullptr);

  auto parsed_result = GetLogicalType(*with_meta);
  ASSERT_TRUE(parsed_result.ok()) << parsed_result.status().ToString();
  auto parsed = *parsed_result;
  EXPECT_EQ(parsed.id, LogicalTypeId::kString);
  EXPECT_EQ(parsed.collation_id, 63);
}

}  // namespace tiforth
