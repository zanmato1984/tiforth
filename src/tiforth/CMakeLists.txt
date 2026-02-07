# Copyright 2026 TiForth Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_library(tiforth
  collation.cc
  compiled_expr.cc
  detail/arrow_compute.cc
  detail/collation_single_key_grouper.cc
  detail/expr_compiler.cc
  detail/small_string_single_key_grouper.cc
  detail/tidb_collation_lut.cc
  engine.cc
  expr.cc
  functions/hash_aggregate_count.cc
  functions/hash_aggregate_mean.cc
  functions/hash_aggregate_minmax.cc
  functions/hash_aggregate_register.cc
  functions/hash_aggregate_sum.cc
  functions/register.cc
  functions/scalar_arithmetic_decimal_arithmetic.cc
  functions/scalar_arithmetic_numeric_arithmetic.cc
  functions/scalar_arithmetic_register.cc
  functions/scalar_collated_compare.cc
  functions/scalar_logical.cc
  functions/scalar_mytime.cc
  operators/arrow_compute_agg.cc
  operators/filter.cc
  operators/hash_agg.cc
  operators/hash_join.cc
  operators/projection.cc
  operators/sort.cc
  type_metadata.cc)
target_compile_features(tiforth PUBLIC cxx_std_20)

target_include_directories(
  tiforth
  PUBLIC $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include> $<INSTALL_INTERFACE:include>)
target_include_directories(tiforth PRIVATE $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src>)

target_link_libraries(tiforth PUBLIC Arrow::arrow ArrowCompute::arrow_compute ArrowAcero::arrow_acero)
target_link_libraries(tiforth PUBLIC broken_pipeline::broken_pipeline)
