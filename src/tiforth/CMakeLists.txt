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

if(TIFORTH_BUILD_TESTS)
  include("${PROJECT_SOURCE_DIR}/cmake/testing.cmake")

  add_tiforth_test(tiforth-smoke-test "${CMAKE_CURRENT_SOURCE_DIR}/smoke_test.cpp")
  add_tiforth_test(tiforth-arrow-array-smoke-test "${CMAKE_CURRENT_SOURCE_DIR}/arrow_array_smoke_test.cpp")
  add_tiforth_test(tiforth-arrow-compute-smoke-test "${CMAKE_CURRENT_SOURCE_DIR}/arrow_compute_smoke_test.cpp")
  add_tiforth_test(tiforth-arrow-compute-agg-test "${CMAKE_CURRENT_SOURCE_DIR}/operators/arrow_compute_agg_test.cpp")
  add_tiforth_test(tiforth-pass-through-reader-test "${CMAKE_CURRENT_SOURCE_DIR}/pass_through_reader_test.cpp")
  add_tiforth_test(tiforth-task-blocked-state-test "${CMAKE_CURRENT_SOURCE_DIR}/task_blocked_state_test.cpp")
  add_tiforth_test(tiforth-filter-test "${CMAKE_CURRENT_SOURCE_DIR}/operators/filter_test.cpp")
  add_tiforth_test(tiforth-hash-agg-test "${CMAKE_CURRENT_SOURCE_DIR}/operators/hash_agg_test.cpp")
  add_tiforth_test(tiforth-parallel-hash-agg-pipeline-test "${CMAKE_CURRENT_SOURCE_DIR}/operators/parallel_hash_agg_pipeline_test.cpp")
  add_tiforth_test(tiforth-hash-join-test "${CMAKE_CURRENT_SOURCE_DIR}/operators/hash_join_test.cpp")
  add_tiforth_test(tiforth-key-hash-table-test "${CMAKE_CURRENT_SOURCE_DIR}/detail/key_hash_table_test.cpp")
  add_tiforth_test(tiforth-fixed-key-hash-table-test "${CMAKE_CURRENT_SOURCE_DIR}/detail/fixed_key_hash_table_test.cpp")
  add_tiforth_test(tiforth-small-string-single-key-grouper-test
                   "${CMAKE_CURRENT_SOURCE_DIR}/detail/small_string_single_key_grouper_test.cpp")
  add_tiforth_test(tiforth-collation-compare-test "${CMAKE_CURRENT_SOURCE_DIR}/functions/collation_compare_test.cpp")
  add_tiforth_test(tiforth-memory-pool-test "${CMAKE_CURRENT_SOURCE_DIR}/memory_pool_test.cpp")
  add_tiforth_test(tiforth-projection-test "${CMAKE_CURRENT_SOURCE_DIR}/operators/projection_test.cpp")
  add_tiforth_test(tiforth-sort-test "${CMAKE_CURRENT_SOURCE_DIR}/operators/sort_test.cpp")
  add_tiforth_test(tiforth-spill-hooks-test "${CMAKE_CURRENT_SOURCE_DIR}/spill_hooks_test.cpp")
  add_tiforth_test(tiforth-type-metadata-test "${CMAKE_CURRENT_SOURCE_DIR}/type_metadata_test.cpp")
  add_tiforth_test(tiforth-plan-breaker-test "${CMAKE_CURRENT_SOURCE_DIR}/plan_breaker_test.cpp")
  add_tiforth_test(tiforth-mytime-temporal-test "${CMAKE_CURRENT_SOURCE_DIR}/functions/mytime_temporal_test.cpp")
  add_tiforth_test(tiforth-decimal-add-test "${CMAKE_CURRENT_SOURCE_DIR}/functions/decimal_add_test.cpp")
  add_tiforth_test(tiforth-decimal-arithmetic-test
                   "${CMAKE_CURRENT_SOURCE_DIR}/functions/decimal_arithmetic_test.cpp")
  add_tiforth_test(tiforth-numeric-arithmetic-test
                   "${CMAKE_CURRENT_SOURCE_DIR}/functions/numeric_arithmetic_test.cpp")
  add_tiforth_test(tiforth-logical-test "${CMAKE_CURRENT_SOURCE_DIR}/functions/logical_test.cpp")
  add_tiforth_test(tiforth-comparison-test "${CMAKE_CURRENT_SOURCE_DIR}/functions/comparison_test.cpp")
endif()
