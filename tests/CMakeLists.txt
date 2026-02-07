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

include("${PROJECT_SOURCE_DIR}/cmake/testing.cmake")

add_tiforth_test(tiforth-smoke-test "${CMAKE_CURRENT_SOURCE_DIR}/smoke_test.cpp")
add_tiforth_test(tiforth-arrow-array-smoke-test "${CMAKE_CURRENT_SOURCE_DIR}/arrow_array_smoke_test.cpp")
add_tiforth_test(tiforth-arrow-compute-smoke-test "${CMAKE_CURRENT_SOURCE_DIR}/arrow_compute_smoke_test.cpp")
add_tiforth_test(tiforth-arrow-compute-agg-test "${CMAKE_CURRENT_SOURCE_DIR}/arrow_compute_agg_test.cpp")
add_tiforth_test(tiforth-pass-through-reader-test "${CMAKE_CURRENT_SOURCE_DIR}/pass_through_reader_test.cpp")
add_tiforth_test(tiforth-task-blocked-state-test "${CMAKE_CURRENT_SOURCE_DIR}/task_blocked_state_test.cpp")
add_tiforth_test(tiforth-filter-test "${CMAKE_CURRENT_SOURCE_DIR}/filter_test.cpp")
add_tiforth_test(tiforth-hash-agg-test "${CMAKE_CURRENT_SOURCE_DIR}/hash_agg_test.cpp")
add_tiforth_test(tiforth-parallel-hash-agg-pipeline-test "${CMAKE_CURRENT_SOURCE_DIR}/parallel_hash_agg_pipeline_test.cpp")
add_tiforth_test(tiforth-hash-join-test "${CMAKE_CURRENT_SOURCE_DIR}/hash_join_test.cpp")
add_tiforth_test(tiforth-key-hash-table-test "${CMAKE_CURRENT_SOURCE_DIR}/key_hash_table_test.cpp")
add_tiforth_test(tiforth-fixed-key-hash-table-test "${CMAKE_CURRENT_SOURCE_DIR}/fixed_key_hash_table_test.cpp")
add_tiforth_test(tiforth-small-string-single-key-grouper-test "${CMAKE_CURRENT_SOURCE_DIR}/small_string_single_key_grouper_test.cpp")
add_tiforth_test(tiforth-collation-compare-test "${CMAKE_CURRENT_SOURCE_DIR}/collation_compare_test.cpp")
add_tiforth_test(tiforth-memory-pool-test "${CMAKE_CURRENT_SOURCE_DIR}/memory_pool_test.cpp")
add_tiforth_test(tiforth-projection-test "${CMAKE_CURRENT_SOURCE_DIR}/projection_test.cpp")
add_tiforth_test(tiforth-sort-test "${CMAKE_CURRENT_SOURCE_DIR}/sort_test.cpp")
add_tiforth_test(tiforth-spill-hooks-test "${CMAKE_CURRENT_SOURCE_DIR}/spill_hooks_test.cpp")
add_tiforth_test(tiforth-type-metadata-test "${CMAKE_CURRENT_SOURCE_DIR}/type_metadata_test.cpp")
add_tiforth_test(tiforth-plan-breaker-test "${CMAKE_CURRENT_SOURCE_DIR}/plan_breaker_test.cpp")
add_tiforth_test(tiforth-mytime-temporal-test "${CMAKE_CURRENT_SOURCE_DIR}/mytime_temporal_test.cpp")
add_tiforth_test(tiforth-decimal-add-test "${CMAKE_CURRENT_SOURCE_DIR}/decimal_add_test.cpp")
add_tiforth_test(tiforth-decimal-arithmetic-test "${CMAKE_CURRENT_SOURCE_DIR}/decimal_arithmetic_test.cpp")
add_tiforth_test(tiforth-numeric-arithmetic-test "${CMAKE_CURRENT_SOURCE_DIR}/numeric_arithmetic_test.cpp")
add_tiforth_test(tiforth-logical-test "${CMAKE_CURRENT_SOURCE_DIR}/logical_test.cpp")
add_tiforth_test(tiforth-comparison-test "${CMAKE_CURRENT_SOURCE_DIR}/comparison_test.cpp")
