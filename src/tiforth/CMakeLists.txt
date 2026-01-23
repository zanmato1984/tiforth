if(NOT TIFORTH_ARROW_CORE_TARGET)
  message(FATAL_ERROR "TiForth Arrow core target is not configured")
endif()

if(NOT TIFORTH_ARROW_COMPUTE_TARGET)
  message(FATAL_ERROR "TiForth Arrow compute target is not configured")
endif()

if(NOT TIFORTH_ARROW_ACERO_TARGET)
  message(FATAL_ERROR "TiForth Arrow Acero target is not configured")
endif()

if(TIFORTH_LIBRARY_LINKAGE STREQUAL "shared")
  add_library(tiforth SHARED)
else()
  add_library(tiforth STATIC)
  set_target_properties(tiforth PROPERTIES POSITION_INDEPENDENT_CODE ON)
endif()
add_library(tiforth::tiforth ALIAS tiforth)

target_sources(tiforth PRIVATE collation.cc compiled_expr.cc detail/arrow_compute.cc detail/collation_single_key_grouper.cc detail/expr_compiler.cc detail/small_string_single_key_grouper.cc detail/tidb_collation_lut.cc engine.cc expr.cc functions/hash_aggregate.cc functions/register.cc functions/scalar/arithmetic/register.cc functions/scalar/arithmetic/decimal_arithmetic.cc functions/scalar/arithmetic/numeric_arithmetic.cc functions/scalar/comparison/collated_compare.cc functions/scalar/logical/logical.cc functions/scalar/temporal/mytime.cc pipeline/physical_pipeline.cc pipeline/pipeline_task.cc pipeline/task_groups.cc operators/arrow_compute_agg.cc operators/filter.cc operators/hash_agg.cc operators/hash_join.cc operators/projection.cc operators/sort.cc type_metadata.cc)
target_compile_features(tiforth PUBLIC cxx_std_20)

target_include_directories(
  tiforth
  PUBLIC $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include> $<INSTALL_INTERFACE:include>)
target_include_directories(tiforth PRIVATE $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src>)

if(TIFORTH_ARROW_INCLUDE_DIRS)
  foreach(_tiforth_arrow_inc_dir IN LISTS TIFORTH_ARROW_INCLUDE_DIRS)
    target_include_directories(tiforth SYSTEM PUBLIC "$<BUILD_INTERFACE:${_tiforth_arrow_inc_dir}>")
  endforeach()
endif()

target_link_libraries(tiforth PUBLIC ${TIFORTH_ARROW_CORE_TARGET})
target_link_libraries(tiforth PUBLIC ${TIFORTH_ARROW_COMPUTE_TARGET})
target_link_libraries(tiforth PUBLIC ${TIFORTH_ARROW_ACERO_TARGET})

if(TIFORTH_LIBRARY_LINKAGE STREQUAL "shared")
  set_target_properties(
    tiforth
    PROPERTIES VERSION ${PROJECT_VERSION} SOVERSION ${PROJECT_VERSION_MAJOR})
endif()
