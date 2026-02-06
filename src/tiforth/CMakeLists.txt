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
