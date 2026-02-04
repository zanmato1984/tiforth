include_guard(GLOBAL)

# TiForth broken_pipeline integration.
#
# Provides `TIFORTH_BROKEN_PIPELINE_TARGET` (currently `broken_pipeline::broken_pipeline`),
# using (in order):
# - a parent-provided `broken_pipeline::broken_pipeline` or `broken_pipeline` target
# - a system package (`find_package(broken_pipeline CONFIG)`)
# - a pinned FetchContent source tarball

message(STATUS "Finding broken_pipeline")

if(TARGET broken_pipeline::broken_pipeline)
  set(TIFORTH_BROKEN_PIPELINE_TARGET broken_pipeline::broken_pipeline)
  return()
endif()

if(TARGET broken_pipeline)
  add_library(broken_pipeline::broken_pipeline ALIAS broken_pipeline)
  set(TIFORTH_BROKEN_PIPELINE_TARGET broken_pipeline::broken_pipeline)
  return()
endif()

find_package(broken_pipeline CONFIG QUIET)
if(TARGET broken_pipeline::broken_pipeline)
  set(TIFORTH_BROKEN_PIPELINE_TARGET broken_pipeline::broken_pipeline)
  return()
endif()

include(FetchContent)
FetchContent_Declare(
  broken_pipeline
  URL https://github.com/zanmato1984/broken-pipeline/archive/58addff61fcbb1829e073ae89cc93184f53c32f4.tar.gz
  URL_HASH SHA256=d3069df988e5b3a27b0be0d7932e55707fc4208946b7681a931d69c50f658cf8
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)
FetchContent_MakeAvailable(broken_pipeline)

if(TARGET broken_pipeline AND NOT TARGET broken_pipeline::broken_pipeline)
  add_library(broken_pipeline::broken_pipeline ALIAS broken_pipeline)
endif()
set(TIFORTH_BROKEN_PIPELINE_TARGET broken_pipeline::broken_pipeline)
