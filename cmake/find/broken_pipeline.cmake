# Build broken_pipeline with the schedule runtime enabled.
set(BROKEN_PIPELINE_BUILD_SCHEDULE ON CACHE BOOL "" FORCE)
set(BROKEN_PIPELINE_BUILD_TESTS OFF CACHE BOOL "" FORCE)

include(FetchContent)
FetchContent_Declare(
  broken_pipeline
  URL https://github.com/zanmato1984/broken-pipeline/archive/refs/tags/v0.1.1.tar.gz
  URL_HASH SHA256=08d052fee898ff40d5bd31b97f56db67eeab78619278ce05c3cead2fade71e0d
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)
FetchContent_MakeAvailable(broken_pipeline)

if(NOT TARGET broken_pipeline::broken_pipeline)
  message(FATAL_ERROR "broken_pipeline::broken_pipeline target was not created.")
endif()

if(NOT TARGET broken_pipeline::schedule)
  message(FATAL_ERROR "broken_pipeline::schedule target was not created.")
endif()
