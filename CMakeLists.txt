cmake_minimum_required(VERSION 3.23)

project(tiforth VERSION 0.0.1 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(TIFORTH_IS_TOP_LEVEL OFF)
if(PROJECT_IS_TOP_LEVEL)
  set(TIFORTH_IS_TOP_LEVEL ON)
endif()

set(TIFORTH_ARROW_PROVIDER "bundled" CACHE STRING "Arrow provider (bundled|system)")
set_property(CACHE TIFORTH_ARROW_PROVIDER PROPERTY STRINGS bundled system)

option(TIFORTH_BUILD_TESTS "Build TiForth tests (top-level only)" OFF)
if(NOT TIFORTH_IS_TOP_LEVEL AND TIFORTH_BUILD_TESTS)
  message(FATAL_ERROR "TIFORTH_BUILD_TESTS must be OFF when tiforth is included as a subproject")
endif()
if(TIFORTH_BUILD_TESTS AND NOT TIFORTH_ARROW_PROVIDER STREQUAL "bundled")
  message(FATAL_ERROR "TIFORTH_BUILD_TESTS requires TIFORTH_ARROW_PROVIDER=bundled (Arrow testing utilities are bundled-only)")
endif()

include(cmake/find/arrow.cmake)
include(cmake/find/broken_pipeline.cmake)

add_subdirectory(src/tiforth)

if(TIFORTH_BUILD_TESTS)
  add_subdirectory(tests)
endif()

include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/install.cmake")
