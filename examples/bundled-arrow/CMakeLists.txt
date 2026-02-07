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

cmake_minimum_required(VERSION 3.20)

project(bundled-arrow LANGUAGES CXX)

# Always build Apache Arrow from source as a bundled dependency.
set(ARROW_VERSION "23.0.0")

include(FetchContent)

# Adjust the version/URL as needed for your environment.
set(ARROW_DEFINE_OPTIONS ON CACHE BOOL "" FORCE)
set(ARROW_IPC OFF CACHE BOOL "" FORCE)
set(ARROW_MIMALLOC OFF CACHE BOOL "" FORCE)
set(ARROW_COMPUTE ON CACHE BOOL "" FORCE)
set(ARROW_ACERO ON CACHE BOOL "" FORCE)

FetchContent_Declare(
  arrow
  URL https://github.com/apache/arrow/archive/refs/tags/apache-arrow-${ARROW_VERSION}.tar.gz
  SOURCE_SUBDIR cpp
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)
FetchContent_MakeAvailable(arrow)

if (TARGET arrow_shared)
  set(_arrow_build_target arrow_shared)
elseif (TARGET arrow_static)
  set(_arrow_build_target arrow_static)
else()
  message(FATAL_ERROR "Bundled Arrow build did not create arrow_shared or arrow_static targets")
endif()

find_path(ARROW_SOURCE_INCLUDE_DIR
  NAMES arrow/record_batch.h
  HINTS "${arrow_SOURCE_DIR}" "${arrow_SOURCE_DIR}/cpp"
  PATH_SUFFIXES src
  NO_DEFAULT_PATH)
find_path(ARROW_BUILD_INCLUDE_DIR
  NAMES arrow/util/config.h
  HINTS "${arrow_BINARY_DIR}" "${arrow_BINARY_DIR}/cpp"
  PATH_SUFFIXES src
  NO_DEFAULT_PATH)

if (NOT ARROW_SOURCE_INCLUDE_DIR OR NOT ARROW_BUILD_INCLUDE_DIR)
  message(FATAL_ERROR "Failed to locate Arrow include directories in the bundled build.")
endif()

target_include_directories(${_arrow_build_target} PUBLIC
  $<BUILD_INTERFACE:${ARROW_SOURCE_INCLUDE_DIR}>
  $<BUILD_INTERFACE:${ARROW_BUILD_INCLUDE_DIR}>
)
add_library(Arrow::arrow ALIAS ${_arrow_build_target})

if(TARGET arrow_compute_shared)
  add_library(ArrowCompute::arrow_compute ALIAS arrow_compute_shared)
elseif(TARGET arrow_compute_static)
  add_library(ArrowCompute::arrow_compute ALIAS arrow_compute_static)
else()
  message(FATAL_ERROR "Bundled Arrow build did not create arrow_compute_shared or arrow_compute_static targets")
endif()

if(TARGET arrow_acero_shared)
  add_library(ArrowAcero::arrow_acero ALIAS arrow_acero_shared)
elseif(TARGET arrow_acero_static)
  add_library(ArrowAcero::arrow_acero ALIAS arrow_acero_static)
else()
  message(FATAL_ERROR "Bundled Arrow build did not create arrow_acero_shared or arrow_acero_static targets")
endif()

add_subdirectory(${CMAKE_CURRENT_LIST_DIR}/../.. tiforth_build)

set(EXAMPLE_MAIN "${CMAKE_CURRENT_LIST_DIR}/../arrow-common/main.cpp")

add_executable(bundled-arrow ${EXAMPLE_MAIN})
target_compile_features(bundled-arrow PRIVATE cxx_std_20)
target_compile_definitions(bundled-arrow PRIVATE EXAMPLE_PIPELINE_NAME="bundled-arrow")

target_link_libraries(bundled-arrow
  PRIVATE
    tiforth broken_pipeline::schedule Arrow::arrow
)
