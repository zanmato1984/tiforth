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

if(NOT PROJECT_SOURCE_DIR STREQUAL CMAKE_SOURCE_DIR)
  if (TARGET Arrow::arrow AND TARGET ArrowCompute::arrow_compute AND TARGET ArrowAcero::arrow_acero)
    return()
  else()
    message(FATAL_ERROR "Arrow::arrow and ArrowCompute::arrow_compute and ArrowAcero::arrow_acero targets must be provided before enabling tiforth")
  endif()
endif()

set(_tiforth_allowed_arrow_providers bundled system)
if(NOT TIFORTH_ARROW_PROVIDER IN_LIST _tiforth_allowed_arrow_providers)
  message(
    FATAL_ERROR
    "Invalid TIFORTH_ARROW_PROVIDER='${TIFORTH_ARROW_PROVIDER}'. Allowed: bundled|system")
endif()

if(TIFORTH_ARROW_PROVIDER STREQUAL "bundled")
  # Always build Apache Arrow from source as a bundled dependency.
  set(ARROW_VERSION "23.0.0")

  include(FetchContent)

  # Adjust the version/URL as needed for your environment.
  set(ARROW_DEFINE_OPTIONS ON CACHE BOOL "" FORCE)
  set(ARROW_IPC OFF CACHE BOOL "" FORCE)
  set(ARROW_MIMALLOC OFF CACHE BOOL "" FORCE)
  set(ARROW_COMPUTE ON CACHE BOOL "" FORCE)
  set(ARROW_ACERO ON CACHE BOOL "" FORCE)
  if(TIFORTH_BUILD_TESTS)
    set(ARROW_TESTING ON CACHE BOOL "" FORCE)
  else()
    set(ARROW_TESTING OFF CACHE BOOL "" FORCE)
  endif()

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

  if(TIFORTH_BUILD_TESTS)
    if(TARGET arrow_testing_shared)
      add_library(ArrowTesting::arrow_testing ALIAS arrow_testing_shared)
    elseif(TARGET arrow_testing_static)
      add_library(ArrowTesting::arrow_testing ALIAS arrow_testing_static)
    else()
      message(FATAL_ERROR "Bundled Arrow build did not create arrow_testing_shared or arrow_testing_static targets")
    endif()
  endif()
else()
  find_package(Arrow CONFIG REQUIRED)

  if (TARGET Arrow::arrow)
    return()
  endif()

  find_package(Arrow CONFIG REQUIRED)
  if (TARGET Arrow::arrow_shared)
    add_library(Arrow::arrow ALIAS Arrow::arrow_shared)
  elseif (TARGET Arrow::arrow_static)
    add_library(Arrow::arrow ALIAS Arrow::arrow_static)
  else()
    message(FATAL_ERROR "Arrow package did not create arrow_shared or arrow_static targets")
  endif()

  find_package(ArrowCompute CONFIG REQUIRED)
  if(TARGET ArrowCompute::arrow_compute_shared)
    add_library(ArrowCompute::arrow_compute ALIAS ArrowCompute::arrow_compute_shared)
  elseif(TARGET ArrowCompute::arrow_compute_static)
    add_library(ArrowCompute::arrow_compute ALIAS ArrowCompute::arrow_compute_static)
  else()
    message(FATAL_ERROR "ArrowCompute package did not create arrow_compute_shared or arrow_compute_static targets")
  endif()

  find_package(ArrowAcero CONFIG REQUIRED)
  if(TARGET ArrowAcero::arrow_acero_shared)
    add_library(ArrowAcero::arrow_acero ALIAS ArrowAcero::arrow_acero_shared)
  elseif(TARGET ArrowAcero::arrow_acero_static)
    add_library(ArrowAcero::arrow_acero ALIAS ArrowAcero::arrow_acero_static)
  else()
    message(FATAL_ERROR "ArrowAcero package did not create arrow_acero_shared or arrow_acero_static targets")
  endif()
endif()
