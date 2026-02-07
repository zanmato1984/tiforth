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

project(system-arrow LANGUAGES CXX)

find_package(Arrow CONFIG REQUIRED)
find_package(ArrowCompute CONFIG REQUIRED)
find_package(ArrowAcero CONFIG REQUIRED)

if (TARGET Arrow::arrow)
  message(FATAL_ERROR "Arrow::arrow is already defined.")
endif()
if (TARGET ArrowCompute::arrow_compute)
  message(FATAL_ERROR "ArrowCompute::arrow_compute is already defined.")
endif()
if (TARGET ArrowAcero::arrow_acero)
  message(FATAL_ERROR "ArrowAcero::arrow_acero is already defined.")
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

add_subdirectory(${CMAKE_CURRENT_LIST_DIR}/../.. tiforth_build)

set(EXAMPLE_MAIN "${CMAKE_CURRENT_LIST_DIR}/../arrow-common/main.cpp")

add_executable(system-arrow ${EXAMPLE_MAIN})
target_compile_features(system-arrow PRIVATE cxx_std_20)
target_compile_definitions(system-arrow PRIVATE EXAMPLE_PIPELINE_NAME="system-arrow")

target_link_libraries(system-arrow
  PRIVATE
    tiforth broken_pipeline::schedule Arrow::arrow
)
