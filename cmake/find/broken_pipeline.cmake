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
