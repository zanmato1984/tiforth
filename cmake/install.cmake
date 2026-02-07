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

include_guard(GLOBAL)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

install(
  TARGETS tiforth
  EXPORT tiforthTargets
  ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
  LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
  RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
)

install(
  DIRECTORY "${PROJECT_SOURCE_DIR}/include/tiforth/"
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/tiforth"
)

install(
  EXPORT tiforthTargets
  FILE tiforthTargets.cmake
  NAMESPACE tiforth::
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/tiforth"
)

configure_package_config_file(
  "${CMAKE_CURRENT_LIST_DIR}/package_config.cmake.in"
  "${PROJECT_BINARY_DIR}/tiforthConfig.cmake"
  INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/tiforth"
)

write_basic_package_version_file(
  "${PROJECT_BINARY_DIR}/tiforthConfigVersion.cmake"
  VERSION "${PROJECT_VERSION}"
  COMPATIBILITY SameMajorVersion
)

install(
  FILES
    "${PROJECT_BINARY_DIR}/tiforthConfig.cmake"
    "${PROJECT_BINARY_DIR}/tiforthConfigVersion.cmake"
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/tiforth"
)
