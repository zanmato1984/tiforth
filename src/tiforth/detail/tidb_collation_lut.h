// Copyright 2026 TiForth Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <array>
#include <cstdint>

namespace tiforth::tidb::GeneralCI {

using WeightType = std::uint16_t;

extern const std::array<WeightType, 256 * 256> weight_lut;

}  // namespace tiforth::tidb::GeneralCI

namespace tiforth::tidb::UnicodeCI {

extern const std::array<std::uint64_t, 256 * 256 + 1> weight_lut_0400;
extern const std::array<std::uint64_t, 0x2CEA1> weight_lut_0900;

}  // namespace tiforth::tidb::UnicodeCI

