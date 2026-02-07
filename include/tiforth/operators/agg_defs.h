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

#include <string>

#include "tiforth/expr.h"

namespace tiforth::op {

struct AggKey {
  std::string name;
  ExprPtr expr;
};

struct AggFunc {
  std::string name;
  // Arrow-compute-backed operators may accept: "count_all", "count", "sum", "mean"/"avg",
  // "min", "max". TiForth native hash agg supports a subset and may accept aliases.
  std::string func;
  ExprPtr arg;  // unused for "count_all"
};

}  // namespace tiforth::op
