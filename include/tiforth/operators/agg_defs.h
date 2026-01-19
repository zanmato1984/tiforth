#pragma once

#include <string>

#include "tiforth/expr.h"

namespace tiforth {

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

}  // namespace tiforth

