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

#include <memory>
#include <string>
#include <variant>
#include <vector>

#include <arrow/datum.h>
#include <arrow/result.h>
#include <arrow/scalar.h>

namespace arrow {
class Array;
class RecordBatch;
namespace compute {
class ExecContext;
}  // namespace compute
}  // namespace arrow

namespace tiforth {

class Engine;

struct Expr;
using ExprPtr = std::shared_ptr<Expr>;

struct FieldRef {
  std::string name;
  int index = -1;
};

struct Literal {
  std::shared_ptr<arrow::Scalar> value;
};

struct Call {
  std::string function_name;
  std::vector<ExprPtr> args;
};

struct Expr {
  std::variant<FieldRef, Literal, Call> node;
};

ExprPtr MakeFieldRef(std::string name);
ExprPtr MakeFieldRef(int index);
ExprPtr MakeLiteral(std::shared_ptr<arrow::Scalar> value);
ExprPtr MakeCall(std::string function_name, std::vector<ExprPtr> args);

arrow::Result<arrow::Datum> EvalExpr(const arrow::RecordBatch& batch, const Expr& expr,
                                    const Engine* engine, arrow::compute::ExecContext* exec_context);

arrow::Result<std::shared_ptr<arrow::Array>> EvalExprAsArray(
    const arrow::RecordBatch& batch, const Expr& expr, const Engine* engine,
    arrow::compute::ExecContext* exec_context);

}  // namespace tiforth
