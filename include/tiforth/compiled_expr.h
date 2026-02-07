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

#include <arrow/compute/expression.h>
#include <arrow/datum.h>
#include <arrow/result.h>

namespace arrow {
class Array;
class RecordBatch;
class Schema;
namespace compute {
class ExecContext;
}  // namespace compute
}  // namespace arrow

namespace tiforth {

class Engine;
struct Expr;

// A bound Arrow compute expression, compiled from `tiforth::Expr` for a specific input schema.
//
// This is the stable API surface for "bind once, execute many" expression evaluation.
struct CompiledExpr {
  std::shared_ptr<arrow::Schema> schema;
  arrow::compute::Expression bound;
};

arrow::Result<CompiledExpr> CompileExpr(const std::shared_ptr<arrow::Schema>& schema, const Expr& expr,
                                       const Engine* engine, arrow::compute::ExecContext* exec_context);

arrow::Result<arrow::Datum> ExecuteExpr(const CompiledExpr& compiled, const arrow::RecordBatch& batch,
                                       arrow::compute::ExecContext* exec_context);

arrow::Result<std::shared_ptr<arrow::Array>> ExecuteExprAsArray(const CompiledExpr& compiled,
                                                                const arrow::RecordBatch& batch,
                                                                arrow::compute::ExecContext* exec_context);

}  // namespace tiforth

