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

#include "tiforth/expr.h"

#include <utility>

#include <arrow/compute/exec.h>
#include <arrow/compute/registry.h>
#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/scalar.h>
#include <arrow/status.h>

#include "tiforth/compiled_expr.h"
#include "tiforth/engine.h"

namespace tiforth {

ExprPtr MakeFieldRef(std::string name) {
  auto expr = std::make_shared<Expr>();
  expr->node = FieldRef{std::move(name), -1};
  return expr;
}

ExprPtr MakeFieldRef(int index) {
  auto expr = std::make_shared<Expr>();
  expr->node = FieldRef{"", index};
  return expr;
}

ExprPtr MakeLiteral(std::shared_ptr<arrow::Scalar> value) {
  auto expr = std::make_shared<Expr>();
  expr->node = Literal{std::move(value)};
  return expr;
}

ExprPtr MakeCall(std::string function_name, std::vector<ExprPtr> args) {
  auto expr = std::make_shared<Expr>();
  expr->node = Call{std::move(function_name), std::move(args)};
  return expr;
}

arrow::Result<arrow::Datum> EvalExpr(const arrow::RecordBatch& batch, const Expr& expr,
                                    const Engine* engine,
                                    arrow::compute::ExecContext* exec_context) {
  arrow::compute::ExecContext local_exec_context(
      engine != nullptr ? engine->memory_pool() : arrow::default_memory_pool(), /*executor=*/nullptr,
      engine != nullptr ? engine->function_registry() : arrow::compute::GetFunctionRegistry());
  auto* ctx = exec_context != nullptr ? exec_context : &local_exec_context;

  ARROW_ASSIGN_OR_RAISE(auto compiled, CompileExpr(batch.schema(), expr, engine, ctx));
  return ExecuteExpr(compiled, batch, ctx);
}

arrow::Result<std::shared_ptr<arrow::Array>> EvalExprAsArray(
    const arrow::RecordBatch& batch, const Expr& expr, const Engine* engine,
    arrow::compute::ExecContext* exec_context) {
  arrow::compute::ExecContext local_exec_context(
      engine != nullptr ? engine->memory_pool() : arrow::default_memory_pool(), /*executor=*/nullptr,
      engine != nullptr ? engine->function_registry() : arrow::compute::GetFunctionRegistry());
  auto* ctx = exec_context != nullptr ? exec_context : &local_exec_context;

  ARROW_ASSIGN_OR_RAISE(auto compiled, CompileExpr(batch.schema(), expr, engine, ctx));
  return ExecuteExprAsArray(compiled, batch, ctx);
}

}  // namespace tiforth
