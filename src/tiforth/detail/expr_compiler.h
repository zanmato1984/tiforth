#pragma once

#include <memory>

#include <arrow/result.h>

#include "tiforth/compiled_expr.h"

namespace arrow {
class Schema;
namespace compute {
class ExecContext;
}  // namespace compute
}  // namespace arrow

namespace tiforth {

class Engine;
struct Expr;

namespace detail {

arrow::Result<CompiledExpr> CompileExpr(const std::shared_ptr<arrow::Schema>& schema,
                                       const Expr& expr, const Engine* engine,
                                       arrow::compute::ExecContext* exec_context);

}  // namespace detail
}  // namespace tiforth
