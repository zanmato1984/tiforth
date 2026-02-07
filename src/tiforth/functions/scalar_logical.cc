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

#include <memory>
#include <vector>

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/compute/exec.h>
#include <arrow/compute/function.h>
#include <arrow/compute/kernel.h>
#include <arrow/compute/registry.h>
#include <arrow/scalar.h>
#include <arrow/status.h>
#include <arrow/type.h>

namespace tiforth::function {

namespace {

struct Truthy {
  bool is_null = false;
  bool value = false;
};

struct ArgView {
  std::shared_ptr<arrow::Array> array;
  const arrow::Scalar* scalar = nullptr;
  arrow::Type::type type_id = arrow::Type::NA;
  bool is_array = false;
};

arrow::Status GetTruthyNumberLike(const arrow::Array& array, int64_t index, Truthy* out) {
  if (out == nullptr) {
    return arrow::Status::Invalid("internal error: missing out");
  }

  out->is_null = array.IsNull(index);
  out->value = false;
  if (out->is_null) {
    return arrow::Status::OK();
  }

  switch (array.type_id()) {
    case arrow::Type::BOOL: {
      const auto& typed = static_cast<const arrow::BooleanArray&>(array);
      out->value = typed.Value(index);
      return arrow::Status::OK();
    }
    case arrow::Type::INT8: {
      const auto& typed = static_cast<const arrow::Int8Array&>(array);
      out->value = typed.Value(index) != 0;
      return arrow::Status::OK();
    }
    case arrow::Type::INT16: {
      const auto& typed = static_cast<const arrow::Int16Array&>(array);
      out->value = typed.Value(index) != 0;
      return arrow::Status::OK();
    }
    case arrow::Type::INT32: {
      const auto& typed = static_cast<const arrow::Int32Array&>(array);
      out->value = typed.Value(index) != 0;
      return arrow::Status::OK();
    }
    case arrow::Type::INT64: {
      const auto& typed = static_cast<const arrow::Int64Array&>(array);
      out->value = typed.Value(index) != 0;
      return arrow::Status::OK();
    }
    case arrow::Type::UINT8: {
      const auto& typed = static_cast<const arrow::UInt8Array&>(array);
      out->value = typed.Value(index) != 0;
      return arrow::Status::OK();
    }
    case arrow::Type::UINT16: {
      const auto& typed = static_cast<const arrow::UInt16Array&>(array);
      out->value = typed.Value(index) != 0;
      return arrow::Status::OK();
    }
    case arrow::Type::UINT32: {
      const auto& typed = static_cast<const arrow::UInt32Array&>(array);
      out->value = typed.Value(index) != 0;
      return arrow::Status::OK();
    }
    case arrow::Type::UINT64: {
      const auto& typed = static_cast<const arrow::UInt64Array&>(array);
      out->value = typed.Value(index) != 0;
      return arrow::Status::OK();
    }
    case arrow::Type::FLOAT: {
      const auto& typed = static_cast<const arrow::FloatArray&>(array);
      out->value = typed.Value(index) != 0.0f;
      return arrow::Status::OK();
    }
    case arrow::Type::DOUBLE: {
      const auto& typed = static_cast<const arrow::DoubleArray&>(array);
      out->value = typed.Value(index) != 0.0;
      return arrow::Status::OK();
    }
    default:
      break;
  }

  return arrow::Status::NotImplemented("unsupported input type: ", array.type()->ToString());
}

arrow::Status GetTruthyNumberLike(const arrow::Scalar& scalar, Truthy* out) {
  if (out == nullptr) {
    return arrow::Status::Invalid("internal error: missing out");
  }

  out->is_null = !scalar.is_valid;
  out->value = false;
  if (out->is_null) {
    return arrow::Status::OK();
  }

  switch (scalar.type->id()) {
    case arrow::Type::BOOL: {
      const auto& typed = static_cast<const arrow::BooleanScalar&>(scalar);
      out->value = typed.value;
      return arrow::Status::OK();
    }
    case arrow::Type::INT8: {
      const auto& typed = static_cast<const arrow::Int8Scalar&>(scalar);
      out->value = typed.value != 0;
      return arrow::Status::OK();
    }
    case arrow::Type::INT16: {
      const auto& typed = static_cast<const arrow::Int16Scalar&>(scalar);
      out->value = typed.value != 0;
      return arrow::Status::OK();
    }
    case arrow::Type::INT32: {
      const auto& typed = static_cast<const arrow::Int32Scalar&>(scalar);
      out->value = typed.value != 0;
      return arrow::Status::OK();
    }
    case arrow::Type::INT64: {
      const auto& typed = static_cast<const arrow::Int64Scalar&>(scalar);
      out->value = typed.value != 0;
      return arrow::Status::OK();
    }
    case arrow::Type::UINT8: {
      const auto& typed = static_cast<const arrow::UInt8Scalar&>(scalar);
      out->value = typed.value != 0;
      return arrow::Status::OK();
    }
    case arrow::Type::UINT16: {
      const auto& typed = static_cast<const arrow::UInt16Scalar&>(scalar);
      out->value = typed.value != 0;
      return arrow::Status::OK();
    }
    case arrow::Type::UINT32: {
      const auto& typed = static_cast<const arrow::UInt32Scalar&>(scalar);
      out->value = typed.value != 0;
      return arrow::Status::OK();
    }
    case arrow::Type::UINT64: {
      const auto& typed = static_cast<const arrow::UInt64Scalar&>(scalar);
      out->value = typed.value != 0;
      return arrow::Status::OK();
    }
    case arrow::Type::FLOAT: {
      const auto& typed = static_cast<const arrow::FloatScalar&>(scalar);
      out->value = typed.value != 0.0f;
      return arrow::Status::OK();
    }
    case arrow::Type::DOUBLE: {
      const auto& typed = static_cast<const arrow::DoubleScalar&>(scalar);
      out->value = typed.value != 0.0;
      return arrow::Status::OK();
    }
    default:
      break;
  }

  return arrow::Status::NotImplemented("unsupported input type: ", scalar.type->ToString());
}

arrow::Status InitArgView(const arrow::compute::ExecValue& value, ArgView* out) {
  if (out == nullptr) {
    return arrow::Status::Invalid("internal error: missing out");
  }

  out->array.reset();
  out->scalar = nullptr;
  out->type_id = arrow::Type::NA;
  out->is_array = false;

  if (value.is_array()) {
    out->array = arrow::MakeArray(value.array.ToArrayData());
    if (out->array == nullptr) {
      return arrow::Status::Invalid("expected non-null array input");
    }
    out->type_id = out->array->type_id();
    out->is_array = true;
    return arrow::Status::OK();
  }

  if (value.is_scalar()) {
    out->scalar = value.scalar;
    if (out->scalar == nullptr || out->scalar->type == nullptr) {
      return arrow::Status::Invalid("expected non-null scalar input");
    }
    out->type_id = out->scalar->type->id();
    out->is_array = false;
    return arrow::Status::OK();
  }

  return arrow::Status::Invalid("unsupported datum kind");
}

arrow::Status GetTruthy(const ArgView& arg, int64_t row, Truthy* out) {
  if (arg.is_array) {
    if (arg.array == nullptr) {
      return arrow::Status::Invalid("internal error: missing array in arg view");
    }
    return GetTruthyNumberLike(*arg.array, row, out);
  }

  if (arg.scalar == nullptr) {
    return arrow::Status::Invalid("internal error: missing scalar in arg view");
  }
  return GetTruthyNumberLike(*arg.scalar, out);
}

arrow::Status ExecAnd(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                      arrow::compute::ExecResult* out) {
  if (ctx == nullptr || out == nullptr) {
    return arrow::Status::Invalid("kernel context/result must not be null");
  }
  if (batch.num_values() < 2) {
    return arrow::Status::Invalid("and requires at least 2 arguments");
  }

  const int64_t rows = batch.length;
  arrow::BooleanBuilder out_builder(ctx->memory_pool());
  ARROW_RETURN_NOT_OK(out_builder.Reserve(rows));

  std::vector<ArgView> args(batch.num_values());
  for (int arg_index = 0; arg_index < batch.num_values(); ++arg_index) {
    ARROW_RETURN_NOT_OK(InitArgView(batch[arg_index], &args[arg_index]));
  }

  for (int64_t i = 0; i < rows; ++i) {
    bool saw_null = false;
    bool result = true;
    for (int arg_index = 0; arg_index < batch.num_values(); ++arg_index) {
      Truthy v;
      ARROW_RETURN_NOT_OK(GetTruthy(args[arg_index], i, &v));
      if (v.is_null) {
        saw_null = true;
        continue;
      }
      if (!v.value) {
        result = false;
        break;
      }
    }

    if (!result) {
      out_builder.UnsafeAppend(false);
    } else if (saw_null) {
      out_builder.UnsafeAppendNull();
    } else {
      out_builder.UnsafeAppend(true);
    }
  }

  std::shared_ptr<arrow::Array> out_array;
  ARROW_RETURN_NOT_OK(out_builder.Finish(&out_array));
  out->value = out_array->data();
  return arrow::Status::OK();
}

arrow::Status ExecOr(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                     arrow::compute::ExecResult* out) {
  if (ctx == nullptr || out == nullptr) {
    return arrow::Status::Invalid("kernel context/result must not be null");
  }
  if (batch.num_values() < 2) {
    return arrow::Status::Invalid("or requires at least 2 arguments");
  }

  const int64_t rows = batch.length;
  arrow::BooleanBuilder out_builder(ctx->memory_pool());
  ARROW_RETURN_NOT_OK(out_builder.Reserve(rows));

  std::vector<ArgView> args(batch.num_values());
  for (int arg_index = 0; arg_index < batch.num_values(); ++arg_index) {
    ARROW_RETURN_NOT_OK(InitArgView(batch[arg_index], &args[arg_index]));
  }

  for (int64_t i = 0; i < rows; ++i) {
    bool saw_null = false;
    bool result = false;
    for (int arg_index = 0; arg_index < batch.num_values(); ++arg_index) {
      Truthy v;
      ARROW_RETURN_NOT_OK(GetTruthy(args[arg_index], i, &v));
      if (v.is_null) {
        saw_null = true;
        continue;
      }
      if (v.value) {
        result = true;
        break;
      }
    }

    if (result) {
      out_builder.UnsafeAppend(true);
    } else if (saw_null) {
      out_builder.UnsafeAppendNull();
    } else {
      out_builder.UnsafeAppend(false);
    }
  }

  std::shared_ptr<arrow::Array> out_array;
  ARROW_RETURN_NOT_OK(out_builder.Finish(&out_array));
  out->value = out_array->data();
  return arrow::Status::OK();
}

arrow::Status ExecXor(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                      arrow::compute::ExecResult* out) {
  if (ctx == nullptr || out == nullptr) {
    return arrow::Status::Invalid("kernel context/result must not be null");
  }
  if (batch.num_values() < 2) {
    return arrow::Status::Invalid("xor requires at least 2 arguments");
  }

  const int64_t rows = batch.length;
  arrow::BooleanBuilder out_builder(ctx->memory_pool());
  ARROW_RETURN_NOT_OK(out_builder.Reserve(rows));

  std::vector<ArgView> args(batch.num_values());
  for (int arg_index = 0; arg_index < batch.num_values(); ++arg_index) {
    ARROW_RETURN_NOT_OK(InitArgView(batch[arg_index], &args[arg_index]));
  }

  for (int64_t i = 0; i < rows; ++i) {
    bool saw_null = false;
    bool result = false;
    for (int arg_index = 0; arg_index < batch.num_values(); ++arg_index) {
      Truthy v;
      ARROW_RETURN_NOT_OK(GetTruthy(args[arg_index], i, &v));
      if (v.is_null) {
        saw_null = true;
        break;
      }
      result = result != v.value;
    }

    if (saw_null) {
      out_builder.UnsafeAppendNull();
    } else {
      out_builder.UnsafeAppend(result);
    }
  }

  std::shared_ptr<arrow::Array> out_array;
  ARROW_RETURN_NOT_OK(out_builder.Finish(&out_array));
  out->value = out_array->data();
  return arrow::Status::OK();
}

arrow::Status ExecNot(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                      arrow::compute::ExecResult* out) {
  if (ctx == nullptr || out == nullptr) {
    return arrow::Status::Invalid("kernel context/result must not be null");
  }
  if (batch.num_values() != 1) {
    return arrow::Status::Invalid("not requires exactly 1 argument");
  }

  const int64_t rows = batch.length;
  arrow::BooleanBuilder out_builder(ctx->memory_pool());
  ARROW_RETURN_NOT_OK(out_builder.Reserve(rows));

  ArgView arg;
  ARROW_RETURN_NOT_OK(InitArgView(batch[0], &arg));

  for (int64_t i = 0; i < rows; ++i) {
    Truthy v;
    ARROW_RETURN_NOT_OK(GetTruthy(arg, i, &v));
    if (v.is_null) {
      out_builder.UnsafeAppendNull();
    } else {
      out_builder.UnsafeAppend(!v.value);
    }
  }

  std::shared_ptr<arrow::Array> out_array;
  ARROW_RETURN_NOT_OK(out_builder.Finish(&out_array));
  out->value = out_array->data();
  return arrow::Status::OK();
}

arrow::compute::ScalarKernel MakeVarArgsKernel(arrow::compute::ArrayKernelExec exec) {
  arrow::compute::ScalarKernel kernel(
      arrow::compute::KernelSignature::Make({arrow::compute::InputType::Any(),
                                             arrow::compute::InputType::Any()},
                                            arrow::compute::OutputType(arrow::boolean()),
                                            /*is_varargs=*/true),
      exec);
  kernel.null_handling = arrow::compute::NullHandling::COMPUTED_NO_PREALLOCATE;
  kernel.mem_allocation = arrow::compute::MemAllocation::NO_PREALLOCATE;
  kernel.can_write_into_slices = false;
  return kernel;
}

arrow::compute::ScalarKernel MakeUnaryKernel(arrow::compute::ArrayKernelExec exec) {
  arrow::compute::ScalarKernel kernel(
      {arrow::compute::InputType::Any()}, arrow::compute::OutputType(arrow::boolean()), exec);
  kernel.null_handling = arrow::compute::NullHandling::COMPUTED_NO_PREALLOCATE;
  kernel.mem_allocation = arrow::compute::MemAllocation::NO_PREALLOCATE;
  kernel.can_write_into_slices = false;
  return kernel;
}

}  // namespace

arrow::Status RegisterScalarLogicalFunctions(arrow::compute::FunctionRegistry* registry,
                                             arrow::compute::FunctionRegistry* fallback_registry) {
  if (registry == nullptr) {
    return arrow::Status::Invalid("function registry must not be null");
  }
  if (fallback_registry == nullptr) {
    return arrow::Status::Invalid("fallback function registry must not be null");
  }

  {
    auto func = std::make_shared<arrow::compute::ScalarFunction>(
        "and", arrow::compute::Arity::VarArgs(/*min_args=*/2),
        arrow::compute::FunctionDoc::Empty());
    ARROW_RETURN_NOT_OK(func->AddKernel(MakeVarArgsKernel(ExecAnd)));
    ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(func), /*allow_overwrite=*/true));
  }
  {
    auto func = std::make_shared<arrow::compute::ScalarFunction>(
        "or", arrow::compute::Arity::VarArgs(/*min_args=*/2),
        arrow::compute::FunctionDoc::Empty());
    ARROW_RETURN_NOT_OK(func->AddKernel(MakeVarArgsKernel(ExecOr)));
    ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(func), /*allow_overwrite=*/true));
  }
  {
    auto func = std::make_shared<arrow::compute::ScalarFunction>(
        "xor", arrow::compute::Arity::VarArgs(/*min_args=*/2),
        arrow::compute::FunctionDoc::Empty());
    ARROW_RETURN_NOT_OK(func->AddKernel(MakeVarArgsKernel(ExecXor)));
    ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(func), /*allow_overwrite=*/true));
  }
  {
    auto func = std::make_shared<arrow::compute::ScalarFunction>(
        "not", arrow::compute::Arity::Unary(), arrow::compute::FunctionDoc::Empty());
    ARROW_RETURN_NOT_OK(func->AddKernel(MakeUnaryKernel(ExecNot)));
    ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(func), /*allow_overwrite=*/true));
  }

  return arrow::Status::OK();
}

}  // namespace tiforth::function
