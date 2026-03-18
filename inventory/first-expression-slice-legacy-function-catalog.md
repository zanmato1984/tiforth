# First Expression Slice Legacy Function Catalog

Status: issue #94 inventory checkpoint

Verified: 2026-03-18

Related issues:

- #84 `design: define first inventoried function and operator families`
- #94 `inventory: add first-expression-slice legacy function catalog`

## Purpose

This note records donor-repo evidence for the first-wave function inventory chosen in `docs/design/first-inventory-wave.md`.

It scopes only to the `first-expression-slice` function surface:

- `literal<int32>(value)`
- `add<int32>(lhs, rhs)`

This artifact treats the donor repository as evidence, not as shared design authority.

## Donor Snapshot

- repository: `https://github.com/zanmato1984/tiforth-legacy`
- inspected commit: `ccaa44be5a092d221032d7cac94ce4f5610c2c60`
- donor commit subject: `2026-02-07 Use broken_pipeline schedulers in operator tests`

## Shared Slice Anchors

This catalog stays anchored to the stable slice vocabulary already defined in `tests/differential/first-expression-slice.md`.

- `slice_id = first-expression-slice`
- `projection_ref = literal-int32-seven`
- `projection_ref = literal-int32-null`
- `projection_ref = add-a-plus-one`
- `case_id = literal-int32-seven`
- `case_id = literal-int32-null`
- `case_id = add-int32-literal`
- `case_id = add-int32-null-propagation`
- `case_id = add-int32-overflow-error`

## Reviewed Donor Sources

- `include/tiforth/expr.h`
- `src/tiforth/expr.cc`
- `src/tiforth/detail/expr_compiler.cc`
- `src/tiforth/functions/register.cc`
- `src/tiforth/functions/scalar_arithmetic_numeric_arithmetic.cc`
- `src/tiforth/operators/projection_test.cpp`
- `src/tiforth/operators/arrow_compute_agg_test.cpp`
- `src/tiforth/operators/hash_agg_test.cpp`
- `src/tiforth/operators/filter_test.cpp`
- `src/tiforth/functions/logical_test.cpp`

## Catalog Entries

### `literal<int32>(value)`

#### Donor Surface

- donor expressions use `Literal { std::shared_ptr<arrow::Scalar> value }` in `include/tiforth/expr.h`
- donor code constructs that node through `MakeLiteral(std::shared_ptr<arrow::Scalar>)` in `src/tiforth/expr.cc`
- the compiler rejects a missing scalar object with `literal value must not be null` and lowers accepted literals through `arrow::compute::literal(arrow::Datum(node.value))` in `src/tiforth/detail/expr_compiler.cc`

#### Observed Donor Spellings

- `MakeLiteral(std::make_shared<arrow::Int32Scalar>(10))` in `src/tiforth/operators/projection_test.cpp`
- `MakeLiteral(std::make_shared<arrow::Int32Scalar>(1))` in `src/tiforth/operators/arrow_compute_agg_test.cpp` and `src/tiforth/operators/hash_agg_test.cpp`
- `MakeLiteral(std::make_shared<arrow::Int32Scalar>(0))`, `1`, and `2` in `src/tiforth/functions/logical_test.cpp` and `src/tiforth/operators/filter_test.cpp`

#### Recorded Donor Facts

- donor literal expressions are constructed programmatically from Arrow scalar objects rather than from SQL text
- reviewed donor usage confirms non-null `int32` literals flowing through projection, filter, logical, and aggregate expression trees
- literal logical typing is inferred from the Arrow scalar type at compile time rather than from output naming or projection context

#### Evidence Gaps

- the reviewed donor files did not show an explicit typed-null `Int32Scalar` literal or a `literal<int32>(NULL)` projection case
- because the compiler only rejects `nullptr` and not an invalid typed scalar, typed-null literal support may exist; this is an inference, not a confirmed catalog fact
- the shared `literal-int32-null` slice case therefore remains current `tiforth` spec and harness evidence, not donor-confirmed behavior from this catalog pass

### `add<int32>(lhs, rhs)`

#### Donor Surface

- donor expressions use `Call { std::string function_name, std::vector<ExprPtr> args }` in `include/tiforth/expr.h`
- donor code constructs the current arithmetic shape through `MakeCall("add", {...})` in `src/tiforth/expr.cc`
- the compiler recursively binds child expressions and keeps the function name `add` for non-decimal arguments in `src/tiforth/detail/expr_compiler.cc`
- the same compiler rewrites decimal calls to `tiforth.decimal_add`, which separates reviewed decimal behavior from the non-decimal path

#### Observed Donor Spellings

- `MakeCall("add", {MakeFieldRef("x"), MakeFieldRef("y")})` in `src/tiforth/operators/projection_test.cpp`
- `MakeCall("add", {MakeFieldRef("x"), MakeLiteral(std::make_shared<arrow::Int32Scalar>(10))})` in `src/tiforth/operators/projection_test.cpp`
- `MakeCall("add", {arg, MakeLiteral(std::make_shared<arrow::Int32Scalar>(1))})` in `src/tiforth/operators/arrow_compute_agg_test.cpp` and `src/tiforth/operators/hash_agg_test.cpp`

#### Recorded Donor Facts

- donor `add` is represented as a normal expression-call node, not as a projection-only primitive
- reviewed donor projection coverage shows row-wise evaluation for both `add(column, column)` and `add(column, literal<int32>)`, producing `{11, 22, 33}` and `{11, 12, 13}` over the first smoke-test batch in `src/tiforth/operators/projection_test.cpp`
- reviewed donor aggregate tests show the same `add(..., literal<int32>(1))` form reused inside computed grouping and aggregate expressions rather than being confined to top-level projection output columns
- inference from reviewed sources: non-decimal `add` is not registered as a `tiforth`-specific arithmetic kernel in `src/tiforth/functions/scalar_arithmetic_numeric_arithmetic.cc`, so donor `add<int32>` appears to rely on the Arrow compute registry or fallback path while decimal `add` receives donor-specific rewriting

#### Evidence Gaps

- the reviewed donor files did not surface an explicit `add<int32>` null-propagation test
- the reviewed donor files did not surface an explicit `add<int32>` overflow test
- the reviewed donor files did not pin an `int32`-only rejection rule for mixed-type arithmetic; donor arithmetic support appears broader than the current milestone-1 shared boundary
- current `tiforth` rules for `add-int32-null-propagation`, `add-int32-overflow-error`, and non-`int32` rejection therefore remain shared spec and harness decisions rather than donor-catalog facts from this pass

## Boundary For This Artifact

- this catalog records donor evidence only for the first-wave function family
- output naming, output order, direct column passthrough, and row-count preservation belong in the separate projection operator catalog
- TiDB and TiFlash compatibility notes remain separate follow-on inventory artifacts
