# First Expression Slice Legacy Operator Catalog

Status: issue #96 inventory checkpoint

Verified: 2026-03-18

Related issues:

- #84 `design: define first inventoried function and operator families`
- #94 `inventory: add first-expression-slice legacy function catalog`
- #96 `inventory: add first-expression-slice legacy operator catalog`

## Purpose

This note records donor-repo evidence for the first-wave operator inventory chosen in `docs/design/first-inventory-wave.md`.

It scopes only to the `first-expression-slice` single-input projection family:

- one input batch enters a projection pipe and one output batch leaves it
- direct `column(index)` passthrough stays part of the projection family rather than a separate operator
- output naming, output order, and row-count preservation remain operator concerns even when expression semantics live in `literal<int32>(value)` or `add<int32>(lhs, rhs)`

This artifact treats the donor repository as evidence, not as shared design authority.

## Donor Snapshot

- repository: `https://github.com/zanmato1984/tiforth-legacy`
- inspected commit: `ccaa44be5a092d221032d7cac94ce4f5610c2c60`
- donor commit subject: `2026-02-07 Use broken_pipeline schedulers in operator tests`

## Shared Slice Anchors

This catalog stays anchored to the stable slice vocabulary already defined in `tests/differential/first-expression-slice.md`.

- `slice_id = first-expression-slice`
- `projection_ref = column-a`
- `projection_ref = literal-int32-seven`
- `projection_ref = literal-int32-null`
- `projection_ref = add-a-plus-one`
- `case_id = column-passthrough`
- `case_id = literal-int32-seven`
- `case_id = literal-int32-null`
- `case_id = add-int32-literal`
- `case_id = add-int32-null-propagation`
- `case_id = add-int32-overflow-error`

The reviewed donor spellings use source fields `x` and `y` rather than the shared slice field `a`. This artifact maps donor evidence back to the shared refs instead of promoting donor spellings into the source of truth.

## Reviewed Donor Sources

- `include/tiforth/expr.h`
- `include/tiforth/operators/projection.h`
- `src/tiforth/expr.cc`
- `src/tiforth/operators/projection.cc`
- `src/tiforth/operators/projection_test.cpp`

## Catalog Entries

### `single-input projection family`

#### Donor Surface

- donor projection outputs are declared as `ProjectionExpr { std::string name, ExprPtr expr }` in `include/tiforth/operators/projection.h`
- donor projection execution enters through `ProjectionPipeOp`, a `PipeOp` that accepts one `Engine*`, one ordered `std::vector<ProjectionExpr>`, and an optional Arrow memory pool in `include/tiforth/operators/projection.h`
- donor `Project(...)` compiles every expression against the first seen input schema, caches those compiled expressions, evaluates them for each input batch, and returns one Arrow `RecordBatch` with the original input row count in `src/tiforth/operators/projection.cc`
- donor `ComputeOutputSchema(...)` builds the output schema from the ordered projection list, preserving input-field metadata for direct field references and otherwise deriving field type metadata from evaluated arrays in `src/tiforth/operators/projection.cc`
- donor `Pipe()` returns `PipeSinkNeedsMore` when no input batch is available, emits `PipeEven(batch)` for each non-null input batch, and `Drain()` returns `Finished()` in `src/tiforth/operators/projection.cc`

#### Observed Donor Spellings

- reviewed smoke coverage constructs three ordered outputs as `{"x", MakeFieldRef("x")}`, `{"x_plus_y", MakeCall("add", {MakeFieldRef("x"), MakeFieldRef("y")})}`, and `{"x_plus_10", MakeCall("add", {MakeFieldRef("x"), MakeLiteral(std::make_shared<arrow::Int32Scalar>(10))})}` in `src/tiforth/operators/projection_test.cpp`
- that smoke test runs the projection operator over two input batches with rows `{1, 2, 3}` / `{10, 20, 30}` and `{4}` / `{100}` in `src/tiforth/operators/projection_test.cpp`
- the same smoke test asserts a stable shared output schema across both batches, validates output field names `x`, `x_plus_y`, and `x_plus_10`, and checks output values `{1, 2, 3}`, `{11, 22, 33}`, `{11, 12, 13}` then `{4}`, `{104}`, `{14}` in `src/tiforth/operators/projection_test.cpp`

#### Recorded Donor Facts

- donor projection output order follows the `ProjectionExpr` vector order rather than later schema sorting or planner rewriting
- donor projection is batch-preserving: one non-null input batch yields one non-null output batch with the same row count
- donor projection caches both compiled expressions and the first computed output schema, then requires later batches to keep the same output field count and output types
- donor projection keeps direct field references inside the operator family rather than splitting passthrough into a separate primitive
- reviewed donor evidence shows the projection family carrying both direct passthrough and computed arithmetic outputs in the same ordered output batch, while the current shared `first-expression-slice` normalizes those outputs into one `projection_ref` per case

### `column(index)` passthrough inside projection`

#### Donor Surface

- donor expressions represent field references as `FieldRef { std::string name, int index }` in `include/tiforth/expr.h`
- donor constructors support both `MakeFieldRef(std::string name)` and `MakeFieldRef(int index)` in `src/tiforth/expr.cc`
- when `ProjectionPipeOp::ComputeOutputSchema(...)` sees a direct `FieldRef`, it resolves by explicit index first and falls back to schema name lookup when `index < 0` and `name` is non-empty in `src/tiforth/operators/projection.cc`
- after resolving a direct input field, donor projection reuses that input field metadata and only applies the requested output name through `WithName(name)` in `src/tiforth/operators/projection.cc`

#### Observed Donor Spellings

- reviewed projection smoke coverage uses `MakeFieldRef("x")` as the first projected output in `src/tiforth/operators/projection_test.cpp`
- that same smoke test verifies the passthrough column keeps values `{1, 2, 3}` for the first batch and `{4}` for the second batch in `src/tiforth/operators/projection_test.cpp`
- the smoke test names that passthrough output `x`, matching the input field name, in `src/tiforth/operators/projection_test.cpp`

#### Recorded Donor Facts

- direct field passthrough is a first-class projection path in donor `tiforth`, not an implementation accident hidden behind computed expressions only
- reviewed donor evidence confirms name-based passthrough from the first input column into the first output slot of a projection batch
- donor schema construction preserves source-field metadata for direct field references before applying the chosen output name, which aligns with the current shared `column-a` intent when the output name matches the source field name

#### Evidence Gaps

- the reviewed donor files did not show an explicit `MakeFieldRef(0)` or other index-based passthrough test, so positional `column(index)` support is interface-defined but not smoke-test-confirmed in this catalog pass
- the reviewed donor files did not show the shared slice spelling `column(a)` over a one-column input named `a`; mapping donor `x` onto shared `column-a` is therefore a semantic normalization performed by this repository
- the reviewed donor files did not show a standalone one-output projection case for `literal<int32>(7)` or `literal<int32>(NULL)` named `lit`, even though the projection family is the operator carrier for those shared cases
- the reviewed donor files did not show projection-level donor checks for null-propagation or overflow outcomes on the current shared `add-a-plus-one` cases; those remain shared spec and harness evidence rather than donor-confirmed operator facts from this pass

## Boundary For This Artifact

- this catalog records donor evidence only for the first-wave single-input projection operator family
- donor expression-node details for `literal<int32>(value)` and `add<int32>(lhs, rhs)` remain in `inventory/first-expression-slice-legacy-function-catalog.md`
- TiDB and TiFlash compatibility notes remain separate follow-on inventory artifacts
