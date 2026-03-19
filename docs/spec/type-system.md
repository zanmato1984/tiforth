# Type System

This document scopes the shared semantic type system. It is not yet a full formal specification.

## Intent

The shared type system should be rich enough to describe behavior across TiDB, TiFlash, and TiKV while remaining testable through harnesses and expressible through the data contract.

## Likely Type Families

- integer and unsigned integer types
- floating-point types
- decimal types
- boolean
- string and binary
- temporal types
- JSON and other semi-structured values
- internal intermediate types, if later issues or spec work require them

## Required Semantic Areas

- type identity
- nullability
- coercion and cast rules
- comparison behavior
- collation-sensitive comparison and ordering behavior
- arithmetic behavior
- function signature matching
- result type derivation

## Representation Boundary

- Arrow physical encodings such as dictionary layout do not change shared semantic type identity by themselves
- current specs and harnesses should normalize dictionary-backed evidence to the underlying logical type before comparison
- representation-level rules for when dictionary arrays may cross a shared stage boundary live in `docs/contracts/data.md`
- milestone-1 nested plus decimal and temporal family scope boundaries live in `docs/design/milestone-1-nested-decimal-temporal-boundary.md`

## Initial Collation Scope And Ownership Checkpoint

Issue #143 adds a docs-first checkpoint in `docs/design/collation-scope-ownership.md`.

For current shared contracts:

- milestone-1 executable slices remain collation-insensitive because they are
  limited to the current `int32` projection and first filter predicate
  boundaries
- shared contracts do not yet require runtime collation state,
  batch-level collation metadata, or a shared collation negotiation API
- adapter-local session and engine collation settings remain
  orchestration details unless and until a later shared slice defines
  explicit collation-sensitive semantics

This checkpoint resolves ownership scope without claiming executable string or binary collation behavior in milestone 1.

## Initial Coercion Lattice And Precedence

This checkpoint defines the first shared implicit-coercion policy for future expression and harness slices beyond the current executable `add<int32>` path.

### Lattice Nodes And Implicit Edges

- identity coercion is always allowed (`T -> T`)
- numeric widening for this checkpoint is limited to:
  - `int32 -> int64`
  - `int64 -> float64`
  - `int32 -> float64` (via widening)
- no other implicit coercions are currently allowed
- boolean, unsigned integer, decimal, string or binary, temporal, and JSON families require exact type matches until follow-up issues define their coercion edges
- untyped `NULL` may adopt the expected type from an already-selected signature, but it does not drive signature selection on its own

### Precedence Rules For Signature Selection

When multiple operator or function signatures are possible:

1. choose an exact-type match when one exists
2. otherwise choose the candidate that needs the fewest implicit coercions
3. for numeric candidates in this checkpoint, choose the least upper bound under `int32 < int64 < float64`
4. if no unique candidate can be selected, raise an execution error instead of guessing

This keeps signature selection deterministic while preserving explicit errors for unsupported or ambiguous type combinations.

### Signed Versus Unsigned Interaction Checkpoint

Issue #141 defines the first shared signed-versus-unsigned interaction
boundary for this initial lattice checkpoint.

- no implicit coercions are allowed between signed and unsigned integer
  families in this checkpoint
- mixed signed and unsigned candidates are not formed indirectly by widening
  through `float64`
- any candidate signature that would require a signed-to-unsigned or
  unsigned-to-signed implicit coercion is rejected before precedence ranking
- if no candidate remains after that rejection, execution fails with an
  explicit error instead of guessing
- exact unsigned matches are still selectable when a function family
  explicitly defines one; untyped `NULL` may adopt that selected unsigned type
  after signature selection, but `NULL` does not choose between signed and
  unsigned candidates by itself

For this checkpoint, representative mixed requests such as `int32` with
`uint32`, or `int64` with `uint64`, are execution errors unless a follow-on
spec issue defines explicit interaction semantics.

## Current Milestone-1 Boundary

The current executable slices fix the type behavior for the milestone-1 expression-projection path plus the first executable post-gate filter path.

### Covered Expression And Predicate Families

- `column(index)`
- `literal<int32>(value)`
- `add<int32>(lhs, rhs)`
- `is_not_null(column(index))`

The lattice and precedence policy above is a shared type-system checkpoint for later slices. The current executable milestone-1 arithmetic path still uses exact `int32` typing for `add<int32>`.

### Milestone-1 Out-Of-Scope Families

- nested logical families are out of scope for the current executable and differential milestone-1 checkpoints
- decimal and temporal logical families are out of scope for current milestone-1 expression evaluation and result typing
- current milestone-1 local execution paths should fail unsupported nested, decimal, or temporal requests explicitly rather than applying implicit casts into the `int32` arithmetic path
- first-slice differential artifacts should continue using the documented `int32` logical-type surface until a follow-on issue extends family coverage

### `column(index)`

- preserves the referenced input field's logical type
- preserves the referenced input field's nullability
- does not coerce the input into another logical type
- reports an execution error when `index` is out of range

### `literal<int32>(value)`

- always derives logical type `int32`
- derives `nullable = false` for non-null literals
- derives `nullable = true` for `NULL` literals
- does not introduce implicit widening or signedness changes in milestone 1

### `add<int32>(lhs, rhs)`

- requires both operands to resolve to `int32` expressions in this slice
- derives logical type `int32`
- derives result nullability as `lhs.nullable OR rhs.nullable`
- propagates nulls row-wise at execution time
- reports overflow as an execution error; milestone 1 does not wrap, saturate, or widen the result
- reports an execution error rather than applying an implicit cast when an operand is not `int32`
- does not yet apply the broader `int32 < int64 < float64` widening policy to this executable slice

This fixes the current milestone-1 arithmetic typing rule without claiming it as the final shared contract for other operator families.

## First Post-Gate Filter Predicate Checkpoint

Issue #139 adds the first docs-first filter semantic checkpoint in
`docs/spec/first-filter-is-not-null.md`.

Issue #149 makes that predicate checkpoint executable in the local shared-kernel filter path. Issue #178 then extends executable predicate input to include `date32` for the first temporal slice. Issue #189 adds docs-first decimal predicate coverage for `decimal128`, and local executable kernel coverage now includes `decimal128` predicate input through `crates/tiforth-kernel/tests/decimal128_slice.rs`.

### `is_not_null(column(index))`

- requires one `column(index)` operand that resolves to logical type `int32`, `date32`, `decimal128`, or `float64` in current shared docs-first checkpoints
- derives logical result type `boolean`
- derives `nullable = false` for predicate evaluation
- evaluates row-wise as `true` for non-null input values and `false` for null input values
- reports an execution error when `index` is out of range
- reports an execution error rather than applying an implicit cast when the operand is outside the currently admitted checkpoint set
- local executable kernel coverage for this predicate currently includes `int32`, `date32`, and `decimal128` in the shared-kernel conformance slices; `float64` remains docs-first coverage in this checkpoint

## First Temporal Follow-On Checkpoint

Issue #174 adds a docs-first temporal checkpoint in
`docs/design/first-temporal-semantic-slice.md`.

Issue #176 adds the first docs-first temporal coverage anchors in:

- `tests/conformance/first-temporal-date32-slice.md`
- `tests/differential/first-temporal-date32-slice.md`
- `adapters/first-temporal-date32-slice.md`

Issue #178 adds the first executable local conformance coverage for this checkpoint in:

- `crates/tiforth-kernel/tests/temporal_date32_slice.rs`

For current shared contracts:

- the first admitted temporal logical type beyond milestone 1 is `date32`
- the first temporal checkpoint reuses existing expression and predicate
  families: passthrough `column(index)` plus `is_not_null(column(index))`
- this checkpoint does not define temporal arithmetic, ordering, casts,
  extraction, truncation, or timezone-aware timestamp semantics
- broader temporal families remain out of scope until follow-on issues define
  their semantics and coverage

## First Decimal Follow-On Checkpoint

Issue #189 adds a docs-first decimal checkpoint in
`docs/design/first-decimal-semantic-slice.md`.

Issue #189 also adds the first docs-first decimal coverage anchors in:

- `tests/conformance/first-decimal128-slice.md`
- `tests/differential/first-decimal128-slice.md`
- `adapters/first-decimal128-slice.md`

For current shared contracts:

- the first admitted decimal logical type beyond milestone 1 is
  `decimal128(precision, scale)`
- the first decimal checkpoint reuses existing expression and predicate
  families: passthrough `column(index)` plus `is_not_null(column(index))`
- this checkpoint preserves decimal precision and scale metadata and compares
  row outcomes through canonical decimal strings
- this checkpoint does not define decimal arithmetic, cast, coercion, or
  rounding semantics
- broader decimal families remain out of scope until follow-on issues define
  their semantics and coverage

Local executable decimal kernel conformance coverage now exists in
`crates/tiforth-kernel/tests/decimal128_slice.rs`.


## First Float64 NaN/Infinity Ordering Checkpoint

Issue #194 adds a docs-first float64 checkpoint in
`docs/design/first-float64-ordering-slice.md`.

Issue #194 also adds the first docs-first float64 coverage anchors in:

- `tests/conformance/first-float64-ordering-slice.md`
- `tests/differential/first-float64-ordering-slice.md`
- `adapters/first-float64-ordering-slice.md`

For current shared contracts:

- the first admitted floating checkpoint for NaN, infinity, and ordering
  semantics is `float64`
- this checkpoint reuses existing expression and predicate families:
  passthrough `column(index)` plus `is_not_null(column(index))`
- `NaN`, `Infinity`, `-Infinity`, `0.0`, and `-0.0` are non-null `float64`
  values for this checkpoint
- comparison intent treats `NaN` as unequal to any value (including `NaN`) and
  treats `0.0` and `-0.0` as compare-equal
- ordered-comparison intent treats comparisons involving `NaN` as false and
  orders non-NaN values as `-Infinity < finite < Infinity`
- canonical differential ordering for this checkpoint is
  `-Infinity < finite < Infinity < NaN`, with `-0.0` ordered before `0.0` only
  as a stable canonicalization tie-break
- broader floating behavior (arithmetic, coercion, casts, and SQL ordering
  policy) remains follow-on scope

## Open Questions

- TODO: extend the initial coercion lattice beyond `int32`, `int64`, and `float64`, including cross-family precedence boundaries
- TODO: define overflow behavior for operator families beyond the current milestone-1 `add<int32>` boundary
- TODO: define the first executable collation-sensitive string or
  binary slice, including shared collation identifiers (if needed),
  ordering and comparison semantics, and conformance plus differential
  coverage
- TODO: extend temporal semantics beyond the first `date32` checkpoint, including timezone-aware timestamp normalization and ordering rules
- TODO: extend decimal semantics beyond the first `decimal128` checkpoint, including arithmetic, cast/coercion, precision/scale propagation, and rounding behavior
- TODO: define JSON comparability and cast behavior

## Boundary For Now

This file should guide future specs and harness cases. It should not yet force low-level representation choices such as dictionary layout that belong in the data contract.
