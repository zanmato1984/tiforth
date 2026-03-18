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
- arithmetic behavior
- function signature matching
- result type derivation

## Representation Boundary

- Arrow physical encodings such as dictionary layout do not change shared semantic type identity by themselves
- current specs and harnesses should normalize dictionary-backed evidence to the underlying logical type before comparison
- representation-level rules for when dictionary arrays may cross a shared stage boundary live in `docs/contracts/data.md`

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

## Current Milestone-1 Boundary

The current executable slice only fixes the type behavior needed for milestone-1 projection coverage.

### Covered Expression Families

- `column(index)`
- `literal<int32>(value)`
- `add<int32>(lhs, rhs)`

The lattice and precedence policy above is a shared type-system checkpoint for later slices. The current executable milestone-1 path still uses exact `int32` typing for arithmetic.

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

## Open Questions

- TODO: extend the initial coercion lattice beyond `int32`, `int64`, and `float64`, including cross-family precedence boundaries
- TODO: define signed versus unsigned interaction rules
- TODO: define overflow behavior for operator families beyond the current milestone-1 `add<int32>` boundary
- TODO: define NaN, infinity, and ordering semantics
- TODO: define collation scope and ownership
- TODO: define timezone handling and temporal normalization
- TODO: define decimal precision and scale propagation rules
- TODO: define JSON comparability and cast behavior

## Boundary For Now

This file should guide future specs and harness cases. It should not yet force low-level representation choices such as dictionary layout that belong in the data contract.
