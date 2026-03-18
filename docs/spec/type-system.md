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

## Current Milestone-1 Boundary

The current executable slice only fixes the type behavior needed for milestone-1 projection coverage.

### Covered Expression Families

- `column(index)`
- `literal<int32>(value)`
- `add<int32>(lhs, rhs)`

No broader coercion lattice, unsigned arithmetic contract, decimal propagation rule, or floating-point ordering rule is implied by this milestone-1 checkpoint.

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

This fixes the current milestone-1 arithmetic typing rule without claiming it as the final shared contract for other operator families.

## Open Questions

- TODO: define the coercion lattice and precedence rules
- TODO: define signed versus unsigned interaction rules
- TODO: define overflow behavior for operator families beyond the current milestone-1 `add<int32>` boundary
- TODO: define NaN, infinity, and ordering semantics
- TODO: define collation scope and ownership
- TODO: define timezone handling and temporal normalization
- TODO: define decimal precision and scale propagation rules
- TODO: define JSON comparability and cast behavior

## Boundary For Now

This file should guide future specs and harness cases. It should not yet force low-level representation choices such as dictionary layout that belong in the data contract.
