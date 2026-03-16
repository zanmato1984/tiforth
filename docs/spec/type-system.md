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
- internal intermediate types, if later plans require them

## Required Semantic Areas

- type identity
- nullability
- coercion and cast rules
- comparison behavior
- arithmetic behavior
- function signature matching
- result type derivation

## Open Questions

- TODO: define the coercion lattice and precedence rules
- TODO: define signed versus unsigned interaction rules
- TODO: define overflow behavior by operator family
- TODO: define NaN, infinity, and ordering semantics
- TODO: define collation scope and ownership
- TODO: define timezone handling and temporal normalization
- TODO: define decimal precision and scale propagation rules
- TODO: define JSON comparability and cast behavior

## Boundary For Now

This file should guide future specs and harness cases. It should not yet force low-level representation choices that belong in the data contract.
