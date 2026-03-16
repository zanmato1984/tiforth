# Data Contract

Direction: use Apache Arrow concepts as the default memory and data interchange contract.

This is a direction, not a frozen ABI.

## Working Principles

- columnar batches are the unit of exchange
- schema is explicit and travels with enough metadata to validate semantics
- nullability is first-class
- vectors should be usable by harnesses and future kernels without engine-specific wrappers
- zero-copy reuse is preferred when ownership rules allow it

## Scope

This contract should eventually cover:

- scalar columns
- selection or validity state
- batch schema metadata
- basic encoding expectations
- handoff rules between runtime stages

## Non-Goals

- inventing a new memory format when Arrow already covers the need
- freezing every physical detail before the first harness exists
- encoding engine-specific quirks directly into the shared contract

## Open Questions

- TODO: define the canonical batch envelope used by tests, adapters, and future kernels
- TODO: define ownership and lifetime rules for buffers across stage boundaries
- TODO: decide how dictionary encoding is treated in the shared contract
- TODO: specify required support for nested types, if any, in the first milestone
- TODO: specify decimal and temporal metadata requirements
- TODO: decide how spill or off-heap behavior is represented, if at all

## Initial Boundary

For now, this document is a semantic contract placeholder. It should be specific enough to guide harness design, but not so specific that it locks the reboot into premature implementation choices.
