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

## Milestone-1 Allocation Contract

For milestone 1, `tiforth` does **not** need to route its internal Arrow allocations directly through a host allocator.

The required contract is:

- before allocating internal memory for Arrow-backed operator state or output, `tiforth` estimates the additional bytes required for the intended peak live growth and reports that request to the host
- the request is for additional admitted bytes, not a retroactive accounting correction after allocation has already happened
- the host either admits or denies that reservation request as an all-or-nothing control point
- if the host denies the request, `tiforth` must fail that allocation or execution path or spill and retry under operator policy, and it must not allocate first
- if the host admits the request, `tiforth` may allocate internally and then materialize ordinary Arrow buffers
- if actual growth uses fewer admitted bytes than reserved, `tiforth` returns the excess through `shrink`
- if later growth needs more bytes, `tiforth` must obtain another reservation before that growth happens
- when admitted buffers or operator state stop being resident, `tiforth` returns those bytes through `shrink` or final `release`

This keeps host memory control explicit while allowing milestone-1 data construction to use `tiforth`-owned internal allocation paths.

Detailed admission semantics live in `docs/design/host-memory-admission-abi.md`.

## Open Questions

- TODO: define the canonical batch envelope used by tests, adapters, and future kernels
- TODO: define ownership and lifetime rules for buffers across stage boundaries, including internally allocated buffers admitted by the host and any future imported immutable buffers whose free path may live in Go, C++, or Rust
- TODO: define how milestone-1 operators estimate intended memory before allocation and how that estimate is reconciled with actual allocated bytes
- TODO: decide how dictionary encoding is treated in the shared contract
- TODO: specify required support for nested types, if any, in the first milestone
- TODO: specify decimal and temporal metadata requirements
- TODO: decide how spill or off-heap behavior is represented, if at all, given that spill is operator-managed rather than transparent inside Arrow allocation paths
- TODO: decide whether any later milestone needs direct host-allocator-backed Arrow buffers or imported immutable buffer bridges beyond the reserve-first milestone-1 contract

## Initial Boundary

For now, this document is a semantic contract placeholder. It should be specific enough to guide harness design, but not so specific that it locks the reboot into premature implementation choices.
