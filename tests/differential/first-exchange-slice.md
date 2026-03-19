# First Differential Exchange Slice

Status: issue #169 design checkpoint, issue #183 harness checkpoint

Related issues:

- #125 `design: define milestone-1 exchange runtime mapping boundary`
- #169 `design: define first in-contract exchange slice boundary`
- #183 `harness: wire first-exchange-slice differential parity checkpoint`

## Question

How should differential harnesses verify that introducing the first in-contract
exchange boundary does not change already-defined cross-engine outcomes?

## Inputs Considered

- `docs/design/first-in-contract-exchange-slice.md`
- `tests/differential/first-expression-slice.md`
- `tests/differential/first-filter-is-not-null-slice.md`
- `tests/differential/first-expression-slice-artifacts.md`
- `tests/differential/first-filter-is-not-null-slice-artifacts.md`
- issue #169
- issue #183

## First Differential Decision

The first differential exchange checkpoint is parity-focused, not semantics-expanding.

### 1. Engines

Use the existing first pair: `TiDB` and `TiFlash`.

### 2. Slice Groups

Reuse existing documented case IDs without renaming them:

- `first-expression-slice`
- `first-filter-is-not-null-slice`

### 3. Comparison Rule

When those case groups are executed through an exchange-enabled path, paired
engine outcomes must stay semantically equivalent to baseline non-exchange
runs on these dimensions:

- normalized `outcome.kind`
- normalized `error_class` for error cases
- row schema, row values, and row count for row cases
- drift classification meanings already defined by existing slice artifact docs

### 4. Out Of Scope

- new exchange-only expression or predicate semantics
- new adapter request fields beyond existing case refs
- TiKV expansion
- exchange transport details that remain adapter-local orchestration

## Executable Harness Wiring

Issue #183 now provides executable parity wiring in:

- `crates/tiforth-harness-differential/src/first_exchange_slice.rs`

That harness executes baseline and exchange paths for:

- `first-expression-slice`
- `first-filter-is-not-null-slice`

It compares normalized per-engine case outcomes plus per-case TiDB-versus-TiFlash
drift status so the checkpoint can detect both result-surface regressions and
drift-classification changes without introducing new slice IDs.
