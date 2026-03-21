# Initial Collation Scope And Ownership Boundary

Status: issue #143 design checkpoint

Verified: 2026-03-19

Related issues:

- #74 `spec: define milestone-1 int32 type-system boundary`
- #92 `design: define adapter-local runtime orchestration boundary`
- #139 `spec: define first filter semantic slice for is_not_null(column(index))`
- #143 `docs: define initial collation scope and ownership boundary`
- #233 `design: define first string collation semantic slice`

## Question

What collation semantics are required in shared `tiforth` contracts now, and which collation responsibilities stay adapter-local?

## Inputs Considered

- `docs/spec/type-system.md`
- `docs/spec/milestone-1-expression-projection.md`
- `docs/spec/first-filter-is-not-null.md`
- `docs/contracts/data.md`
- `docs/contracts/runtime.md`
- `docs/design/adapter-runtime-orchestration-boundary.md`
- `tests/conformance/expression-projection-slice.md`
- `tests/conformance/first-filter-is-not-null-slice.md`
- `tests/differential/first-expression-slice.md`
- `docs/design/first-collation-string-slice.md`
- issue #143
- issue #233

## Design Summary

The current shared boundary remains collation-light:

- milestone-1 executable slices are `int32`-only and therefore collation-insensitive
- current shared contracts do not require a runtime collation state, collation negotiation API, or collation metadata in batch handoff
- adapter implementations may still set engine session collation options as local orchestration policy, but that does not change shared case IDs or expected semantic outcomes
- future string or binary semantic slices must define explicit collation semantics before they enter shared executable or differential scope

## Shared Ownership

- shared specs own the semantic meaning of collation-sensitive behavior once such behavior is admitted into a shared slice
- shared conformance and differential docs own which cases are collation-sensitive and what outcomes are expected
- shared contracts own any later cross-adapter carrier fields, if needed, only after a follow-on issue proves they are required

## Adapter-Local Ownership

- engine-specific collation names, session variables, and transport details remain adapter-local
- adapter-local defaults, fallback choices, and environment wiring remain outside the shared runtime contract
- adapters must not reinterpret shared case semantics based on local collation defaults when a future shared slice defines explicit expectations

## Milestone-1 Boundary

- no milestone-1 shared executable checkpoint requires collation-sensitive expression families
- no milestone-1 fixture or differential artifact requires explicit collation fields
- milestone-1 docs should continue to treat executable collation-sensitive families as out of scope
- the first post-milestone docs-first collation checkpoint is now fixed in `docs/design/first-collation-string-slice.md`

## Why This Boundary

- it resolves the open ownership ambiguity without forcing premature string-family semantics
- it keeps milestone-1 scope aligned with current executable evidence
- it preserves adapter flexibility while protecting future cross-engine comparability once collation-sensitive cases are introduced

## Follow-On Boundary

Later issues may extend this checkpoint to define:

- executable adapter and differential-harness coverage for `first-collation-string-slice` beyond the first local kernel checkpoint
- broader shared collation identifier vocabulary beyond `binary` and `unicode_ci`
- cross-adapter normalization rules when engines expose different collation names or capabilities
- required conformance and differential case coverage for collation-sensitive behavior

## Result

Collation ownership is now explicit: milestone-1 shared slices stay collation-insensitive, adapter session-level collation choices remain local orchestration concerns, the first post-milestone collation slice is fixed in `docs/design/first-collation-string-slice.md`, and first local executable kernel coverage now exists in `crates/tiforth-kernel/tests/collation_string_slice.rs`.
