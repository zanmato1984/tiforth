# First Post-Gate Kernel Expansion Boundary

Status: issue #137 design checkpoint

Verified: 2026-03-18

Related issues:

- #10 `milestone-1: first Arrow-bound operator and expression slice`
- #80 `design: define the next thin end-to-end slice after milestone-1 projection`
- #82 `design: define acceptance criteria for expanding the kernel beyond milestone-1`
- #113 `harness: compare first-expression-slice results for TiDB and TiFlash`
- #129 `inventory: add first-expression-slice coverage-gap artifact`
- #137 `design: choose first post-gate kernel expansion boundary`
- #139 `spec: define first filter semantic slice for is_not_null(column(index))`

## Question

Now that the first executable differential checkpoint exists, which single shared-kernel boundary should come first after milestone-1 projection?

## Inputs Considered

- `docs/vision.md`
- `docs/architecture.md`
- `docs/design/next-thin-end-to-end-slice.md`
- `docs/design/kernel-expansion-acceptance.md`
- `docs/spec/milestone-1-expression-projection.md`
- `docs/spec/type-system.md`
- `tests/conformance/expression-projection-slice.md`
- `tests/differential/first-expression-slice.md`
- `inventory/first-expression-slice-tidb-vs-tiflash-drift-report.md`
- `inventory/first-expression-slice-coverage-gap.md`
- issue #137

## Design Summary

The first post-gate kernel expansion boundary is a narrow filter slice:

- add one row-filter operator boundary to the shared kernel
- keep predicate scope to one `int32` null-check family for the first checkpoint (`is_not_null(column(index))`)
- preserve row order and preserve existing column values for kept rows
- keep the current expression-projection operator behavior unchanged for cases that do not use the new filter boundary

This is the smallest layer-3 expansion that introduces a new relational capability (row selection) while keeping type and expression scope intentionally narrow.

## Why This Boundary First

- The first executable differential slice proved the current projection expression core is stable enough to anchor one next capability.
- The current shared kernel still cannot express row-count-changing semantics.
- A filter boundary is a direct relational step up from projection and is reviewable as one dimension.
- Restricting the first predicate family to `is_not_null(column(index))` keeps the scope thin and avoids bundling broader comparison or boolean-algebra work into one issue.

## Boundary

### In Scope For The First Filter Expansion Issue

- one shared filter operator boundary over Arrow batches
- one predicate family: `is_not_null(column(index))`
- deterministic row-retention semantics:
  - keep rows where the predicate evaluates to `true`
  - drop rows where the predicate evaluates to `false`
  - preserve input row order for all retained rows
- local conformance and first differential checkpoints that prove the filter path over the shared request and artifact carriers

### Out Of Scope

- general boolean expression trees (`and`, `or`, `not`)
- numeric comparison families (`eq`, `lt`, `gt`, etc.)
- widening arithmetic or coercion behavior beyond the current milestone-1 `int32` core
- exchange, join, aggregate, sort, or other multi-operator families
- adapter-local execution orchestration policy (session setup, retry, timeout, cancellation transport)

## Required Docs And Harness Prep Before Code

A follow-on implementation issue for this boundary should name and update, or explicitly mark unchanged, all of these surfaces before kernel code lands:

1. one filter semantic spec under `docs/spec/` covering predicate truth table, null handling, row-order guarantees, and error behavior
2. `docs/spec/type-system.md` entries for the first predicate result typing and nullability behavior
3. `docs/contracts/data.md` and `docs/contracts/runtime.md` notes only where the new filter handoff or ownership behavior changes shared boundaries
4. conformance cases under `tests/conformance/` for keep, drop, and mixed-row outcomes
5. one differential slice/checkpoint update under `tests/differential/` that compares the new filter behavior across TiDB and TiFlash
6. adapter boundary updates under `adapters/` only where the shared filter request or normalized result fields must grow

Issue #139 now establishes docs-first checkpoints for items 1, 2, and 4 through `docs/spec/first-filter-is-not-null.md`, `docs/spec/type-system.md`, and `tests/conformance/first-filter-is-not-null-slice.md`.

## Completion Signal For This Decision

This issue is complete when the repository has one explicit post-gate boundary selection that a future implementation issue can reference directly without reopening candidate selection.

The resulting implementation issue should be able to say:

- which exact filter boundary is being implemented
- which docs and harness checkpoints prove completion
- which adjacent operator and expression families remain explicitly out of scope

## Follow-On Boundary

After the first filter boundary lands with evidence, later issues may choose the next single expansion dimension, such as broader predicate families or one additional operator family.

Until then, post-gate shared-kernel growth should stay anchored to this first filter boundary selection.
