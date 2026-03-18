# First Inventory Wave For Function And Operator Families

Status: issue #84 design checkpoint

Verified: 2026-03-18

Related issues:

- #10 `milestone-1: first Arrow-bound operator and expression slice`
- #68 `design: define first differential expression slice and drift report format`
- #72 `design: define first differential adapter request/response surface`
- #74 `spec: define milestone-1 int32 type-system boundary`
- #80 `design: define the next thin end-to-end slice after milestone-1 projection`
- #82 `design: define acceptance criteria for expanding the kernel beyond milestone-1`
- #84 `design: define first inventoried function and operator families`

## Question

Which function and operator families should donor and engine inventory target first now that the milestone-1 projection slice and the first differential expression slice are documented?

## Inputs Considered

- `README.md`
- `docs/vision.md`
- `docs/architecture.md`
- `docs/spec/milestone-1-expression-projection.md`
- `docs/spec/type-system.md`
- `docs/spec/functions/README.md`
- `docs/spec/operators/README.md`
- `tests/conformance/expression-projection-slice.md`
- `tests/differential/first-expression-slice.md`
- `adapters/first-expression-slice.md`
- `docs/design/next-thin-end-to-end-slice.md`
- `docs/design/kernel-expansion-acceptance.md`
- `inventory/README.md`
- issue #84

## Design Summary

The first inventory wave should stay inside the current milestone-1 semantic core and the already-documented first differential slice.

Start with these families together:

- the single-input projection operator, including output order, output naming, direct column passthrough, and computed-column materialization boundaries
- the expression-leaf families `column(index)` and `literal<int32>(value)`
- the first arithmetic family `add<int32>(lhs, rhs)`, including null propagation, overflow, and current non-`int32` rejection behavior

This keeps donor extraction and engine compatibility notes aligned with the only accepted shared-kernel boundary and with the next thin end-to-end checkpoint from `docs/design/next-thin-end-to-end-slice.md`.

## Why These Families First

- they already have stable spec and harness anchors in the milestone-1 expression slice and in `tests/differential/first-expression-slice.md`
- they cover both one operator family and the smallest useful expression families without inventing a broader SQL catalog too early
- they line up with the current `int32`-only type-system boundary from `docs/spec/type-system.md`
- they give future donor catalogs, engine compatibility notes, and coverage-gap artifacts one narrow, reviewable scope that can feed the first executable differential harness directly

## Scope For The First Wave

### 1. Projection Operator Family

Inventory work should capture the shared semantic surface for:

- one input batch projected into one output batch
- output expression order and output-field naming
- direct column reuse versus computed-column materialization
- row-count preservation across passthrough, literal, and `add<int32>` outputs

For this first wave, inventory should describe the shared semantic behavior only. Local runtime-event, admission-event, and ownership-trace checkpoints remain conformance evidence, not first-wave operator inventory targets.

### 2. Expression Leaf Families

Inventory work should capture the shared semantic surface for:

- `column(index)` as the direct input-reference path used by passthrough projection
- `literal<int32>(value)` for both non-null and `NULL` literals
- current naming, nullability, and type-derivation rules attached to those leaf expressions

When donor or engine material uses different syntax, inventory should still map that behavior back to the shared semantic refs rather than promoting engine-native SQL text into the source of truth.

### 3. First Arithmetic Family

Inventory work should capture the shared semantic surface for:

- `add<int32>(lhs, rhs)` over the documented `first-expression-slice` inputs
- row-wise null propagation
- overflow as an execution error for the current milestone-1 boundary
- rejection of implicit widening or non-`int32` arithmetic within this checkpoint

This wave should treat the current overflow checkpoint as shared semantic evidence. Broader arithmetic promotion and coercion rules stay deferred until a later spec issue names them explicitly.

## Inventory Anchors And Expected Outputs

Follow-on extraction issues for this wave should keep their evidence anchored to the stable refs that already exist for `first-expression-slice`, including `slice_id`, `case_id`, `input_ref`, and `projection_ref`.

Expected first-wave artifact shapes include:

- one donor function catalog scoped to `literal<int32>` and `add<int32>`
- one donor operator catalog scoped to the single-input projection family, including the direct `column(index)` passthrough path
- engine compatibility notes for TiDB and TiFlash over the same shared slice
- a coverage-gap note only when a documented family edge is intentionally deferred from the first differential slice

Those artifacts should follow the naming and refresh rules in `docs/process/inventory-artifact-naming.md` and `docs/process/inventory-refresh.md`.

Example filenames for follow-on issues could include:

- `first-expression-slice-legacy-function-catalog.md`
- `first-expression-slice-legacy-operator-catalog.md`
- `first-expression-slice-tidb-compat-notes.md`
- `first-expression-slice-tiflash-compat-notes.md`

This issue does **not** create those artifacts yet. It only fixes the first priority so later inventory work does not need to guess where to start.

## Deferred Families

Do **not** broaden the first inventory wave to include these yet:

- comparison and boolean predicate families
- cast and coercion behavior beyond the current milestone-1 `int32` boundary
- wider arithmetic families such as subtraction, multiplication, division, unsigned arithmetic, decimal arithmetic, or floating-point arithmetic
- filter, join, aggregate, sort, exchange, or other multi-operator runtime families
- string, temporal, JSON, and collation-sensitive function families
- TiKV-specific lower-level expression or storage families

Those become valid follow-on inventory candidates after the first executable differential slice exists or when another accepted docs issue identifies one of them as the next smallest blocked semantic surface.

## Follow-On Boundary

This issue sets the first inventory priority only.

It does **not**:

- extract donor catalogs yet
- add executable differential harness code
- widen the shared kernel beyond the current milestone-1 slice
- choose the first post-gate kernel expansion candidate
