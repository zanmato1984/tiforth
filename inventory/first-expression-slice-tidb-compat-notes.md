# First Expression Slice TiDB Compatibility Notes

Status: issue #105 inventory checkpoint

Verified: 2026-03-18

Related issues:

- #68 `design: define first differential expression slice and drift report format`
- #72 `design: define first differential adapter request/response surface`
- #84 `design: define first inventoried function and operator families`
- #105 `inventory: add first-expression-slice TiDB compatibility notes`

## Purpose

This note records TiDB-side compatibility evidence for the first-wave inventory chosen in `docs/design/first-inventory-wave.md`.

It scopes only to the `first-expression-slice` semantic surface:

- the single-input projection operator
- `column(index)` as normalized by the shared slice refs
- `literal<int32>(value)`
- `add<int32>(lhs, rhs)`

This artifact treats TiDB docs and TiDB source tests as evidence, not as shared design authority.

## TiDB Snapshot

- PingCAP docs repo commit: `f965c033583e737843c0cdb362f248415db198ec`
- TiDB source repo commit: `1871ffa3cc15f8e3dd68605b52902dd191ecad6e`
- public docs checked: current `stable` docs plus the current `dev` literal-values page on 2026-03-18

## Shared Slice Anchors

This note stays anchored to the stable slice vocabulary already defined in `tests/differential/first-expression-slice.md`.

- `slice_id = first-expression-slice`
- `projection_ref = column-a`
- `projection_ref = literal-int32-seven`
- `projection_ref = literal-int32-null`
- `projection_ref = add-a-plus-one`
- `case_id = column-passthrough`
- `case_id = literal-int32-seven`
- `case_id = literal-int32-null`
- `case_id = add-int32-literal`
- `case_id = add-int32-null-propagation`
- `case_id = add-int32-overflow-error`

## Reviewed TiDB Sources

- `https://docs.pingcap.com/tidb/stable/functions-and-operators-overview/`
- `https://docs.pingcap.com/tidb/stable/numeric-functions-and-operators/`
- `https://docs.pingcap.com/tidb/stable/dev-guide-use-views/`
- `https://docs.pingcap.com/tidb/dev/literal-values/`
- `https://github.com/pingcap/docs/blob/f965c033583e737843c0cdb362f248415db198ec/data-type-numeric.md`
- `https://github.com/pingcap/docs/blob/f965c033583e737843c0cdb362f248415db198ec/functions-and-operators/precision-math.md`
- `https://github.com/pingcap/docs/blob/f965c033583e737843c0cdb362f248415db198ec/sql-statements/sql-statement-show-columns-from.md`
- `https://github.com/pingcap/tidb/blob/1871ffa3cc15f8e3dd68605b52902dd191ecad6e/tests/integrationtest/r/types/const.result`
- `https://github.com/pingcap/tidb/blob/1871ffa3cc15f8e3dd68605b52902dd191ecad6e/pkg/expression/builtin_arithmetic_test.go`
- `https://github.com/pingcap/tidb/blob/1871ffa3cc15f8e3dd68605b52902dd191ecad6e/tests/integrationtest/r/expression/builtin.result`

## Compatibility Notes

### Projection Output Shape And Naming

#### TiDB Surface

- TiDB views use the `SELECT` statement that defines the view as the source of schema shape.
- TiDB examples show select-list aliases such as `book_id`, `book_title`, and `average_score` becoming the exposed view column names.
- TiDB also documents the unaliased case: `CREATE VIEW v1 AS SELECT 1;` yields a view column named `1` with type `bigint` and `Null = YES` in `SHOW COLUMNS` output.

#### Recorded TiDB Facts

- TiDB projection schema is driven by the select list, not by a separate shared expression carrier.
- output aliases are the adapter-local way to preserve stable shared field names when the raw expression text would otherwise surface as the field name
- unaliased literal projections widen to TiDB `bigint` metadata in the reviewed `SHOW COLUMNS` example, even for the literal `1`

#### Evidence Gaps

- the reviewed docs did not include a one-column `SELECT a FROM t` / `SHOW COLUMNS` example for the shared `column-a` case
- this note therefore treats stable passthrough output naming as something the future TiDB adapter should preserve explicitly, not as a TiDB docs detail fully re-proved here

### `column(index)` / Passthrough Projection

#### TiDB Surface

- TiDB's function and operator reference says expressions can use literals, column names, `NULL`, built-in functions, and operators.
- reviewed TiDB view examples use name-based column references such as `b.id` inside the select list.

#### Recorded TiDB Facts

- TiDB exposes column references through SQL column names, not through a repository-local positional spelling such as `column(index)`
- inference from reviewed sources: the shared `projection_ref = column-a` should lower into adapter-local TiDB SQL that selects the named field `a` from the one-column shared input

#### Evidence Gaps

- the reviewed TiDB docs and source tests did not show a direct positional field-reference syntax corresponding to the shared `column(index)` vocabulary
- positional `column(index)` therefore remains shared-harness normalization rather than a TiDB surface fact

### `literal<int32>(value)`

#### TiDB Surface

- TiDB supports exact numeric literals, optional leading `+` and `-` signs, and `NULL` literals.
- TiDB's documented integer types include `INT` / `INTEGER` with the signed range `-2147483648` to `2147483647`, matching the shared `int32` range.
- TiDB's reviewed `SHOW COLUMNS` example for `SELECT 1` reports `bigint` rather than `int` for the resulting output field.

#### Recorded TiDB Facts

- TiDB can represent the shared `int32` value range in table schema because its documented `INT` type matches the shared signed 32-bit range
- TiDB supports both integer literals and `NULL` literals in SQL expressions
- a bare literal projection does not automatically preserve the shared `int32` output type; reviewed TiDB metadata surfaces an unaliased integer literal as nullable `bigint`
- inference from reviewed sources: a future TiDB adapter will likely need an explicit narrowing step plus an explicit alias if it wants `literal-int32-seven` and `literal-int32-null` to normalize back into shared `logical_type = int32` with stable field names

#### Evidence Gaps

- the reviewed docs did not show a direct `SELECT CAST(7 AS ... ) AS lit` metadata example pinned to the shared `int32` output contract
- the reviewed docs and source tests did not show a standalone `SELECT CAST(NULL AS ... )` result pinned to TiDB `INT` metadata
- typed-null materialization for the shared `literal-int32-null` case therefore remains adapter and harness evidence to confirm later

### `add<int32>(lhs, rhs)`

#### TiDB Surface

- TiDB documents `+` as a supported arithmetic operator.
- TiDB's precision-math docs say exact integer-only numeric expressions are evaluated using integer arithmetic with `BIGINT` precision.
- reviewed TiDB source tests and result files show arithmetic overflow surfacing as `Error 1690 (22003)` / `value is out of range` on arithmetic expressions once the engine exceeds its numeric boundary.

#### Recorded TiDB Facts

- TiDB has a first-class arithmetic `+` operator suitable for the shared `add-a-plus-one` family
- TiDB does perform arithmetic overflow checks and exposes a stable out-of-range error family in reviewed engine evidence
- inference from reviewed sources: TiDB's documented integer-expression evaluation uses `BIGINT` precision, so a straightforward translation of `add-a-plus-one` over `INT` inputs does not automatically preserve the shared `add<int32>` overflow boundary at `2147483647 + 1`
- the shared `add-int32-overflow-error` checkpoint therefore looks like a likely TiDB compatibility wrinkle: the future adapter will need either an explicit narrowing strategy or an explicit drift / unsupported decision if TiDB cannot reproduce the shared `int32` overflow boundary through a narrow SQL spelling

#### Evidence Gaps

- the reviewed docs and source tests did not surface a direct `INT + INT` example at the shared `2147483647 + 1` boundary
- the reviewed sources did not surface a direct `NULL + 1` TiDB example for the shared `add-int32-null-propagation` case
- null-propagation and exact overflow behavior for the shared `add<int32>` slice should therefore remain executable adapter evidence rather than overclaimed docs-only fact in this inventory pass

## Boundary For This Artifact

- this note records TiDB-side compatibility evidence only for the first-wave single-input projection, `column`, `literal<int32>`, and `add<int32>` family
- it does not define the shared adapter request and response contract; that remains in `adapters/first-expression-slice.md`
- it does not add executable TiDB adapter code, checked-in case-result captures, or pairwise drift reports
- TiFlash compatibility notes remain a separate follow-on inventory artifact
