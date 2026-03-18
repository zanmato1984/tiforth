# First Expression Slice TiFlash Compatibility Notes

Status: issue #107 inventory checkpoint

Verified: 2026-03-18

Related issues:

- #68 `design: define first differential expression slice and drift report format`
- #72 `design: define first differential adapter request/response surface`
- #84 `design: define first inventoried function and operator families`
- #107 `inventory: add first-expression-slice TiFlash compatibility notes`

## Purpose

This note records TiFlash-side compatibility evidence for the first-wave inventory chosen in `docs/design/first-inventory-wave.md`.

It scopes only to the `first-expression-slice` semantic surface:

- the single-input projection operator
- `column(index)` as normalized by the shared slice refs
- `literal<int32>(value)`
- `add<int32>(lhs, rhs)`

This artifact treats TiFlash docs and TiFlash source tests as evidence, not as shared design authority.

## TiFlash Snapshot

- PingCAP docs repo commit: `f965c033583e737843c0cdb362f248415db198ec`
- TiFlash source repo commit: `7fe4b3b8f765f72ed1e5490c1da299776669f4f2`
- public docs checked: current stable TiFlash pushdown calculations, numeric functions and operators, numeric types, plus the current `dev` literal-values page on 2026-03-18

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

## Reviewed TiFlash Sources

- `https://docs.pingcap.com/tidb/stable/tiflash-supported-pushdown-calculations/`
- `https://docs.pingcap.com/tidb/stable/numeric-functions-and-operators/`
- `https://docs.pingcap.com/tidb/stable/data-type-numeric/`
- `https://docs.pingcap.com/tidb/dev/literal-values/`
- `https://github.com/pingcap/docs/blob/f965c033583e737843c0cdb362f248415db198ec/tiflash/tiflash-supported-pushdown-calculations.md`
- `https://github.com/pingcap/docs/blob/f965c033583e737843c0cdb362f248415db198ec/data-type-numeric.md`
- `https://github.com/pingcap/docs/blob/f965c033583e737843c0cdb362f248415db198ec/literal-values.md`
- `https://github.com/pingcap/tiflash/blob/7fe4b3b8f765f72ed1e5490c1da299776669f4f2/tests/tidb-ci/fullstack-test-dt/projection_push_down.test`
- `https://github.com/pingcap/tiflash/blob/7fe4b3b8f765f72ed1e5490c1da299776669f4f2/tests/tidb-ci/fullstack-test-dt/expr_push_down.test`
- `https://github.com/pingcap/tiflash/blob/7fe4b3b8f765f72ed1e5490c1da299776669f4f2/tests/fullstack-test/expr/null_literal.test`

## Compatibility Notes

### Projection Output Shape And Naming

#### TiFlash Surface

- TiFlash docs say `Project` is a supported push-down operator.
- TiFlash docs also say that when an operator contains expressions, all of those expressions must themselves be push-down compatible before the operator can be pushed to TiFlash.
- reviewed TiFlash projection tests show unaliased computed outputs surfacing with engine-native names such as `id+1` and `value+1`
- the same tests show aliased derived outputs surfacing with the chosen alias, for example `select id - 2 as b` producing output column `b`

#### Recorded TiFlash Facts

- TiFlash can execute pushed-down projection calculations over the reviewed numeric cases
- unaliased projection expressions expose engine-native expression text as result column names in reviewed TiFlash evidence
- explicit aliases are the adapter-local way to preserve stable shared field names when the raw expression text would otherwise surface as the field name

#### Evidence Gaps

- the reviewed sources did not include a TiFlash-specific schema-metadata example pinned to the shared one-column `column-a` case
- this note therefore treats stable shared field naming as something the future adapter should preserve explicitly rather than as a TiFlash docs detail fully re-proved here

### `column(index)` / Passthrough Projection

#### TiFlash Surface

- TiFlash docs describe pushdown in TiDB SQL terms, with explain output referring to named fields such as `test.t.id` and pushed-down expressions such as `plus(test.t.id, test.t.a)`
- reviewed TiFlash tests use named columns such as `id` and `value` in pushed-down projection queries

#### Recorded TiFlash Facts

- TiFlash surface syntax uses named SQL columns, not a repository-local positional spelling such as `column(index)`
- inference from reviewed sources: the shared `projection_ref = column-a` should lower into adapter-local TiFlash SQL that selects the named field `a` from the documented shared input

#### Evidence Gaps

- the reviewed TiFlash docs and tests did not show a direct positional field-reference syntax corresponding to the shared `column(index)` vocabulary
- positional `column(index)` therefore remains shared-harness normalization rather than a TiFlash surface fact

### `literal<int32>(value)`

#### TiFlash Surface

- TiDB/TiFlash literal-value docs say numeric literals include integers with optional leading `+` or `-` signs and that `NULL` is a literal value
- reviewed TiDB numeric-type docs define `INT` / `INTEGER` with the signed range `-2147483648` to `2147483647`, matching the shared `int32` range exposed to TiFlash through the TiDB SQL layer
- reviewed TiFlash null-literal tests exercise arithmetic, comparison, logical, conditional, and cast expressions with `NULL` operands under `tidb_isolation_read_engines='tiflash'`
- the reviewed TiFlash null-literal tests also note that bare `NULL` literal expressions are not directly folded by TiDB for TiFlash pushdown and therefore use `CASE ... WHEN NULL THEN NULL END` forms to emulate that shape

#### Recorded TiFlash Facts

- TiFlash receives integer and `NULL` literals through the TiDB SQL surface rather than through a separate shared literal carrier
- TiFlash reviewed tests explicitly cover `NULL` operands flowing through pushed-down arithmetic and comparison expressions
- inference from reviewed sources: the future TiFlash adapter can spell `literal-int32-seven` with ordinary SQL integer literal syntax, but typed-null materialization for `literal-int32-null` may require adapter-local shaping because the reviewed TiFlash tests call out current TiDB folding limits for bare `NULL` expressions on the TiFlash path

#### Evidence Gaps

- the reviewed sources did not pin a direct `SELECT 7 AS ...` or `SELECT CAST(NULL AS ... ) AS ...` metadata example to the shared `logical_type = int32` contract
- the reviewed sources did not re-prove how a standalone literal projection is typed after TiDB-to-TiFlash planning
- stable field naming and exact `int32` typing for the shared literal cases therefore remain adapter and harness evidence to confirm later

### `add<int32>(lhs, rhs)`

#### TiFlash Surface

- TiFlash pushdown docs list `+` as a supported push-down expression and show `id + a` being pushed down to TiFlash in explain output
- reviewed TiFlash projection-pushdown tests execute queries such as `select id+1, value+1 from t order by id` under TiFlash-oriented settings and show `NULL` inputs yielding `NULL` results in the computed columns

#### Recorded TiFlash Facts

- TiFlash has a first-class pushed-down arithmetic `+` surface suitable for the shared `add-a-plus-one` family
- reviewed TiFlash tests show row-count preservation across pushed-down computed projection and show `NULL` propagation through `id + 1` / `value + 1` results in the tested data
- inference from reviewed sources: TiFlash can likely execute the shared non-overflow `add<int32>` cases directly once the adapter lowers them into ordinary SQL projection form

#### Evidence Gaps

- the reviewed TiFlash docs and source tests did not surface a direct `2147483647 + 1` example or a stable TiFlash-specific error family pinned to the shared `add-int32-overflow-error` checkpoint
- the reviewed sources also did not pin exact output metadata for a pushed-down `INT + INT` projection back to the shared `logical_type = int32`
- the shared overflow checkpoint and any required narrowing or drift decision should therefore remain executable adapter evidence rather than a docs-only conclusion in this inventory pass

## Boundary For This Artifact

- this note records TiFlash-side compatibility evidence only for the first-wave single-input projection, `column`, `literal<int32>`, and `add<int32>` family
- it does not define the shared adapter request and response contract; that remains in `adapters/first-expression-slice.md`
- it does not add executable TiFlash adapter code, checked-in case-result captures, or pairwise drift reports
- TiDB compatibility notes remain a separate inventory artifact
