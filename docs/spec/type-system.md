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
- collation-sensitive comparison and ordering behavior
- arithmetic behavior
- function signature matching
- result type derivation

## Representation Boundary

- Arrow physical encodings such as dictionary layout do not change shared semantic type identity by themselves
- current specs and harnesses should normalize dictionary-backed evidence to the underlying logical type before comparison
- representation-level rules for when dictionary arrays may cross a shared stage boundary live in `docs/contracts/data.md`
- milestone-1 nested plus decimal and temporal family scope boundaries live in `docs/design/milestone-1-nested-decimal-temporal-boundary.md`

## Initial Collation Scope And Ownership Checkpoint

Issue #143 adds a docs-first checkpoint in `docs/design/collation-scope-ownership.md`.

For current shared contracts:

- milestone-1 executable slices remain collation-insensitive because they are
  limited to the current `int32` projection and first filter predicate
  boundaries
- shared contracts do not yet require runtime collation state,
  batch-level collation metadata, or a shared collation negotiation API
- adapter-local session and engine collation settings remain
  orchestration details unless and until a later shared slice defines
  explicit collation-sensitive semantics

This checkpoint resolves ownership scope without claiming executable string or binary collation behavior in milestone 1.

## Initial Coercion Lattice And Precedence

This checkpoint defines the first shared implicit-coercion policy for future expression and harness slices beyond the current executable `add<int32>` path.

### Lattice Nodes And Implicit Edges

- identity coercion is always allowed (`T -> T`)
- numeric widening for this checkpoint is limited to:
  - `int32 -> int64`
  - `int64 -> float64`
  - `int32 -> float64` (via widening)
- no other implicit coercions are currently allowed
- boolean, unsigned integer, decimal, string or binary, temporal, and JSON families require exact type matches until follow-up issues define their coercion edges
- untyped `NULL` may adopt the expected type from an already-selected signature, but it does not drive signature selection on its own

### Precedence Rules For Signature Selection

When multiple operator or function signatures are possible:

1. choose an exact-type match when one exists
2. otherwise choose the candidate that needs the fewest implicit coercions
3. for numeric candidates in this checkpoint, choose the least upper bound under `int32 < int64 < float64`
4. if no unique candidate can be selected, raise an execution error instead of guessing

This keeps signature selection deterministic while preserving explicit errors for unsupported or ambiguous type combinations.

### Signed Versus Unsigned Interaction Checkpoint

Issue #141 defines the first shared signed-versus-unsigned interaction
boundary for this initial lattice checkpoint.

- no implicit coercions are allowed between signed and unsigned integer
  families in this checkpoint
- mixed signed and unsigned candidates are not formed indirectly by widening
  through `float64`
- any candidate signature that would require a signed-to-unsigned or
  unsigned-to-signed implicit coercion is rejected before precedence ranking
- if no candidate remains after that rejection, execution fails with an
  explicit error instead of guessing
- exact unsigned matches are still selectable when a function family
  explicitly defines one; untyped `NULL` may adopt that selected unsigned type
  after signature selection, but `NULL` does not choose between signed and
  unsigned candidates by itself

For this checkpoint, representative mixed requests such as `int32` with
`uint32`, or `int64` with `uint64`, are execution errors unless a follow-on
spec issue defines explicit interaction semantics.

## Current Milestone-1 Boundary

The current executable slices fix the type behavior for the milestone-1 expression-projection path plus the first executable post-gate filter path.

### Covered Expression And Predicate Families

- `column(index)`
- `literal<int32>(value)`
- `add<int32>(lhs, rhs)`
- `is_not_null(column(index))`

The lattice and precedence policy above is a shared type-system checkpoint for later slices. The current executable milestone-1 arithmetic path still uses exact `int32` typing for `add<int32>`.

### Milestone-1 Out-Of-Scope Families

- nested logical families are out of scope for the current executable and differential milestone-1 checkpoints
- decimal and temporal logical families are out of scope for current milestone-1 expression evaluation and result typing
- current milestone-1 local execution paths should fail unsupported nested, decimal, or temporal requests explicitly rather than applying implicit casts into the `int32` arithmetic path
- first-slice differential artifacts should continue using the documented `int32` logical-type surface until a follow-on issue extends family coverage

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
- does not yet apply the broader `int32 < int64 < float64` widening policy to this executable slice

This fixes the current milestone-1 arithmetic typing rule without claiming it as the final shared contract for other operator families.

## First Post-Gate Filter Predicate Checkpoint

Issue #139 adds the first docs-first filter semantic checkpoint in
`docs/spec/first-filter-is-not-null.md`.

Issue #149 makes that predicate checkpoint executable in the local shared-kernel filter path. Issue #178 then extends executable predicate input to include `date32` for the first temporal slice. Issue #189 adds docs-first decimal predicate coverage for `decimal128`, issue #196 adds executable float64 predicate coverage, issue #288 adds executable `timestamp_tz(us)` predicate coverage, and issue #308 adds executable `uint64` predicate coverage.

### `is_not_null(column(index))`

- requires one `column(index)` operand that resolves to logical type `int32`, `uint64`, `date32`, `decimal128`, `float64`, `json`, or `timestamp_tz(us)` in current shared checkpoints
- derives logical result type `boolean`
- derives `nullable = false` for predicate evaluation
- evaluates row-wise as `true` for non-null input values and `false` for null input values
- reports an execution error when `index` is out of range
- reports an execution error rather than applying an implicit cast when the operand is outside the currently admitted checkpoint set
- local executable kernel coverage for this predicate now includes `int32`, `uint64`, `date32`, `decimal128`, `float64`, and `timestamp_tz(us)` in shared-kernel conformance slices through `crates/tiforth-kernel/tests/filter_is_not_null.rs`, `crates/tiforth-kernel/tests/unsigned_arithmetic_slice.rs`, `crates/tiforth-kernel/tests/temporal_date32_slice.rs`, `crates/tiforth-kernel/tests/decimal128_slice.rs`, `crates/tiforth-kernel/tests/float64_slice.rs`, and `crates/tiforth-kernel/tests/temporal_timestamp_tz_slice.rs`

## First Struct Nested Follow-On Checkpoint

Issue #226 adds a docs-first nested struct checkpoint in
`docs/design/first-struct-aware-handoff-slice.md`.

Issue #329 adds the first executable local shared-kernel coverage for this
checkpoint in:

- `crates/tiforth-kernel/tests/struct_slice.rs`

Issue #226 also adds the first docs-first struct coverage anchors in:

- `tests/conformance/first-struct-slice.md`
- `tests/differential/first-struct-slice.md`
- `adapters/first-struct-slice.md`

For current shared contracts:

- the first admitted struct nested logical type beyond milestone 1 is
  `struct<a:int32, b:int32?>`
- this struct checkpoint reuses existing expression family scope through
  passthrough `column(index)`
- this checkpoint does not define nested predicate behavior
  (`is_not_null(column(index))` over `struct`), nested casts, or nested
  ordering semantics
- this checkpoint does not define `map` or `union` behavior; those remain
  separate checkpoints
- nested compute families and broader nested logical types (nested
  combinations beyond current list, struct, map, and union checkpoints)
  remain out of scope until follow-on issues
- local executable kernel coverage for this struct checkpoint now exists in
  `crates/tiforth-kernel/tests/struct_slice.rs`

## First Map Nested Follow-On Checkpoint

Issue #230 adds a docs-first nested map checkpoint in
`docs/design/first-map-aware-handoff-slice.md`.

Issue #230 also adds the first docs-first map coverage anchors in:

- `tests/conformance/first-map-slice.md`
- `tests/differential/first-map-slice.md`
- `adapters/first-map-slice.md`

For current shared contracts:

- the first admitted map nested logical type beyond milestone 1 is
  `map<int32, int32?>`
- this map checkpoint reuses existing expression family scope through
  passthrough `column(index)`
- this checkpoint does not define nested predicate behavior
  (`is_not_null(column(index))` over `map`), nested casts, or nested ordering
  semantics
- this map checkpoint does not define `union` behavior; see
  `docs/design/first-union-aware-handoff-slice.md`
- broader nested logical types (nested combinations beyond current list,
  struct, map, and union checkpoints) remain out of scope until follow-on
  issues
- local executable kernel coverage for this map checkpoint now exists in
  `crates/tiforth-kernel/tests/map_slice.rs`

## First Union Nested Follow-On Checkpoint

Issue #241 adds a docs-first nested union checkpoint in
`docs/design/first-union-aware-handoff-slice.md`.

Issue #241 also adds the first docs-first union coverage anchors in:

- `tests/conformance/first-union-slice.md`
- `tests/differential/first-union-slice.md`
- `adapters/first-union-slice.md`

Issue #336 adds the first executable local conformance coverage for this
checkpoint in:

- `crates/tiforth-kernel/tests/union_slice.rs`

For current shared contracts:

- the first admitted union nested logical type beyond milestone 1 is
  `dense_union<i:int32, n:int32?>`
- this union checkpoint reuses existing expression family scope through
  passthrough `column(index)`
- this checkpoint preserves per-row variant tags and per-variant value
  nullability through canonical differential carriers with stable `tag` and
  `value` keys
- this checkpoint does not define nested predicate behavior
  (`is_not_null(column(index))` over `union`), nested casts, or nested
  ordering semantics
- broader nested logical types (nested combinations and wider union-mode
  expansion beyond current list, struct, map, and dense-union checkpoints)
  remain out of scope until follow-on issues
- local executable kernel coverage for this union checkpoint now exists in
  `crates/tiforth-kernel/tests/union_slice.rs`

## First Temporal Follow-On Checkpoint

Issue #174 adds a docs-first temporal checkpoint in
`docs/design/first-temporal-semantic-slice.md`.

Issue #176 adds the first docs-first temporal coverage anchors in:

- `tests/conformance/first-temporal-date32-slice.md`
- `tests/differential/first-temporal-date32-slice.md`
- `adapters/first-temporal-date32-slice.md`

Issue #178 adds the first executable local conformance coverage for this checkpoint in:

- `crates/tiforth-kernel/tests/temporal_date32_slice.rs`

For current shared contracts:

- the first admitted temporal logical type beyond milestone 1 is `date32`
- the first temporal checkpoint reuses existing expression and predicate
  families: passthrough `column(index)` plus `is_not_null(column(index))`
- this checkpoint does not define temporal arithmetic, casts, extraction, or
  truncation semantics
- broader temporal families remain out of scope until follow-on issues define
  their semantics and coverage

## First Timestamp-Timezone Temporal Checkpoint

Issue #280 adds a docs-first timezone-aware timestamp checkpoint in
`docs/design/first-temporal-timestamp-tz-slice.md`.

Issue #280 also adds the first docs-first timestamp-timezone coverage anchors in:

- `tests/conformance/first-temporal-timestamp-tz-slice.md`
- `tests/differential/first-temporal-timestamp-tz-slice.md`
- `adapters/first-temporal-timestamp-tz-slice.md`

Issue #288 adds the first executable local conformance coverage for this checkpoint in:

- `crates/tiforth-kernel/tests/temporal_timestamp_tz_slice.rs`

Issue #304 adds the first executable TiDB-versus-TiFlash differential harness coverage for this checkpoint in:

- `crates/tiforth-adapter-tidb`
- `crates/tiforth-adapter-tiflash`
- `crates/tiforth-harness-differential/src/first_temporal_timestamp_tz_slice.rs`

with checked-in paired artifacts under `inventory/first-temporal-timestamp-tz-slice-*`.

Issue #306 adds the first executable TiKV single-engine and pairwise differential harness coverage for this checkpoint in:

- `crates/tiforth-adapter-tikv`
- `crates/tiforth-harness-differential/src/first_temporal_timestamp_tz_slice_tikv.rs`
- `crates/tiforth-harness-differential/src/first_temporal_timestamp_tz_slice_tikv_pairwise.rs`

with checked-in TiKV single-engine and pairwise artifacts under `inventory/first-temporal-timestamp-tz-slice-*`.

For current shared contracts:

- the first admitted timezone-aware timestamp logical type beyond `date32` is
  `timestamp_tz(us)`
- this checkpoint reuses existing expression and predicate families:
  passthrough `column(index)` plus `is_not_null(column(index))`
- this checkpoint adds one docs-first ordering probe boundary over
  `timestamp_tz(us)` under stable `ordering_ref`
- shared differential comparison for this checkpoint normalizes non-null
  timestamp values to UTC epoch-microsecond integers
- `timestamp` without timezone and non-`us` timestamp units are execution
  errors in this checkpoint
- temporal arithmetic, casts, extraction, truncation, and broader
  timestamp-timezone policy remain out of scope until follow-on issues define
  semantics and coverage
- local executable kernel coverage for this checkpoint now exists in
  `crates/tiforth-kernel/tests/temporal_timestamp_tz_slice.rs`
- adapter and differential-harness executable coverage for this checkpoint now exists in `crates/tiforth-adapter-tidb`, `crates/tiforth-adapter-tiflash`, `crates/tiforth-adapter-tikv`, `crates/tiforth-harness-differential/src/first_temporal_timestamp_tz_slice.rs`, `crates/tiforth-harness-differential/src/first_temporal_timestamp_tz_slice_tikv.rs`, and `crates/tiforth-harness-differential/src/first_temporal_timestamp_tz_slice_tikv_pairwise.rs`, with checked-in paired and single-engine artifacts under `inventory/first-temporal-timestamp-tz-slice-*`

## First Decimal Follow-On Checkpoint

Issue #189 adds a docs-first decimal checkpoint in
`docs/design/first-decimal-semantic-slice.md`.

Issue #189 also adds the first docs-first decimal coverage anchors in:

- `tests/conformance/first-decimal128-slice.md`
- `tests/differential/first-decimal128-slice.md`
- `adapters/first-decimal128-slice.md`

For current shared contracts:

- the first admitted decimal logical type beyond milestone 1 is
  `decimal128(precision, scale)`
- the first decimal checkpoint reuses existing expression and predicate
  families: passthrough `column(index)` plus `is_not_null(column(index))`
- this checkpoint preserves decimal precision and scale metadata and compares
  row outcomes through canonical decimal strings
- this checkpoint does not define decimal arithmetic, cast, coercion, or
  rounding semantics
- broader decimal families remain out of scope until follow-on issues define
  their semantics and coverage

Local executable decimal kernel conformance coverage now exists in
`crates/tiforth-kernel/tests/decimal128_slice.rs`.


## First Float64 NaN/Infinity Ordering Checkpoint

Issue #194 adds a docs-first float64 checkpoint in
`docs/design/first-float64-ordering-slice.md`.

Issue #194 also adds the first docs-first float64 coverage anchors in:

- `tests/conformance/first-float64-ordering-slice.md`
- `tests/differential/first-float64-ordering-slice.md`
- `adapters/first-float64-ordering-slice.md`

Issue #196 adds the first executable local conformance coverage for this checkpoint in:

- `crates/tiforth-kernel/tests/float64_slice.rs`

For current shared contracts:

- the first admitted floating checkpoint for NaN, infinity, and ordering
  semantics is `float64`
- this checkpoint reuses existing expression and predicate families:
  passthrough `column(index)` plus `is_not_null(column(index))`
- `NaN`, `Infinity`, `-Infinity`, `0.0`, and `-0.0` are non-null `float64`
  values for this checkpoint
- comparison intent treats `NaN` as unequal to any value (including `NaN`) and
  treats `0.0` and `-0.0` as compare-equal
- ordered-comparison intent treats comparisons involving `NaN` as false and
  orders non-NaN values as `-Infinity < finite < Infinity`
- canonical differential ordering for this checkpoint is
  `-Infinity < finite < Infinity < NaN`, with `-0.0` ordered before `0.0` only
  as a stable canonicalization tie-break
- broader floating behavior (arithmetic, coercion, casts, and SQL ordering
  policy) remains follow-on scope

## First JSON Comparability/Cast Checkpoint

Issue #224 adds a docs-first JSON checkpoint in
`docs/design/first-json-semantic-slice.md`.

Issue #224 also adds the first docs-first JSON coverage anchors in:

- `tests/conformance/first-json-slice.md`
- `tests/differential/first-json-slice.md`
- `adapters/first-json-slice.md`

For current shared contracts:

- the first admitted JSON logical type beyond milestone 1 is `json`
- this checkpoint admits passthrough `column(index)`, docs-first structural
  equality intent `json_eq(lhs, rhs)`, and explicit cast boundaries
  `cast<string>(json_expr)` and `cast<json>(string_expr)`
- this checkpoint defines object-key-order-insensitive equality intent,
  array-order-sensitive equality intent, and canonical JSON-string
  normalization for differential comparison
- this checkpoint does not define JSON path extraction, containment,
  mutation, ordering, or implicit cast expansion
- local executable kernel and adapter coverage for this JSON checkpoint remains
  follow-on scope

## First Collation-Sensitive String Checkpoint

Issue #233 adds a docs-first collation checkpoint in
`docs/design/first-collation-string-slice.md`.

Issue #233 also adds the first docs-first collation coverage anchors in:

- `tests/conformance/first-collation-string-slice.md`
- `tests/differential/first-collation-string-slice.md`
- `adapters/first-collation-string-slice.md`

For current shared contracts:

- the first admitted collation-sensitive string logical type beyond milestone 1
  is `utf8`
- the first shared collation identifiers are `binary` and `unicode_ci`
- this checkpoint admits passthrough `column(index)`,
  `is_not_null(column(index))`, and docs-first collation-tagged comparison and
  ordering probes under explicit `collation_ref`
- this checkpoint does not add implicit collation derivation, collation
  coercion, or shared runtime collation negotiation state
- local executable kernel and adapter coverage for this collation checkpoint
  remains follow-on scope

## First Overflow Follow-On Checkpoint

Issue #276 adds a docs-first overflow checkpoint for operator families beyond
the current executable `add<int32>` path.

For current shared contracts:

- `add<int32>` keeps its existing executable overflow-as-error behavior from
  milestone 1
- for follow-on numeric operator families, overflow or underflow is an
  execution error unless that family spec explicitly defines a different rule
- shared defaults do not silently wrap, saturate, widen, or coerce overflowed
  results
- any future family that needs non-error overflow behavior must document that
  behavior in its own spec checkpoint before executable coverage lands
- overflow remains an operator compute failure and does not change runtime
  ownership or admission error taxonomy

## First Unsigned Arithmetic Follow-On Checkpoint

Issue #300 adds a docs-first unsigned arithmetic checkpoint in
`docs/design/first-unsigned-arithmetic-slice.md`.

Issue #300 also adds the first docs-first unsigned arithmetic coverage anchors
in:

- `tests/conformance/first-unsigned-arithmetic-slice.md`
- `tests/differential/first-unsigned-arithmetic-slice.md`
- `adapters/first-unsigned-arithmetic-slice.md`

Issue #308 adds the first executable local conformance coverage for this
checkpoint in:

- `crates/tiforth-kernel/tests/unsigned_arithmetic_slice.rs`

For current shared contracts:

- the first admitted unsigned arithmetic logical type beyond milestone 1 is
  `uint64`
- this checkpoint admits passthrough `column(index)`,
  `literal<uint64>(value)`, `add<uint64>(lhs, rhs)`, and
  `is_not_null(column(index))` over `uint64`
- `add<uint64>` requires both operands to resolve to `uint64`; mixed signed and
  unsigned arithmetic requests remain execution errors
- `add<uint64>` overflow is an execution error in this checkpoint; shared
  defaults do not wrap, saturate, widen, or coerce overflowed results
- this checkpoint does not add unsigned cast/coercion edges, wider unsigned
  families, or unsigned arithmetic beyond `add<uint64>`
- local executable kernel coverage for this unsigned checkpoint now exists in
  `crates/tiforth-kernel/tests/unsigned_arithmetic_slice.rs`
- adapter and differential harness coverage for this unsigned checkpoint
  remains follow-on scope

## Open Questions

- TODO: extend the initial coercion lattice beyond `int32`, `int64`, and `float64`, including cross-family precedence boundaries
- TODO: define family-specific overflow semantics for decimal and temporal arithmetic checkpoints when those families become executable
- TODO: extend collation semantics beyond the first string checkpoint,
  including locale-specific collation families, binary-type collation
  interactions, and executable adapter plus kernel coverage
- TODO: extend temporal semantics beyond the first `date32` and `timestamp_tz(us)` checkpoints, including arithmetic, cast, extract, truncation, broader unit coverage, and timezone-name canonicalization rules
- TODO: extend decimal semantics beyond the first `decimal128` checkpoint, including arithmetic, cast/coercion, precision/scale propagation, and rounding behavior
- TODO: extend JSON semantics beyond the first checkpoint, including path extraction, containment, ordering, implicit casts, and executable adapter/kernel coverage
## Boundary For Now

This file should guide future specs and harness cases. It should not yet force low-level representation choices such as dictionary layout that belong in the data contract.
