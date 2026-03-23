# First TiDB-To-Arrow Type Mapping Boundary

Status: issue #418 design checkpoint

Verified: 2026-03-22

Related issues:

- #409 `epic: complete function-family program`
- #413 `sub-epic: TiDB-to-Arrow mapping for the active function family`
- #414 `spec: pin the first complete function family as numeric add/plus`
- #418 `design: fix TiDB-to-Arrow mapping for the numeric add/plus family`
- #423 `spec: fix decimal add result derivation for the numeric add/plus family`

## Question

What first durable boundary should `tiforth` set for mapping TiDB logical types
onto Arrow-native or Arrow-extension representations while preserving shared
semantic type identity and avoiding engine-local encodings in the shared data
contract?

## Inputs Considered

- `docs/spec/type-system.md`
- `docs/contracts/data.md`
- `docs/design/milestone-1-nested-decimal-temporal-boundary.md`
- `docs/design/first-temporal-semantic-slice.md`
- `docs/design/first-temporal-timestamp-tz-slice.md`
- `docs/design/first-decimal-semantic-slice.md`
- `docs/spec/functions/numeric-add-family-decimal-result-derivation.md`
- `docs/design/first-json-semantic-slice.md`
- `docs/design/first-collation-string-slice.md`

## Design Summary

The TiDB-to-Arrow mapping task should live as one cross-cutting design
checkpoint instead of being scattered across individual slice docs.

That checkpoint should classify every in-scope TiDB logical family into one of
three buckets:

- exact Arrow-native mapping
- Arrow-native mapping with explicit metadata or normalization policy
- `tiforth`-defined Arrow extension type or other explicitly documented custom
  carrier

The default bias should stay Arrow-native. Extension types should appear only
when native Arrow types cannot preserve TiDB semantics without hidden side
channels, unstable adapter-local rules, or lossy normalization.

## Mapping Questions This Checkpoint Must Resolve

The initial matrix should make these questions explicit:

- which TiDB logical types map 1:1 to existing Arrow logical families with no
  extra semantics
- which types need explicit timezone, collation, precision, scale, or other
  metadata to remain semantically stable
- which types cannot be represented faithfully enough without an Arrow
  extension type or another named custom carrier
- which current TiDB representations are semantically correct but physically
  inefficient, and whether Arrow-native representations improve that tradeoff

## Required Family Buckets

Follow-on work should at least classify these families:

- exact-or-likely-native review bucket:
  signed integers, unsigned integers, booleans, `float32`, `float64`,
  `utf8`, and `binary`
- native-with-explicit-policy bucket:
  `date`, `datetime`, `timestamp`, `time`, duration-like families,
  collation-tagged strings, and decimal precision or scale handling
- extension-type-or-custom-carrier review bucket:
  `json`, any TiDB family whose semantics depend on more than Arrow-native
  type identity plus schema metadata, and any type whose null or ordering
  rules would otherwise be hidden in adapter-local code

This planning checkpoint does not claim that every family above is already
settled. It fixes the review buckets so follow-on issues do not guess where
the mapping work belongs.

## Decimal Direction

Decimal needs explicit review rather than inheritance from legacy layouts.

The mapping checkpoint should answer all of these:

- when TiDB decimal values fit Arrow `decimal128`
- when wider precision forces `decimal256` or an explicitly deferred path
- whether Arrow decimal arrays are semantically compatible enough to replace
  slower legacy decimal carriers
- which operations still need TiDB-specific rounding, overflow, rescale, or
  string-format policy even if storage becomes Arrow-native

The default direction should be to prefer Arrow-native decimal storage when it
preserves TiDB semantics and materially improves the physical contract.

## Temporal And JSON Direction

Temporal and JSON families need separate treatment.

- temporal mapping should distinguish `date`, timezone-free `timestamp`,
  timezone-aware `timestamp`, `datetime`, and `time` semantics instead of
  collapsing them into one Arrow temporal bucket
- shared docs should state which temporal meanings are represented directly by
  Arrow temporal types and which require extension metadata or deferred support
- JSON should remain an explicit extension-type or custom-carrier candidate
  until shared docs prove that Arrow-native representation can preserve SQL
  `NULL`, JSON literal `null`, canonical comparison, and transfer semantics
  without hidden adapter-local rules

## First Active Function-Family Mapping Checkpoint

Issue #418 now fixes the first concrete use of this boundary for the scalar
numeric `add/plus` family.

For the current active family:

- exact signed integer add overloads map to Arrow-native signed integer types of
  matching width; engine-local arithmetic widening to `bigint` does not change
  the shared result-type identity
- exact unsigned integer add overloads map to Arrow-native unsigned integer
  types of matching width; current executable evidence already covers `uint64`
- exact floating-point add overloads map to Arrow-native `float32` or `float64`
  of matching width; current active-family completion should prioritize
  `float64` before reopening `float32`
- exact decimal add overloads whose derived result precision fits Arrow
  `decimal128` map to Arrow-native `decimal128(result.precision, result.scale)`
  under the accepted result-derivation rule in
  `docs/spec/functions/numeric-add-family-decimal-result-derivation.md`
- exact decimal add overloads whose derived result precision would require
  `decimal256` remain directionally Arrow-native, but stay deferred until a
  shared `decimal256` checkpoint exists
- values beyond Arrow decimal range stay deferred for this family; `tiforth`
  does not introduce a numeric-add-specific extension type or custom carrier now

This resolves the first active-family mapping stance:

- exact Arrow-native mapping bucket: signed integers, unsigned integers,
  `float64`, and later `float32` when that overload is admitted
- Arrow-native-with-explicit-policy bucket: decimal add through Arrow decimal
  types with accepted precision/scale result derivation, while `decimal256`
  and broader decimal rescale policy remain follow-on scope
- extension-type-or-custom-carrier bucket: none for the current numeric
  add/plus family

Adapters may still need explicit narrowing, casts, or metadata normalization so
engine-local arithmetic plans line up with the shared exact overload identity.
TiDB or TiKV surfacing `BIGINT` metadata for integer arithmetic does not by
itself justify widening the shared `add<int32>` result to Arrow `int64`.

## Required Follow-On Docs Before Implementation

Any implementation-facing type-mapping issue should update or create, or
explicitly mark unchanged, all of these surfaces first:

1. this mapping boundary doc for the matrix and rationale
2. `docs/spec/type-system.md` for logical identity, cast, comparison, and
   function-signature consequences
3. `docs/contracts/data.md` for Arrow-native, metadata-carrying, or
   extension-type handoff rules
4. slice-specific design docs for temporal, decimal, JSON, collation, or other
   families when the mapping changes admitted execution scope
5. adapter and harness docs wherever normalized `rows[]`, schema carriers, or
   artifact expectations change

## Program Decomposition

Recommended decomposition:

1. classify direct native mappings and document exact criteria
2. classify temporal families and timezone metadata rules
3. classify decimal families and Arrow-native performance direction
4. classify JSON and any remaining extension-type candidates
5. update adapter and harness normalization rules for the admitted families

## Risks

- confusing semantic type identity with one convenient Arrow physical encoding
- forcing extension types too early and blocking zero-copy or ecosystem reuse
- forcing Arrow-native storage where TiDB semantics need extra metadata or
  different null, ordering, or cast behavior
- treating decimal migration as a storage-only choice when arithmetic and
  rescale behavior may still need separate spec work
- collapsing SQL `NULL` and JSON literal `null` during normalization

## Result

`tiforth` now has one explicit planning home for TiDB-to-Arrow type mapping:
the shared matrix belongs under `docs/design/`, with `docs/spec/type-system.md`
owning semantic identity and `docs/contracts/data.md` owning the handoff
contract.

For the current active function-family program, that home now also carries one
concrete mapping decision: numeric `add/plus` stays Arrow-native by default,
uses explicit decimal metadata policy instead of a custom numeric carrier, and
does not let engine-local `BIGINT` widening silently change the shared exact
overload type.
