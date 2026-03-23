# Function Specs

This directory will hold function specifications.

For now it is intentionally empty of concrete implementations and detailed per-function files. The reboot should first establish:

- naming and grouping
- signatures
- result type rules
- null and error behavior
- engine compatibility notes
- harness coverage expectations

Current first-wave priority:

- donor and engine inventory should start with `literal<int32>(value)` and `add<int32>(lhs, rhs)` as documented in `docs/design/first-inventory-wave.md`
- keep that work anchored to the stable `first-expression-slice` refs instead of engine-native SQL spellings

## Current Function-Family Direction

- the first complete function family is now fixed in
  `docs/spec/functions/numeric-add-family.md`
- the canonical shared completion entry for that same family now lives in
  `docs/spec/functions/numeric-add-family-completion.md`
- the admitted decimal add result-derivation boundary for that same family now
  lives in
  `docs/spec/functions/numeric-add-family-decimal-result-derivation.md`
- the first signed-widening `add<int64>` slice boundary for that same family
  now lives in `docs/design/first-signed-widening-add-int64-slice.md`
- shared docs use `add` as the family name, while `plus` and SQL `+` remain
  accepted external spellings for the same numeric family when donor, engine,
  or protocol sources need to be discussed
- current repo evidence that justifies this choice is the existing
  `add<int32>(lhs, rhs)` checkpoint from `first-expression-slice` plus the
  `add<uint64>(lhs, rhs)` checkpoint from
  `docs/design/first-unsigned-arithmetic-slice.md`
- family-specific TiDB-to-Arrow mapping, `tipb`/`kvproto` enum reuse, and the
  generic-first completion checklist are now fixed in shared docs, and decimal
  add now has an accepted `decimal128` result-derivation boundary; executable
  follow-ons still need to satisfy the admitted overload and evidence
  expectations named there before the family can be claimed complete

## Planned Follow-On Checkpoints

- the TiDB-to-Arrow mapping required by the numeric add/plus family is now
  fixed in `docs/design/first-tidb-arrow-type-mapping-boundary.md`
- the `tipb`/`kvproto` enum reuse boundary for the same family is now fixed in
  `docs/design/first-add-family-tipb-kvproto-enum-reuse.md`
- the generic-first family-completion boundary is now fixed in
  `docs/spec/functions/numeric-add-family-completion.md`
- the exact decimal add result-derivation boundary for the same family is now
  fixed in
  `docs/spec/functions/numeric-add-family-decimal-result-derivation.md`
- the first signed-widening `add<int64>` slice boundary for the same family is
  now fixed in `docs/design/first-signed-widening-add-int64-slice.md`
- the next same-epic follow-ons should land the remaining admitted overload
  checkpoints for `add<float64>` and admitted exact decimal add plus their
  executable harness coverage and checked-in evidence without reopening
  unrelated families

Do not place implementation code here.
