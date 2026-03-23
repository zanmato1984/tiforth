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
- shared docs use `add` as the family name, while `plus` and SQL `+` remain
  accepted external spellings for the same numeric family when donor, engine,
  or protocol sources need to be discussed
- current repo evidence that justifies this choice is the existing
  `add<int32>(lhs, rhs)` checkpoint from `first-expression-slice` plus the
  `add<uint64>(lhs, rhs)` checkpoint from
  `docs/design/first-unsigned-arithmetic-slice.md`
- family-specific TiDB-to-Arrow mapping and `tipb`/`kvproto` enum reuse remain
  required same-epic follow-ons before the family can be claimed complete

## Planned Follow-On Checkpoints

- the TiDB-to-Arrow mapping required by the numeric add/plus family is now
  fixed in `docs/design/first-tidb-arrow-type-mapping-boundary.md`
- the `tipb`/`kvproto` enum reuse boundary for the same family is now fixed in
  `docs/design/first-add-family-tipb-kvproto-enum-reuse.md`
- the next follow-on should complete the family through generic-first overload
  reuse, with any remaining type-system updates landing there before the family
  is claimed complete

Do not place implementation code here.
