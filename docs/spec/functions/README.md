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

## Planned Follow-On Checkpoints

- one follow-on should define the first complete function family under
  `docs/spec/functions/` with generic-first signature families, explicit
  overload selection rules, and clear reuse expectations across same-name
  signatures instead of one implementation path per overload
- that follow-on should update `docs/spec/type-system.md` for signature
  matching, coercion, and result-type derivation before implementation starts
- function identity should prefer shared `tipb` and `kvproto` enum reuse when
  their existing function identifiers match the intended `tiforth` semantic
  family, rather than inventing a parallel ID space by default
- exact upstream enum locations, mismatch cases, and any required
  `tiforth`-only IDs remain issue-scoped inventory and design work until a
  follow-on doc records the mapping explicitly

Do not place implementation code here.
