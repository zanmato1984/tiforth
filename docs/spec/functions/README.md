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

Do not place implementation code here.
