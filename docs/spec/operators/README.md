# Operator Specs

This directory will hold operator specifications.

For now it is intentionally empty of concrete implementations and detailed per-operator files. The reboot should first establish:

- operator categories
- operand and result typing rules
- precedence and associativity where relevant
- null, overflow, and error semantics
- engine compatibility notes
- harness coverage expectations

Current first-wave priority:

- donor and engine inventory should start with the single-input projection family documented in `docs/design/first-inventory-wave.md`
- keep that operator inventory scoped to output ordering, output naming, direct column passthrough, and computed-column materialization before broader operator families are considered

## Planned Follow-On Checkpoints

- the first intentionally broad multi-input operator program should stay
  docs-first and is currently anchored in
  `docs/design/first-hash-join-build-probe-boundary.md`
- that program should define operator semantics, build/probe runtime
  attachment, ownership, admission, collation, adapter, and harness boundaries
  before a shared-kernel implementation issue starts
- later per-operator semantic specs should live under `docs/spec/operators/`
  rather than letting one implementation PR become the source of truth

Do not place implementation code here.
