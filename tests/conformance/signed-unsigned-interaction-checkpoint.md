# Signed/Unsigned Interaction Checkpoint Cases

Spec source: `docs/spec/type-system.md`

## Canonical Cases

- `exact signed match`: when a function family exposes a signed signature that
  exactly matches all non-null operands, that signature remains selectable
- `exact unsigned match`: when a function family exposes an unsigned signature
  that exactly matches all non-null operands, that signature remains selectable
- `mixed int32/uint32`: a request that needs implicit signed/unsigned coercion
  fails as an execution error
- `mixed int64/uint64`: a request that needs implicit signed/unsigned coercion
  fails as an execution error
- `no float bridge`: mixed signed/unsigned requests do not become valid by
  coercing both operands through `float64`
- `null adoption only after selection`: untyped `NULL` may adopt the type from
  an already-selected signature but does not decide signed-versus-unsigned
  selection by itself

## Initial Harness Boundary

Issue #141 defines this as a docs-first type-system checkpoint.

This file fixes canonical signed/unsigned signature-selection expectations for
future harness slices without expanding the current milestone-1 executable
`add<int32>` kernel path.

## Follow-On Coverage

When a later executable slice introduces unsigned function signatures, local
and differential conformance coverage should include at least one assertion for
each canonical case above.
