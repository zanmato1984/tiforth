# First Union Slice Cases

Status: issue #241 docs checkpoint

Spec source: `docs/design/first-union-aware-handoff-slice.md`

## Canonical Cases

- `union column passthrough`: `column(index)` over
  `dense_union<i:int32, n:int32?>` preserves row count, variant-tag order, and
  per-variant value payloads
- `union variant-switch preservation`: alternating variant tags remain stable
  row-wise and preserve the corresponding child payload values
- `union variant-null preservation`: nullable `n:int32?` variant payloads
  remain null only for rows that select variant `n`
- `missing column`: out-of-range `column(index)` in projection paths fails as
  an execution error
- `unsupported nested family`: nested inputs outside this checkpoint (for
  example `map<int32, int32?>` or wider and sparse union modes) fail as
  execution errors in this slice

## Differential Anchor

Cross-engine comparison for these cases is defined in:

- `tests/differential/first-union-slice.md`
- `adapters/first-union-slice.md`

## Executable Harness Boundary

This checkpoint is docs-first only. Local executable kernel conformance for
`union` remains follow-on scope.
