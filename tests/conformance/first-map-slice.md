# First Map Slice Cases

Status: issue #230 docs checkpoint

Spec source: `docs/design/first-map-aware-handoff-slice.md`

## Canonical Cases

- `map column passthrough`: `column(index)` over `map<int32, int32?>`
  preserves row count, per-row entry counts, key values, value nullability, and
  map-row null placement
- `map nullable passthrough`: nullable `map` input preserves SQL `NULL` row
  placement while preserving key and value payloads for non-null rows
- `map value-null preservation`: nullable map values remain null only in the
  affected value entries and do not mark the full map row null
- `missing column`: out-of-range `column(index)` in projection paths fails as
  an execution error
- `unsupported nested family`: nested inputs outside this checkpoint (for
  example `union`) fail as execution errors in this slice

## Differential Anchor

Cross-engine comparison for these cases is defined in:

- `tests/differential/first-map-slice.md`
- `adapters/first-map-slice.md`

## Executable Harness Boundary

This checkpoint is docs-first only. Local executable kernel conformance for
`map` remains follow-on scope.
