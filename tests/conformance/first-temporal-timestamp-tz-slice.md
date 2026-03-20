# First Temporal `timestamp_tz(us)` Slice Cases

Status: issue #280 docs checkpoint

Spec source: `docs/design/first-temporal-timestamp-tz-slice.md`

## Canonical Cases

- `timestamp tz column passthrough`: `column(index)` over `timestamp_tz(us)`
  preserves row count, normalized UTC epoch-microsecond values, null
  positions, and field nullability
- `timestamp tz equivalent instant normalization`: equivalent instants encoded
  with different timezone offsets normalize to the same UTC
  epoch-microsecond value
- `timestamp tz nullable passthrough`: nullable `timestamp_tz(us)` input
  remains nullable with unchanged null placement
- `timestamp tz predicate all kept`: `is_not_null(column(0))` over non-null
  `timestamp_tz(us)` keeps every row
- `timestamp tz predicate all dropped`: `is_not_null(column(0))` over all-null
  `timestamp_tz(us)` drops every row
- `timestamp tz predicate mixed keep/drop`: mixed-null `timestamp_tz(us)` input
  keeps only non-null rows while preserving retained-row order and full-row
  passthrough
- `timestamp tz ordering asc nulls last`: ordering probes over
  `timestamp_tz(us)` sort by normalized UTC instant ascending and place null
  rows last
- `missing column`: out-of-range `column(index)` in projection, predicate, or
  ordering paths fails as an execution error
- `unsupported temporal type`: `timestamp` without timezone fails as an
  execution error in this slice
- `unsupported temporal unit`: timezone-aware timestamps with units other than
  `us` fail as an execution error in this slice

## Differential Anchor

Cross-engine comparison for these cases is defined in:

- `tests/differential/first-temporal-timestamp-tz-slice.md`
- `adapters/first-temporal-timestamp-tz-slice.md`

## Executable Harness Boundary

This checkpoint is docs-first only. Local executable kernel conformance for
`timestamp_tz(us)` remains follow-on scope.
