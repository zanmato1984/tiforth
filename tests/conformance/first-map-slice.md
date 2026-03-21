# First Map Slice Cases

Status: issue #230 docs checkpoint, issue #334 executable kernel checkpoint, issue #362 executable differential checkpoint

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

Local executable kernel conformance for this checkpoint now exists in
`crates/tiforth-kernel/tests/map_slice.rs`.

Issue #362 adds executable TiDB-versus-TiFlash adapter and differential harness coverage for this checkpoint in `crates/tiforth-adapter-tidb/src/first_map_slice.rs`, `crates/tiforth-adapter-tiflash/src/first_map_slice.rs`, and `crates/tiforth-harness-differential/src/first_map_slice.rs`, with checked-in artifacts under `inventory/first-map-slice-*`
