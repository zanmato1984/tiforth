# Local Execution Fixture Files

Status: issue #27 local artifact checkpoint

Related issues:

- #23 `design: freeze milestone-1 local event snapshot shape`
- #25 `conformance: define and export milestone-1 local execution snapshot fixtures`
- #27 `conformance: add file-backed local execution fixtures for milestone-1`

## Purpose

`tiforth_kernel::LocalExecutionFixture` freezes the current local Rust-side event carrier for milestone-1 conformance coverage. This note defines how the current projection-path tests store that carrier as checked-in fixture files.

These files are local harness artifacts only. They make projection-path runtime and admission expectations reviewable without freezing an adapter-visible callback or FFI format.

## File Layout

Current fixture files live under `tests/conformance/fixtures/local-execution/`.

Each file captures one documented scenario checkpoint from `crates/tiforth-kernel/tests/expression_projection.rs`.

Use lower-case kebab-case file names that combine the scenario and checkpoint, for example:

- `projection-computed-before-terminal.json`
- `projection-computed-finished.json`

## JSON Shape

Each fixture file is one JSON object with these top-level arrays:

- `admission_events`
- `runtime_events`

Each event record contains `event` plus only the primitive payload keys that apply to that event. Keys that do not apply to a given event are omitted rather than serialized as `null`.

For `error` events, `message` stores the current local Rust execution error text verbatim. The checked-in milestone-1 fixtures therefore preserve wrapper prefixes that come from the current `ArrowError` surface instead of normalizing them away.

`origin` remains a nested object with:

- `query`
- `stage`
- `operator`

Ordering guarantees stay per event family only: `admission_events[]` stay ordered within admission observations, and `runtime_events[]` stay ordered within runtime observations. These files do **not** create a merged cross-family total order or add timestamps.

## Current Scope

The initial checked-in files cover the current milestone-1 projection slice only:

- computed projection before terminal completion
- computed projection after final release and terminal completion
- deny-before-emit failure
- passthrough claim forwarding before terminal completion
- passthrough claim forwarding after final release and terminal completion

Broader adapter-visible fixtures, full `claims[]` serialization, and non-projection harness carriers remain out of scope.
