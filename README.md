# tiforth

`tiforth` is a reboot, not a lift-and-shift.

The legacy repository at `https://github.com/zanmato1984/tiforth-legacy` is donor material for concepts, extracted catalogs, and compatibility reference. This repository stays harness-first and docs-first. Issue #10 introduces only the first minimal Rust kernel slice: Arrow-bound expression evaluation plus projection on top of `broken-pipeline-rs`.

## Current Shape

- `docs/`: source-of-truth design notes, contracts, and spec scaffolding
- `plans/`: active and completed execution plans
- `inventory/`: future extracted catalogs, donor notes, and drift reports
- `tests/`: harness skeletons, case plans, and fixtures
- `adapters/`: engine-facing adapter boundaries for TiDB, TiFlash, and TiKV
- `crates/`: narrow Rust implementation slices that are justified by accepted docs and local tests

## Working Rules

- Preserve donor ideas, not donor code, unless a later plan explicitly says otherwise.
- Prefer explicit TODOs over guessed semantics.
- Keep implementation slices narrow until specs and harness coverage say they should grow.
- Each issue or parallel workstream must use its own local git worktree; do not reuse one directory for multiple active issue tasks.
- Each PR must link its issue explicitly; use `Closes #...` for auto-close on merge and `Refs #...` for partial or stacked work.
