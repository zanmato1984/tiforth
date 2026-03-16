# tiforth

`tiforth` is a reboot, not a lift-and-shift.

The legacy repository at `https://github.com/zanmato1984/tiforth-legacy` is donor material for concepts, extracted catalogs, and compatibility reference. This repository starts harness-first so the team can define contracts, measure drift, and prove semantics before introducing kernel code.

This initial commit is structure and documentation only. It intentionally does not include concrete operator or function implementations.

## Current Shape

- `docs/`: source-of-truth design notes, contracts, and spec scaffolding
- `plans/`: active and completed execution plans
- `inventory/`: future extracted catalogs, donor notes, and drift reports
- `tests/`: harness skeletons for conformance, differential, and performance work
- `adapters/`: engine-facing adapter boundaries for TiDB, TiFlash, and TiKV

## Working Rules

- Preserve donor ideas, not donor code, unless a later plan explicitly says otherwise.
- Prefer explicit TODOs over guessed semantics.
- Keep concrete implementations out until the harness and contracts are ready.
