# tiforth

`tiforth` is a reboot, not a lift-and-shift.

The legacy repository at `https://github.com/zanmato1984/tiforth-legacy` is donor material for concepts, extracted catalogs, and compatibility reference. This repository stays harness-first and docs-first while growing through narrow executable slices and docs-first follow-on checkpoints.

## Current Shape

- `docs/`: source-of-truth design notes, contracts, semantic specs, and accepted decisions
- `inventory/`: checked-in normalized case-results, compatibility notes, donor notes, and drift reports
- `tests/`: harness skeletons, cases, and fixtures
- `adapters/`: engine-facing adapter boundaries for TiDB, TiFlash, and TiKV
- `crates/`: narrow Rust implementation slices that are justified by accepted docs and local tests
- `scripts/`: local workflow helpers for repeatable repository maintenance tasks

## Current Checkpoints

- executable differential coverage exists for `first-expression-slice`, `first-filter-is-not-null-slice`, `first-temporal-date32-slice`, `first-temporal-timestamp-tz-slice`, `first-decimal128-slice`, `first-float64-ordering-slice`, and `first-unsigned-arithmetic-slice`
- checked-in `inventory/` evidence now includes normalized per-engine `case-results`, compatibility notes, pairwise drift reports, and the first baseline-versus-exchange parity drift reports for the executable slices that have landed
- TiKV executable coverage currently extends through the first expression, filter, temporal, decimal, float64-ordering, and unsigned-arithmetic checkpoints, with pairwise drift artifacts recorded against TiDB and TiFlash where those slices are implemented
- local shared-kernel executable coverage now includes the first `struct<a:int32, b:int32?>` and `map<int32, int32?>` passthrough checkpoints, while JSON, collation-sensitive string, and nested union slices remain docs-first follow-on boundaries

## Local Rust Setup

Kernel and harness crate development expects a local Rust toolchain with `rustfmt`.

For a clean machine setup, run:

```sh
scripts/setup-rust-toolchain.sh
```

Then run formatting and tests locally:

```sh
cargo fmt --all -- --check
cargo test --workspace
```

`rust-toolchain.toml` keeps the repository on the stable channel with the required formatting component.

## Working Rules

- Preserve donor ideas, not donor code, unless a tracked issue or accepted decision explicitly says otherwise.
- Prefer explicit TODOs and follow-up issues over guessed semantics.
- Keep implementation slices narrow until specs and harness coverage say they should grow.
- Track execution in GitHub issues and PRs rather than in repo-local plan or status docs.
- Each issue or parallel workstream must use its own local git worktree; do not reuse one directory for multiple active issue tasks.
- For the ordinary clean setup case, prefer `scripts/start-issue-worktree.sh <issue-number>` to create the issue branch and dedicated worktree consistently.
- After an issue PR merges, remove its dedicated local worktree and return the primary local `main` worktree to a clean, up-to-date state.
- Each PR must link its issue explicitly; use `Closes #...` for auto-close on merge and `Refs #...` for partial or stacked work.
