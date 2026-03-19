# TiKV Adapter

This directory defines the TiKV-facing adapter boundary.

The adapter should eventually translate TiKV expression and operator behavior into shared specs and contracts without turning adapter code into the semantic source of truth.

Current checkpoint:

- the first shared adapter boundary in `adapters/first-expression-slice.md` remains intentionally limited to TiDB and TiFlash
- `adapters/first-expression-slice-tikv.md` now defines the first TiKV-specific request and response boundary for `first-expression-slice`
- `docs/design/adapter-milestone-breakdown.md` records why TiKV follows the initial TiDB/TiFlash pairwise checkpoint sequence
- `docs/design/adapter-runtime-orchestration-boundary.md` fixes which TiKV environment, timeout, retry, cancellation, and diagnostic concerns should stay adapter-local
- `crates/tiforth-adapter-tikv` now encodes `first-expression-slice` request catalogs with TiKV-oriented SQL lowering and row / error normalization behind a runner boundary

Next checkpoint:

- document TiKV-specific semantic mismatches from executable first-expression-slice runs as compatibility notes once that inventory checkpoint is accepted

## TODOs

- extend the TiKV request and response surface beyond `first-expression-slice` after the first executable checkpoint and compatibility notes are reviewable
