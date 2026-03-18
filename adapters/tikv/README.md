# TiKV Adapter

This directory defines the TiKV-facing adapter boundary.

The adapter should eventually translate TiKV expression and operator behavior into shared specs and contracts without turning adapter code into the semantic source of truth.

Current checkpoint:

- the first shared adapter boundary in `adapters/first-expression-slice.md` is intentionally limited to TiDB and TiFlash; TiKV remains a follow-on differential adapter issue
- `docs/design/adapter-milestone-breakdown.md` keeps TiKV out of the first executable differential checkpoint until the TiDB-versus-TiFlash path is reviewable

Next checkpoint:

- wait for the TiDB and TiFlash single-engine checkpoints plus the first pairwise drift-report checkpoint before proposing a TiKV-specific adapter boundary

## TODOs

- define the minimal request and response surface needed by harnesses
- document TiKV-specific semantic mismatches found during inventory
- define which runtime concerns stay adapter-local
