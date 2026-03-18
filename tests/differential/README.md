# Differential Harness

This directory is for tests that compare behavior across TiDB, TiFlash, and TiKV.

Current checkpoint:

- `tests/differential/first-expression-slice.md` defines the first differential target: TiDB versus TiFlash on the milestone-1 expression-projection semantic core
- `adapters/first-expression-slice.md` defines the minimal request and response surface for that TiDB-versus-TiFlash slice
- `inventory/first-expression-slice-tidb-vs-tiflash-drift-report.md` defines the minimum artifact shape that follow-on differential harness work should emit for that slice
- `docs/process/inventory-artifact-naming.md` defines how future checked-in inventory evidence should be named
- `docs/process/inventory-refresh.md` defines when differential evidence should be checked into git or refreshed in follow-on PRs

Likely contents later:

- cross-engine case definitions
- adapters for query or expression execution
- drift reports and mismatch triage artifacts

Current rule: use this directory to define comparison strategy before introducing executable harness code.
