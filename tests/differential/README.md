# Differential Harness

This directory is for tests that compare behavior across TiDB, TiFlash, and TiKV.

Current checkpoint:

- `tests/differential/first-expression-slice.md` defines the first differential target: TiDB versus TiFlash on the milestone-1 expression-projection semantic core
- `inventory/differential-expression-drift-report.md` defines the minimum artifact shape that follow-on differential harness work should emit for that slice

Likely contents later:

- cross-engine case definitions
- adapters for query or expression execution
- drift reports and mismatch triage artifacts

Current rule: use this directory to define comparison strategy before introducing executable harness code.
