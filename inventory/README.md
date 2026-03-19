# Inventory

`inventory/` is reserved for extracted catalogs, drift reports, and other durable reviewable evidence.

Expected future contents:

- donor-derived function catalogs
- donor-derived operator catalogs
- engine compatibility notes
- semantic drift reports
- coverage gap reports

Rules:

- treat inventory as evidence, not design authority
- keep extracted facts separate from proposed shared specs
- prefer machine-readable formats when practical, but do not introduce tooling yet
- keep raw or unstable local captures out of git until a docs-defined normalized artifact shape exists

Source-of-truth process guidance:

- `docs/process/inventory-artifact-naming.md` defines how checked-in inventory artifacts should be named
- `docs/process/inventory-refresh.md` defines when inventory evidence should be checked into git or refreshed in follow-on PRs

Current checkpoint:

- `tests/differential/drift-report-carrier.md` defines the reusable minimum carrier for differential `drift-report` artifacts across slices
- `tests/differential/first-expression-slice-artifacts.md` defines the stable carrier for first-slice `case-results` artifacts plus first-slice constraints on the shared `drift-report` carrier
- `tests/differential/first-filter-is-not-null-slice-artifacts.md` defines the stable carrier for first-filter `case-results` artifacts plus first-filter constraints on the shared `drift-report` carrier
- `inventory/first-expression-slice-tidb-case-results.json` records the current TiDB-side case results for the executable first-slice harness checkpoint
- `inventory/first-expression-slice-tiflash-case-results.json` records the current TiFlash-side case results for the same checkpoint
- `inventory/first-expression-slice-tidb-vs-tiflash-drift-report.md` records the current paired TiDB-versus-TiFlash classification summary
- `inventory/first-expression-slice-tidb-vs-tiflash-drift-report.json` records the paired machine-readable TiDB-versus-TiFlash drift-report sidecar for the same first-slice checkpoint
- `inventory/first-filter-is-not-null-slice-tidb-case-results.json` records the current TiDB-side case results for the executable first-filter harness checkpoint
- `inventory/first-filter-is-not-null-slice-tiflash-case-results.json` records the current TiFlash-side case results for the same first-filter checkpoint
- `inventory/first-filter-is-not-null-slice-tidb-vs-tiflash-drift-report.md` records the current paired TiDB-versus-TiFlash classification summary for the first-filter checkpoint
- `inventory/first-filter-is-not-null-slice-tidb-vs-tiflash-drift-report.json` records the paired machine-readable TiDB-versus-TiFlash drift-report sidecar for the same first-filter checkpoint
- `inventory/first-expression-slice-coverage-gap.md` records the current deferred and unsupported first-slice coverage edges that still need explicit follow-on decisions
- `inventory/first-expression-slice-legacy-function-catalog.md` records the first donor function catalog for `literal<int32>(value)` and `add<int32>(lhs, rhs)` within the `first-expression-slice`
- `inventory/first-expression-slice-legacy-operator-catalog.md` records the matching donor operator catalog for the single-input projection family, including direct passthrough ordering and row-count preservation evidence within the `first-expression-slice`
- `inventory/first-expression-slice-tidb-compat-notes.md` records the first TiDB-side compatibility notes for the shared projection, `column`, `literal<int32>`, and `add<int32>` surface
- `inventory/first-expression-slice-tiflash-compat-notes.md` records the matching TiFlash-side compatibility notes for the shared projection, `column`, `literal<int32>`, and `add<int32>` surface
- earlier checkpoint notes remain in `inventory/` until a follow-on issue chooses to rename or retire them
- issue #159 fixes milestone-1 drift-report sidecar policy: checked-in Markdown `drift-report` artifacts are required and machine-readable sidecars remain optional
- issue #161 adds the first checked-in JSON drift-report sidecars for the first-expression and first-filter differential slices

Current inventory priority:

- `docs/design/first-inventory-wave.md` defines the first donor and engine inventory scope
- that first wave stays inside the milestone-1 projection core and the `first-expression-slice` semantic surface
