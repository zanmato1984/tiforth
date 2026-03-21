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
- `tests/differential/first-temporal-date32-slice-artifacts.md` defines the stable carrier for first-temporal `date32` case-results artifacts plus first-temporal constraints on the shared `drift-report` carrier
- `tests/differential/first-temporal-timestamp-tz-slice-artifacts.md` defines the stable carrier for first-temporal timezone-aware `timestamp_tz(us)` case-results artifacts plus first-temporal timestamp-timezone constraints on the shared `drift-report` carrier
- `tests/differential/first-decimal128-slice-artifacts.md` defines the stable carrier for first-decimal `decimal128` case-results artifacts plus first-decimal constraints on the shared `drift-report` carrier
- `tests/differential/first-float64-ordering-slice-artifacts.md` defines the stable carrier for first-float64 ordering case-results artifacts plus first-float64 constraints on the shared `drift-report` carrier
- `tests/differential/first-unsigned-arithmetic-slice-artifacts.md` defines the stable carrier for first unsigned arithmetic `uint64` case-results artifacts plus first-unsigned constraints on the shared `drift-report` carrier
- `tests/differential/first-exchange-slice-artifacts.md` defines the stable carrier for the first baseline-versus-exchange parity `drift-report` artifacts
- `inventory/first-expression-slice-tidb-case-results.json` records the current TiDB-side case results for the executable first-slice harness checkpoint
- `inventory/first-expression-slice-tiflash-case-results.json` records the current TiFlash-side case results for the same checkpoint
- `inventory/first-expression-slice-tikv-case-results.json` records the current TiKV-side single-engine case results for the same first-expression checkpoint
- `inventory/first-expression-slice-tidb-vs-tiflash-drift-report.md` records the current paired TiDB-versus-TiFlash classification summary
- `inventory/first-expression-slice-tidb-vs-tiflash-drift-report.json` records the paired machine-readable TiDB-versus-TiFlash drift-report sidecar for the same first-slice checkpoint
- `inventory/first-expression-slice-tidb-vs-tikv-drift-report.md` records the current paired TiDB-versus-TiKV classification summary for the same first-expression checkpoint
- `inventory/first-expression-slice-tidb-vs-tikv-drift-report.json` records the paired machine-readable TiDB-versus-TiKV drift-report sidecar for that first-expression checkpoint
- `inventory/first-expression-slice-tiflash-vs-tikv-drift-report.md` records the current paired TiFlash-versus-TiKV classification summary for the same first-expression checkpoint
- `inventory/first-expression-slice-tiflash-vs-tikv-drift-report.json` records the paired machine-readable TiFlash-versus-TiKV drift-report sidecar for that first-expression checkpoint
- `inventory/first-filter-is-not-null-slice-tidb-case-results.json` records the current TiDB-side case results for the executable first-filter harness checkpoint
- `inventory/first-filter-is-not-null-slice-tiflash-case-results.json` records the current TiFlash-side case results for the same first-filter checkpoint
- `inventory/first-filter-is-not-null-slice-tidb-vs-tiflash-drift-report.md` records the current paired TiDB-versus-TiFlash classification summary for the first-filter checkpoint
- `inventory/first-filter-is-not-null-slice-tidb-vs-tiflash-drift-report.json` records the paired machine-readable TiDB-versus-TiFlash drift-report sidecar for the same first-filter checkpoint
- `inventory/first-filter-is-not-null-slice-tidb-vs-tikv-drift-report.md` records the current paired TiDB-versus-TiKV classification summary for the first-filter checkpoint
- `inventory/first-filter-is-not-null-slice-tidb-vs-tikv-drift-report.json` records the paired machine-readable TiDB-versus-TiKV drift-report sidecar for the same first-filter checkpoint
- `inventory/first-filter-is-not-null-slice-tiflash-vs-tikv-drift-report.md` records the current paired TiFlash-versus-TiKV classification summary for the first-filter checkpoint
- `inventory/first-filter-is-not-null-slice-tiflash-vs-tikv-drift-report.json` records the paired machine-readable TiFlash-versus-TiKV drift-report sidecar for the same first-filter checkpoint
- `inventory/first-exchange-slice-baseline-vs-exchange-drift-report.md` records the current baseline-versus-exchange parity summary for the first exchange checkpoint
- `inventory/first-exchange-slice-baseline-vs-exchange-drift-report.json` records the paired machine-readable baseline-versus-exchange parity sidecar for the same exchange checkpoint
- `inventory/first-temporal-date32-slice-tidb-case-results.json` records the current TiDB-side case results for the first temporal `date32` harness checkpoint
- `inventory/first-temporal-date32-slice-tiflash-case-results.json` records the current TiFlash-side case results for the same first temporal checkpoint
- `inventory/first-temporal-date32-slice-tikv-case-results.json` records the current TiKV-side single-engine case results for the same first temporal checkpoint
- `inventory/first-temporal-date32-slice-tidb-vs-tiflash-drift-report.md` records the current paired TiDB-versus-TiFlash classification summary for the first temporal checkpoint
- `inventory/first-temporal-date32-slice-tidb-vs-tiflash-drift-report.json` records the paired machine-readable TiDB-versus-TiFlash drift-report sidecar for the same first temporal checkpoint
- `inventory/first-temporal-timestamp-tz-slice-tidb-case-results.json` records the current TiDB-side case results for the first temporal timezone-aware `timestamp_tz(us)` harness checkpoint
- `inventory/first-temporal-timestamp-tz-slice-tiflash-case-results.json` records the current TiFlash-side case results for the same first temporal timezone-aware checkpoint
- `inventory/first-temporal-timestamp-tz-slice-tidb-vs-tiflash-drift-report.md` records the current paired TiDB-versus-TiFlash classification summary for the first temporal timezone-aware checkpoint
- `inventory/first-temporal-timestamp-tz-slice-tidb-vs-tiflash-drift-report.json` records the paired machine-readable TiDB-versus-TiFlash drift-report sidecar for the same first temporal timezone-aware checkpoint
- `inventory/first-temporal-date32-slice-tidb-vs-tikv-drift-report.md` records the current paired TiDB-versus-TiKV classification summary for the same first temporal checkpoint
- `inventory/first-temporal-date32-slice-tidb-vs-tikv-drift-report.json` records the paired machine-readable TiDB-versus-TiKV drift-report sidecar for that first temporal checkpoint
- `inventory/first-temporal-date32-slice-tiflash-vs-tikv-drift-report.md` records the current paired TiFlash-versus-TiKV classification summary for the same first temporal checkpoint
- `inventory/first-temporal-date32-slice-tiflash-vs-tikv-drift-report.json` records the paired machine-readable TiFlash-versus-TiKV drift-report sidecar for that first temporal checkpoint
- `inventory/first-decimal128-slice-tidb-case-results.json` records the current TiDB-side case results for the first decimal `decimal128` harness checkpoint
- `inventory/first-decimal128-slice-tiflash-case-results.json` records the current TiFlash-side case results for the same first decimal checkpoint
- `inventory/first-decimal128-slice-tidb-vs-tiflash-drift-report.md` records the current paired TiDB-versus-TiFlash classification summary for the first decimal checkpoint
- `inventory/first-decimal128-slice-tidb-vs-tiflash-drift-report.json` records the paired machine-readable TiDB-versus-TiFlash drift-report sidecar for the same first decimal checkpoint
- `inventory/first-float64-ordering-slice-tidb-case-results.json` records the current TiDB-side case results for the first float64 ordering harness checkpoint
- `inventory/first-float64-ordering-slice-tiflash-case-results.json` records the current TiFlash-side case results for the same first float64 ordering checkpoint
- `inventory/first-float64-ordering-slice-tikv-case-results.json` records the current TiKV-side single-engine case results for the same first float64 ordering checkpoint
- `inventory/first-float64-ordering-slice-tidb-vs-tiflash-drift-report.md` records the current paired TiDB-versus-TiFlash classification summary for the first float64 ordering checkpoint
- `inventory/first-float64-ordering-slice-tidb-vs-tiflash-drift-report.json` records the paired machine-readable TiDB-versus-TiFlash drift-report sidecar for the same first float64 ordering checkpoint
- `inventory/first-unsigned-arithmetic-slice-tidb-case-results.json` records the current TiDB-side case results for the first unsigned arithmetic `uint64` harness checkpoint
- `inventory/first-unsigned-arithmetic-slice-tiflash-case-results.json` records the current TiFlash-side case results for the same first unsigned arithmetic checkpoint
- `inventory/first-unsigned-arithmetic-slice-tikv-case-results.json` records the current TiKV-side single-engine case results for the same first unsigned arithmetic checkpoint
- `inventory/first-unsigned-arithmetic-slice-tidb-vs-tiflash-drift-report.md` records the current paired TiDB-versus-TiFlash classification summary for the first unsigned arithmetic checkpoint
- `inventory/first-unsigned-arithmetic-slice-tidb-vs-tiflash-drift-report.json` records the paired machine-readable TiDB-versus-TiFlash drift-report sidecar for the same first unsigned arithmetic checkpoint
- `inventory/first-unsigned-arithmetic-slice-tidb-vs-tikv-drift-report.md` records the current paired TiDB-versus-TiKV classification summary for the same first unsigned arithmetic checkpoint
- `inventory/first-unsigned-arithmetic-slice-tidb-vs-tikv-drift-report.json` records the paired machine-readable TiDB-versus-TiKV drift-report sidecar for that first unsigned arithmetic checkpoint
- `inventory/first-unsigned-arithmetic-slice-tiflash-vs-tikv-drift-report.md` records the current paired TiFlash-versus-TiKV classification summary for the same first unsigned arithmetic checkpoint
- `inventory/first-unsigned-arithmetic-slice-tiflash-vs-tikv-drift-report.json` records the paired machine-readable TiFlash-versus-TiKV drift-report sidecar for that first unsigned arithmetic checkpoint
- `inventory/first-filter-is-not-null-slice-tidb-compat-notes.md` records TiDB-side compatibility notes for the first-filter predicate slice
- `inventory/first-filter-is-not-null-slice-tiflash-compat-notes.md` records TiFlash-side compatibility notes for the same first-filter predicate slice
- `inventory/first-temporal-date32-slice-tidb-compat-notes.md` records TiDB-side compatibility notes for the first temporal `date32` differential slice
- `inventory/first-temporal-date32-slice-tiflash-compat-notes.md` records TiFlash-side compatibility notes for the same first temporal differential slice
- `inventory/first-temporal-date32-slice-tikv-compat-notes.md` records TiKV-side compatibility notes for the same first temporal differential slice
- `inventory/first-temporal-timestamp-tz-slice-tidb-compat-notes.md` records TiDB-side compatibility notes for the first temporal timezone-aware `timestamp_tz(us)` differential slice
- `inventory/first-temporal-timestamp-tz-slice-tiflash-compat-notes.md` records TiFlash-side compatibility notes for the same first temporal timezone-aware differential slice
- `inventory/first-temporal-timestamp-tz-slice-tikv-compat-notes.md` records TiKV-side compatibility notes for the same first temporal timezone-aware differential slice
- `inventory/first-decimal128-slice-tidb-compat-notes.md` records TiDB-side compatibility notes for the first decimal `decimal128` differential slice
- `inventory/first-decimal128-slice-tiflash-compat-notes.md` records TiFlash-side compatibility notes for the same first decimal differential slice
- `inventory/first-float64-ordering-slice-tidb-compat-notes.md` records TiDB-side compatibility notes for the first float64 ordering differential slice
- `inventory/first-float64-ordering-slice-tiflash-compat-notes.md` records TiFlash-side compatibility notes for the same first float64 ordering differential slice
- `inventory/first-float64-ordering-slice-tikv-compat-notes.md` records TiKV-side compatibility notes for the same first float64 ordering differential slice
- `inventory/first-unsigned-arithmetic-slice-tidb-compat-notes.md` records TiDB-side compatibility notes for the first unsigned arithmetic `uint64` differential slice
- `inventory/first-unsigned-arithmetic-slice-tiflash-compat-notes.md` records TiFlash-side compatibility notes for the same first unsigned arithmetic differential slice
- `inventory/first-unsigned-arithmetic-slice-tikv-compat-notes.md` records TiKV-side compatibility notes for the same first unsigned arithmetic differential slice
- `inventory/first-expression-slice-coverage-gap.md` records the current deferred and unsupported first-slice coverage edges that still need explicit follow-on decisions
- `inventory/first-expression-slice-legacy-function-catalog.md` records the first donor function catalog for `literal<int32>(value)` and `add<int32>(lhs, rhs)` within the `first-expression-slice`
- `inventory/first-expression-slice-legacy-operator-catalog.md` records the matching donor operator catalog for the single-input projection family, including direct passthrough ordering and row-count preservation evidence within the `first-expression-slice`
- `inventory/first-expression-slice-tidb-compat-notes.md` records the first TiDB-side compatibility notes for the shared projection, `column`, `literal<int32>`, and `add<int32>` surface
- `inventory/first-expression-slice-tiflash-compat-notes.md` records the matching TiFlash-side compatibility notes for the shared projection, `column`, `literal<int32>`, and `add<int32>` surface
- `inventory/first-expression-slice-tikv-compat-notes.md` records the first TiKV-side compatibility notes for the shared projection, `column`, `literal<int32>`, and `add<int32>` surface
- earlier checkpoint notes remain in `inventory/` until a follow-on issue chooses to rename or retire them
- issue #159 fixes milestone-1 drift-report sidecar policy: checked-in Markdown `drift-report` artifacts are required and machine-readable sidecars remain optional
- issue #161 adds the first checked-in JSON drift-report sidecars for the first-expression and first-filter differential slices
- issue #185 defines docs-first artifact carriers for the first temporal `date32` differential slice
- issue #187 adds the first checked-in temporal `date32` differential case-results artifacts plus paired drift-report outputs
- issue #206 adds the first checked-in decimal `decimal128` differential case-results artifacts plus paired drift-report outputs
- issue #208 adds the first checked-in float64 ordering differential case-results artifacts plus paired drift-report outputs
- issue #304 adds the first checked-in temporal timezone-aware `timestamp_tz(us)` differential case-results artifacts plus paired drift-report outputs
- issue #292 adds the first checked-in TiKV single-engine float64 ordering case-results and compatibility notes artifacts
- issue #204 adds the first checked-in temporal `date32` per-engine compatibility notes artifacts for TiDB and TiFlash
- issue #266 adds the first checked-in TiKV single-engine temporal `date32` case-results and compatibility notes artifacts
- issue #270 adds the first checked-in TiKV pairwise `drift-report` artifacts for `first-temporal-date32-slice` (`tidb-vs-tikv` and `tiflash-vs-tikv`)
- issue #214 adds the first checked-in decimal `decimal128` and float64 ordering per-engine compatibility notes artifacts
- issue #228 adds the first checked-in TiKV-side compatibility notes artifact for `first-expression-slice`
- issue #235 adds the first checked-in TiKV single-engine `case-results` artifact for `first-expression-slice`
- issue #245 adds the first checked-in TiKV pairwise `drift-report` artifacts for `first-expression-slice` (`tidb-vs-tikv` and `tiflash-vs-tikv`)
- issue #249 follow-on checkpoint adds the first checked-in TiKV pairwise `drift-report` artifacts for `first-filter-is-not-null-slice` (`tidb-vs-tikv` and `tiflash-vs-tikv`)
- issue #328 adds the first checked-in TiKV unsigned arithmetic single-engine `case-results`, compatibility notes, and pairwise `drift-report` artifacts

Current inventory priority:

- `docs/design/first-inventory-wave.md` defines the first donor and engine inventory scope
- that first wave stays inside the milestone-1 projection core and the `first-expression-slice` semantic surface
