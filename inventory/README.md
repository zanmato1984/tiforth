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

- `inventory/first-expression-slice-tidb-vs-tiflash-drift-report.md` documents the first differential drift artifact shape
- earlier checkpoint notes remain in `inventory/` until a follow-on issue chooses to rename or retire them

## TODOs

- extend drift report formats beyond the first differential expression slice
