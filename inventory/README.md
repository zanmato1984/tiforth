# Inventory

`inventory/` is reserved for extracted catalogs and drift reports.

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

Source-of-truth naming guidance:

- `docs/process/inventory-artifact-naming.md` defines how checked-in inventory artifacts should be named

Current checkpoint:

- `inventory/first-expression-slice-tidb-vs-tiflash-drift-report.md` documents the first differential drift artifact shape
- earlier checkpoint notes remain in `inventory/` until a follow-on issue chooses to rename or retire them

## TODOs

- extend drift report formats beyond the first differential expression slice
- decide when inventory should be checked into git versus regenerated
