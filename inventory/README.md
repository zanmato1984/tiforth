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

## TODOs

- define naming conventions for extracted artifacts
- define the first drift report format
- decide when inventory should be checked into git versus regenerated
