# AGENTS

This file is a table of contents for future agents. The source of truth lives under `docs/` and `plans/`.

## Read In This Order

1. `README.md`
2. `docs/vision.md`
3. `docs/architecture.md`
4. `docs/contracts/data.md`
5. `docs/contracts/runtime.md`
6. `docs/spec/type-system.md`
7. `plans/active/`

## Where Things Go

- `docs/`: intended architecture, contracts, and semantic spec
- `plans/`: execution plans and status
- `inventory/`: extracted catalogs, donor analysis, drift reports
- `tests/`: harness definitions and fixtures
- `adapters/`: boundary docs for engine integration points

## Operating Rules

- Treat `docs/` as the source of truth. Update docs before adding code.
- Do not add concrete operators or functions until their specs and harness coverage exist.
- Preserve Apache Arrow as the data-contract direction and broken-pipeline ideas as runtime inspiration, but do not copy donor implementations by default.
- Record unresolved decisions as TODOs with context instead of forcing premature choices.
- Do not add a build system, package manager, CI workflow, or implementation runtime unless a plan explicitly requires it.
