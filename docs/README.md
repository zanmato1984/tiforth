# TiForth docs

This directory contains **user-facing** documentation for the standalone TiForth library.

- `architecture.md`: overall architecture, data model, execution model.
- `pipeline_apis.md`: pipeline/task APIs (C++ and C ABI).
- `type_mapping_tidb_to_arrow.md`: TiDB/MySQL logical types -> Arrow physical types + TiForth field-metadata contract (detailed).
- `operators_and_functions.md`: operator model, built-in operators, and expression/function fundamentals.

Related docs:

- Design/milestones: tracked in the integrating host project (not in this repo yet)
- Consumer build/packaging examples: `examples/README.md`
