# Conformance Harness

This directory is for tests that check shared specs against expected behavior.

Likely contents later:

- canonical cases tied to spec text
- fixtures for types, null handling, and edge cases
- result snapshots or assertions

Current rule: issue #10 approves the first narrow local conformance slice. Keep canonical cases documented here, and keep executable coverage tightly scoped to that documented projection path until broader harness coverage is approved.

Issue #25 adds the current local fixture expectation for that slice: prefer asserting through `tiforth_kernel::LocalExecutionFixture`, exported from `LocalExecutionSnapshot`, when recording milestone-1 runtime and admission outcomes for local Rust-side coverage.

Issue #27 adds the first checked-in fixture files for that carrier under `tests/conformance/fixtures/local-execution/`. The format and scope for those local JSON artifacts are described in `tests/conformance/local-execution-fixtures.md`.

Issue #139 adds the first post-gate filter conformance case checkpoint in `tests/conformance/first-filter-is-not-null-slice.md`.

Issue #141 adds the signed/unsigned interaction checkpoint in `tests/conformance/signed-unsigned-interaction-checkpoint.md`.

Issue #176 adds the first temporal `date32` conformance checkpoint in `tests/conformance/first-temporal-date32-slice.md`.
