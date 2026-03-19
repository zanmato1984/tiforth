# First Exchange Slice Artifact Carriers

Status: issue #183 harness checkpoint, issue #221 artifact-carrier checkpoint

Related issues:

- #169 `design: define first in-contract exchange slice boundary`
- #183 `harness: wire first-exchange-slice differential parity checkpoint`
- #221 `docs: define first exchange differential artifact carriers`

## Purpose

This note defines the stable differential artifact carriers for
`first-exchange-slice` from `tests/differential/first-exchange-slice.md`.

The goal is to keep artifact shape under source-of-truth harness docs while
checked-in `inventory/` files remain reviewable evidence instead of schema
authority.

The executable parity harness for this checkpoint lives in
`crates/tiforth-harness-differential/src/first_exchange_slice.rs`.

## Artifact Set

The first exchange parity checkpoint defines two artifacts:

1. one baseline-versus-exchange Markdown `drift-report`
2. one machine-readable baseline-versus-exchange `drift-report` sidecar

Expected artifact filenames for this slice:

- `inventory/first-exchange-slice-baseline-vs-exchange-drift-report.md`
- `inventory/first-exchange-slice-baseline-vs-exchange-drift-report.json`

## `drift-report` Carrier For This Slice

This first exchange checkpoint compares baseline and exchange execution paths
over already-defined first-expression and first-filter case IDs. Unlike
TiDB-versus-TiFlash slice reports, this carrier is parity-focused and therefore
uses a `subject` field rather than a top-level engine pair.

Each report should record at least:

- top-level `slice_id`
- top-level `spec_refs[]`
- summary status counts for every status used in this report
- `cases[]`, where each case entry includes:
  - `subject`
  - `slice_id`
  - `case_id`
  - optional `engine`
  - `status`
  - `comparison_dimensions[]`
  - `summary`

Allowed `subject` values for this slice:

- `tidb_case_results`
- `tiflash_case_results`
- `tidb_vs_tiflash_drift`

Status vocabulary for this slice:

- `match`: baseline and exchange paths stayed equivalent for the compared
  dimensions
- `drift`: baseline and exchange paths diverged for one or more compared
  dimensions

Allowed `comparison_dimensions[]` values for this slice:

- `outcome_kind`
- `error_class`
- `field_name`
- `field_nullability`
- `logical_type`
- `row_count`
- `row_values`
- `drift_status`

## Machine-Readable Sidecar Boundary

The machine-readable sidecar should mirror the same normalized report fields as
the paired Markdown artifact:

- `slice_id`
- `spec_refs[]`
- `cases[]` with the same case identity, status vocabulary, and comparison
  dimensions

This checkpoint does not require sidecar-only fields that are absent from the
Markdown report.

## Inventory Refresh Boundary

Issue #221 defines the carrier contract for this slice; it does not require
checking in refreshed exchange parity artifacts in the same PR.

Follow-on PRs that check in or refresh first-exchange artifacts should update
both files above when any of these change:

- reused slice case IDs
- compared dimensions or status vocabulary
- parity conclusions in case summaries
- spec references for the exchange parity checkpoint

## Boundary For Now

The first exchange artifact carriers are intentionally narrow.

They do not yet define:

- live engine exchange orchestration captures
- multi-topology exchange artifact families beyond first local parity checks
- merged summaries across more than one parity mode in one report
