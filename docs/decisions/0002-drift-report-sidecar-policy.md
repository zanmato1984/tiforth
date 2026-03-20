# 0002 - Differential Drift-Report Machine-Readable Sidecar Policy

Status: accepted

Date: 2026-03-20

Related issues:

- #133 `design: define reusable differential drift-report carrier guidance`
- #159 `docs: define machine-readable sidecar policy for differential drift reports`
- #161 `harness: add machine-readable drift-report sidecars for first differential slices`
- #221 `docs: define first exchange differential artifact carriers`
- #256 `docs: decide cross-slice machine-readable drift-report schema policy`

## Context

`docs/architecture.md` has carried an open TODO about when `tiforth` should
require machine-readable differential `drift-report` schema coverage across
slices.

Earlier checkpoints established:

- review-first Markdown `drift-report` artifacts
- optional JSON sidecars in milestone-1 documentation
- executable harness paths that already emit paired Markdown and JSON artifacts
  for the current first expression, first filter, first temporal, first
  decimal, and first float64 slices, including TiKV pairwise variants where
  defined

At this point, the optional-sidecar wording is stale for current repository
practice and leaves avoidable ambiguity for future slice PRs.

## Decision

### 1. Sidecar requirement

For differential slices that check in a `drift-report` artifact, the
machine-readable JSON sidecar is required alongside the Markdown report.

In practice, slice completion now expects both:

- `inventory/<subject>-drift-report.md`
- `inventory/<subject>-drift-report.json`

### 2. Shared minimum machine-readable carrier

For engine-pair differential slices that use the shared carrier in
`tests/differential/drift-report-carrier.md`, the sidecar must carry the same
semantic report identity as the paired Markdown artifact:

- `slice_id`
- `engines[]`
- `spec_refs[]`
- status vocabulary and case statuses sufficient to produce summary counts
- `cases[]` entries with shared case identity, status vocabulary, compared
  dimensions, summary text, and evidence references

### 3. Family-specific extensions remain explicit

This decision does not force every differential report family into one
monolithic JSON struct today.

Slice families with intentionally different compared subjects (for example,
baseline-versus-exchange parity in
`tests/differential/first-exchange-slice-artifacts.md`) may define additional
or alternate case identity fields through docs-first updates, while still
requiring paired Markdown and JSON artifacts.

### 4. Aggregation boundary

Differential evidence remains one compared pair (or one explicitly documented
parity subject) per checked-in drift report artifact. Merged multi-pair
summaries stay out of scope until a follow-on decision says otherwise.

## Alternatives Considered

### Keep sidecars optional

Rejected. Current repository evidence already uses sidecars broadly, and
optional wording causes policy drift between docs and practice.

### Force one strict global JSON struct now

Rejected for now. Current slice families include at least one parity-focused
carrier whose case identity differs from engine-pair reports. Forcing one
global struct now would add churn without clear review value.

## Consequences

### Positive

- future differential PRs get an explicit machine-readable artifact requirement
- architecture docs can stop carrying an unresolved sidecar-policy TODO
- shared carrier expectations stay concrete without overfitting one global
  struct prematurely

### Tradeoffs

- every differential artifact refresh now includes both Markdown and JSON files
- slice-specific carrier docs must stay disciplined when introducing
  family-specific fields

## Follow-Up

- if future slices converge on one common case identity shape, consider a
  stricter shared JSON schema plus validation tooling as a dedicated issue
- revisit merged multi-pair summaries only when there is concrete review need
  beyond current pairwise or parity-scoped reports
