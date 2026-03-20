# Adapter-Visible Batch-Envelope Claim Carrier

Status: issue #274 design checkpoint

Verified: 2026-03-20

Related issues:

- #121 `design: define adapter-visible runtime event carrier boundary`
- #135 `design: define adoption gate for shared runtime event streaming`
- #274 `design: define adapter-visible batch-envelope claim carrier boundary`

## Question

When adapter-visible integrations need batch-level ownership observability, what
is the first shared carrier boundary for `batch_id`, `origin`, and `claims[]`
without widening milestone-1 case-result contracts or introducing streaming
requirements?

## Inputs Considered

- `docs/contracts/data.md`
- `docs/contracts/runtime.md`
- `docs/design/adapter-visible-runtime-event-carrier.md`
- `docs/design/runtime-event-streaming-adoption-gate.md`
- issue #274

## Design Summary

The first claim-carrier checkpoint is a sidecar-first boundary:

- keep milestone-1 adapter `case result` payloads unchanged
- keep `LocalExecutionSnapshot` internal to local Rust execution paths
- keep runtime-event sidecars on `LocalExecutionFixture` as already defined by
  issue #121
- add one separate optional serialized carrier boundary for batch-envelope claim
  observability when a slice explicitly opts in

This keeps ownership observability explicit without forcing a global event stream
or embedding full batch-envelope claim payloads into every differential case
result.

## Carrier Boundary

### 1. Milestone-1 default

For milestone 1, adapters and harnesses should continue to treat full
batch-envelope claim details as optional evidence.

- first-slice adapter request and `case result` contracts stay unchanged
- existing `LocalExecutionFixture` event-sidecar translation remains the default
  adapter-visible observability carrier
- no shared callback or streaming surface is added by this checkpoint

### 2. First serialized claim-carrier shape

When a follow-on slice needs explicit batch-envelope claim detail, it should use
one optional sidecar document with this semantic shape:

- top-level identity: `query_ref` and `stage_ref`
- `batches[]`, where each batch record includes:
  - `batch_id`
  - `origin` (query, stage, and operator identity)
  - `claims[]`
- each claim record includes at minimum:
  - `consumer_ref`: stable consumer identity
  - `live_bytes`: admitted live bytes tied to the batch
  - `domain`: ownership-domain label for review and drift explanation

This checkpoint defines required semantic fields only. It does not freeze one
Rust struct name, FFI layout, or transport API.

### 3. Attachment and refresh policy

- claim-carrier sidecars remain optional until a slice doc marks them required
- when a slice requires this carrier, that slice must also define artifact names
  and refresh expectations under `tests/differential/*-artifacts.md` and
  `docs/process/inventory-refresh.md`
- claim-carrier sidecars should remain review-first, normalized, and free of
  unstable host-local noise

## Why This Boundary

- keeps current milestone-1 adapter boundaries stable
- separates runtime-event fixture transport from richer claim-envelope detail
- prevents premature coupling to callback or streaming protocol choices
- gives later slices a concrete minimal field set for ownership observability

## Follow-On Boundary

Later issues may extend this checkpoint to define:

- the first concrete slice that requires claim-carrier sidecars
- optional-to-required promotion rules for those sidecars
- compatibility/versioning rules if multiple claim-carrier schema revisions
  become necessary
- whether any future shared streaming surface should mirror these fields after
  satisfying `docs/design/runtime-event-streaming-adoption-gate.md`
