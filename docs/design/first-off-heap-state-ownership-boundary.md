# First Off-Heap State Ownership Boundary

Status: issue #282 design checkpoint

Verified: 2026-03-20

Related issues:

- #8 `design: host memory admission ABI for tiforth`
- #19 `design: define milestone-1 Arrow batch handoff and memory-ownership contract`
- #123 `design: define milestone-1 spill and retry runtime mapping`
- #169 `design: define first in-contract exchange runtime slice`
- #282 `design: define first off-heap state ownership boundary`

## Question

How should shared contracts represent off-heap resident state after milestone 1 without changing the adopted Arrow batch handoff envelope?

## Inputs Considered

- `docs/contracts/data.md`
- `docs/contracts/runtime.md`
- `docs/design/host-memory-admission-abi.md`
- `docs/design/spill-retry-runtime-mapping.md`
- `docs/design/first-in-contract-exchange-slice.md`
- issue #282

## Design Summary

The first off-heap ownership checkpoint keeps the existing milestone-1 envelope and claim rules intact while naming one explicit representation boundary for resident non-batch state.

For this checkpoint:

- off-heap resident state means governed admitted bytes that are reachable from operator or runtime-owned state but are not currently reachable from any live emitted Arrow batch
- off-heap resident state still follows reserve-before-allocate admission and remains attributable to a live memory consumer while resident
- `claims[]` attached to a live batch continue to describe only resident governed bytes that remain reachable from that batch
- resident off-heap state that is not batch-reachable remains governed under local consumer ownership until release, spill, or conversion into batch-reachable buffers
- spilled non-resident bytes remain outside live-batch claim scope and may re-enter residency only through fresh reserve-before-allocate paths

## Handoff Implications

- when a stage materializes emitted batch buffers from previously off-heap resident state, outgoing `claims[]` must cover only the resident bytes that remain batch-reachable after handoff
- if a stage keeps separate resident off-heap state after emission, that state stays governed under local ownership and is not merged into outgoing batch claims
- when off-heap resident bytes become non-resident spill, the corresponding resident ownership bytes must be shrunk or released from the live consumer according to existing admission rules

## Runtime Observability Boundary

This checkpoint does not add new shared runtime states or a new required event family. Existing admission and runtime outcome meanings remain sufficient:

- reserve and release behavior stays observable through existing admission events
- batch handoff and release behavior stays observable through existing runtime events
- adapter-visible sidecar evidence remains optional unless a later slice explicitly requires off-heap carrier fields

## Deferred Boundary

This checkpoint does not define:

- a shared serialized off-heap payload format
- adapter-visible request or response fields for off-heap state
- spill file formats, storage engines, or transport protocols
- direct host-allocator-backed Arrow buffer requirements

## Result

Off-heap ownership is now explicit in shared contracts: resident non-batch state is governed and attributable, batch claims remain batch-reachability-only, and spill plus rehydration continue to use existing reserve-first ownership rules.
