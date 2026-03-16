# Language Constraints

This file records evidence and constraints relevant to the shared-kernel language decision.

It is not the place to declare the winner.

## Decision Scope

In scope:

- language for the shared `tiforth` kernel / runtime core
- implications for Arrow-native data handling
- implications for broken-pipeline-inspired runtime work
- implications for TiDB / TiFlash / TiKV integration

Out of scope:

- adapter language standardization
- repo-wide tooling standardization beyond what the core decision requires

## Priority Concerns From Rossi

Pending input.

This section should be updated first when Rossi supplies the most important decision axes.

## Baseline Constraints To Investigate

### 1. Donor Leverage

Questions:

- Which parts of `tiforth-legacy` are easier to reinterpret if the new core is C++?
- Which donor concepts remain equally portable regardless of implementation language?
- Where would choosing C++ accidentally encourage carrying forward implementation debt?

### 2. Host Project Integration

Questions:

- What is the practical embedding cost for TiFlash?
- What is the practical embedding cost for TiKV?
- What is the practical embedding cost for TiDB?
- Which integration surfaces are likely to require stable C ABI boundaries either way?

### 3. Arrow And Data Contract Fit

Questions:

- How mature is the Arrow story needed by tiforth in each language?
- Where do ownership and buffer-lifetime rules become easier or harder?
- Does either choice make the shared data contract easier to keep engine-neutral?

### 4. Runtime Contract Fit

Questions:

- Which language better supports the runtime properties tiforth likely needs: staged execution, interruption, backpressure, cancellation, observability?
- Where does concurrency become safer versus more predictable versus more debuggable?
- How much scheduler freedom is realistically needed in the first milestones?

### 5. Build, Debug, And Tooling Cost

Questions:

- What does local development look like for contributors?
- What does cross-project integration testing look like?
- Which option creates more friction in build graph, symbol management, packaging, or debugging?

### 6. Long-Term Maintenance

Questions:

- Which language better protects the project from memory and concurrency bugs in the core?
- Which language better matches the maintenance burden tiforth is willing to carry?
- Which language will make later contributors faster or slower to make correct changes?

## Known Structural Tension

There is an obvious tension between:

- maximizing TiFlash donor leverage and immediate practicality
- maximizing reboot cleanliness and long-term kernel quality

The decision process should make that tension explicit instead of hiding it inside implementation momentum.

## Evidence Rules

- record observations before conclusions
- separate facts from preferences
- if a concern is speculative, mark it as speculative
- prefer repo-local notes over chat-only conclusions
