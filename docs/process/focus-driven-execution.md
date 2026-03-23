# Focus-Driven Execution

`tiforth` keeps durable execution rules in repo docs and live execution state in
GitHub issues.

This document defines how contributors and agents decide what to do next
without turning repo docs into a running status log.

## Purpose

- stop opportunistic slice expansion across unrelated directions
- make the current priority explicit from one GitHub entry point
- keep future execution inside one active epic and one current concrete issue
- force blocked work to create the missing boundary inside the same epic rather
  than jumping to another direction

## Durable Rules Versus Live State

Keep these in repo docs:

- startup read order
- admission and promotion rules
- issue and epic body conventions
- naming and workflow rules that outlive one issue

Keep these in GitHub issues and PRs only:

- the current active epic
- the current concrete issue
- current checklists, blockers, handoff notes, and promotion decisions
- the ordered queue of follow-on issues inside the active epic

Do not add repo-local plan or status files for live execution tracking.

## Startup Read Order

Every work session starts in this order:

1. `AGENTS.md`
2. this document
3. the GitHub `program:` meta issue named as the live control entry
4. the active `epic:` issue named by that meta issue
5. the current concrete issue named by that meta issue

After those are read, load stable design docs from the `AGENTS.md` read order
as needed for the current issue.

`docs/vision.md` and other planning docs remain durable context only. Their
`Next Checkpoint` or other forward-looking notes do not override the live
priority unless the program meta issue explicitly promotes that direction.

## Required Live-Control Objects

### `program:` Meta Issue

There must be exactly one open `program:` meta issue that acts as the only live
control entry for the repository.

Its body should always name:

- `Current epic: #...`
- `Current issue: #...` or `Current issue: none`
- `Promotion rule:` a short statement that epic switching requires explicit
  meta-issue promotion after the current epic exit criteria are satisfied
- `Epic queue:` the ordered top-level epic list

If the current issue is `none`, the meta issue still fixes the active epic and
therefore fixes where the next issue must come from.

### `epic:` Issues

Top-level `epic:` issues define one major direction at a time, such as a
complete function-family program or a hash-join program.

Only one top-level epic may be active at a time.

Each epic issue should capture:

- goal
- scope
- non-goals
- exit criteria
- admission rule for child issues
- ordered child issue queue or next candidates

### `sub-epic:` Issues

`sub-epic:` issues are optional intermediate containers inside one top-level
epic when the active epic needs its own durable decomposition.

Sub-epics do not bypass the one-active-epic rule. They belong to the current
top-level epic only.

### Concrete Issues

Concrete issues are the only units of active implementation or docs-first work.

At any moment there may be many open issues, but there may be only:

- one active top-level epic
- one current concrete issue

Only the current concrete issue may receive active work.

## Admission Rules

### When The Meta Issue Names `Current issue: #...`

- work only that issue
- do not start a sibling issue in the same epic
- do not start an issue in another epic

### When The Meta Issue Names `Current issue: none`

- choose the first issue inside the active epic whose prerequisites are already
  satisfied
- if the active epic has sub-epics, choose from the first admitted issue in the
  first still-relevant sub-epic
- once chosen, update the meta issue so that `Current issue: #...` is explicit

### When No Admitted Issue Exists Inside The Active Epic

- open one bootstrap or docs-first issue inside the same active epic
- use that issue to settle the missing boundary, spec, contract, or inventory
  evidence that blocks the epic
- do not jump to another top-level epic just because it looks easier

### When The Current Issue Is Blocked

If the current issue is blocked by a missing accepted boundary:

- open a docs-first or bootstrap issue inside the same epic
- update the meta issue so the blocker issue becomes the current issue
- return to the blocked issue only after the blocker is resolved

Do not use a blocker as permission to open work in another epic.

## Promotion Rules

Top-level epic promotion is allowed only when both are true:

1. the current epic exit criteria are satisfied
2. the `program:` meta issue is updated explicitly to promote the next epic

Without that explicit meta-issue update, the repository remains in the current
epic even if other docs contain attractive next steps.

### Exit Criteria Must Be Mainline-Visible

Treat the current epic as unresolved while any issue required for its exit
criteria is still in one of these states:

- open as a child issue
- landed only in an open stacked PR chain
- merged into another non-`main` branch but not yet merged to `main`

For epic-promotion purposes, a stacked `Refs #...` PR is always intermediate.
It may move work between issue branches, but it does not by itself close the
linked issue, satisfy the parent epic exit criteria, or authorize switching to
another top-level epic.

Before promotion, either:

1. merge the required child work to `main`, or
2. explicitly retire, supersede, or abandon the child issue and stacked PR in
   GitHub with a short note explaining why it no longer blocks the epic

## Recommended Body Skeletons

### `program:` Meta Issue

```md
## Role

This is the only live control entry for repository execution.

## Current focus

Current epic: #
Current issue: #

## Promotion rule

Promote to the next epic only after the current epic exit criteria are met and
this issue is updated explicitly. Open stacked PRs or non-`main` merges for
required child issues still block promotion until they are merged to `main` or
explicitly retired.

## Epic queue

1. epic: ...
2. epic: ...
3. epic: ...

## Notes

- keep durable rules in repo docs
- keep live status, blockers, and handoffs in GitHub issues and PRs only
```

### `epic:` Or `sub-epic:` Issue

```md
## Goal

## Scope

## Non-goals

## Exit criteria

## Admission rule

## Ordered child queue

1. #
2. #
3. #
```

### Concrete Issue

```md
## Problem statement

## Parent

- Program: #
- Epic: #
- Sub-epic: # or none

## Scope

## Non-goals

## Deliverables
