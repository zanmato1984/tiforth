# Documentation Updates

`tiforth` is docs-first. Implementation work should leave a documentation trail instead of relying on chat history or reviewer memory.

Live execution tracking should stay in GitHub issues and PRs. Repo docs should capture durable knowledge, not running status logs.

## When A PR Must Update Docs

Update docs when a PR changes any of these in a meaningful way:

- operator or function semantics
- data, runtime, or admission boundaries
- top-level repository shape or crate boundaries
- adapter-facing contracts
- conformance expectations, fixtures, or documented cases
- project workflow rules that contributors or agents are expected to follow

## Where Updates Belong

### Semantics

- `docs/spec/`

### Data / Runtime / Admission Boundaries

- `docs/contracts/`
- `docs/design/`

### Top-Level Structure And Crate Boundaries

- `docs/architecture.md`
- `README.md` when the top-level repository shape changes materially

### Accepted Decisions

- `docs/decisions/`

### Conformance Expectations And Fixtures

- `tests/conformance/`

### Project Workflow Rules

- `docs/process/`
- `AGENTS.md`
- `.github/pull_request_template.md` when author workflow changes

## PR Requirement

Every PR must do one of these:

1. update the relevant docs
2. explicitly say why no docs update is needed

Use this line in the PR body when the second case applies:

```md
Docs-Impact: none - <reason>
```

If docs were updated, say so in the PR body as well, for example:

```md
Docs-Impact: updated
```

## Automation

The repository may use a lightweight CI check as a backstop.

That check is intentionally heuristic. It is expected to catch silent doc drift for obvious implementation or workflow changes, not to replace reviewer judgment.

If the check fires incorrectly, keep the PR explicit by either:

- updating the relevant docs, or
- adding a clear `Docs-Impact: none - ...` note to the PR body
