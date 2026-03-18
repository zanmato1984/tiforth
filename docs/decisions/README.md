# Decisions

Architectural decisions that need to outlive issue threads should be recorded here.

## Purpose

This directory exists so tiforth can move important conclusions out of chat and issue comments and into versioned repo documents.

Decision records are for durable architectural conclusions, not for day-to-day execution tracking. Live project status belongs in GitHub issues and PRs.

## Format

Use numbered decision records, for example:

- `0001-kernel-language.md`
- `0002-runtime-boundary.md`

Each decision record should capture:

- context
- decision
- alternatives considered
- consequences
- follow-up work

## Current Records

- `0001-kernel-language.md`: accepted decision for shared-kernel language and runtime substrate.

## Adding The Next Record

When a new architectural conclusion needs a durable record, use the next
sequential identifier (for example, `0002-...`).
