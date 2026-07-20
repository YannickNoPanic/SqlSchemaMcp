# Title

id: short-kebab-id
type: spec
status: active | completed | archived | superseded
created: YYYY-MM-DD
summary: One sentence describing what behavior or design this spec locks in.

<!-- Optional: owner, expires, superseded_by -->

A spec is an approved design or behavior contract — the "what and why" that
a plan later turns into tasks. Keep it implementation-detail-light; step-by-step
work belongs in a plan (see `templates/plan.md`), not here.

## Problem

What need or gap does this address? Link the decision or idea that triggered it, if any.

## Requirements

Numbered, testable statements. Each one should be checkable as done/not-done
by a future implementer or reviewer.

1. ...
2. ...

## Design

The approach: interfaces, data shapes, boundaries, sequencing — whatever a
plan's author needs to derive tasks from this without re-deriving the design.

## Out Of Scope

What this spec deliberately does not cover, so a future reader does not
assume it was forgotten.

## Acceptance Criteria

How do we know an implementation satisfies this spec?

## Lifecycle

Lives in `docs/workflow/specs/active/` while the design is current but not
(fully) implemented.

- **Completed:** implemented and verified against Acceptance Criteria. Move
  to `docs/workflow/specs/completed/` and set `status: completed`.
- **Archived/Superseded:** the design changed or was dropped before or after
  implementation. Set `status: archived` or `superseded` (with
  `superseded_by`) and move to `docs/workflow/specs/archived/`.
