# [Feature Name] Implementation Plan

id: short-kebab-id
type: plan
status: active | completed | archived | superseded
created: YYYY-MM-DD
summary: One sentence describing what this plan builds and why it exists.

<!-- Optional: owner, expires, superseded_by -->

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** [One sentence describing what this builds]

**Architecture:** [2-3 sentences about approach]

**Tech Stack:** [Key technologies/libraries]

## Global Constraints

[Project-wide requirements that apply to every task below: version floors,
dependency limits, naming/copy rules, platform requirements. One line each,
copied verbatim from the spec or decision that set them.]

---

## Task 1: [Component Name]

**Files:**
- Create: `exact/path/to/file.ext`
- Modify: `exact/path/to/existing.ext:123-145`
- Test: `tests/exact/path/to/test.ext`

**Interfaces:**
- Consumes: [what this task uses from earlier tasks — exact signatures]
- Produces: [what later tasks rely on — exact names, parameter and return types]

- [ ] **Step 1: Write the failing test**
- [ ] **Step 2: Run test to verify it fails**
- [ ] **Step 3: Write minimal implementation**
- [ ] **Step 4: Run test to verify it passes**
- [ ] **Step 5: Commit**

<!-- Repeat Task N for each remaining unit of work. Every step needs real
     content an engineer can execute — no "TBD", no "similar to Task N". -->

## Lifecycle

This plan lives in `docs/workflow/plans/active/` while work is in progress.

- **Completed:** all tasks checked and verified. Move the file to
  `docs/workflow/plans/completed/`, set `status: completed`, and update the
  memory index. If only the decision matters going forward (not the
  step-by-step history), summarize it into a `docs/decisions/` entry instead
  and archive this file.
- **Archived:** the plan was abandoned or replaced before completion. Set
  `status: archived` (or `superseded` with `superseded_by`) and move it to
  `docs/workflow/plans/archived/`.
- **From superpowers:** plans drafted by `superpowers:writing-plans` start
  under `docs/superpowers/plans/`. Promote a plan into
  `docs/workflow/plans/active/` — adding the metadata block above — once the
  team accepts it as the current plan of record. Treat anything still under
  `docs/superpowers/` as draft/tool output, not committed workflow memory.
