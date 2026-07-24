---
id: model-deletion-gate-false-block
title: Deleting/renaming a model hard-blocked CI — check.py treated deleted files as "unknown to the artifact"
status: enforced
scope: >-
  scripts/agent_context/check.py (agent-context-check, full check tier only);
  any branch that deletes or renames a model .sql file
symptom: >-
  CI fails BLOCKING with "changed model(s) unknown to the agent context: <deleted
  models>" and the suggested fix (dbt parse + rebuild) cannot ever clear it — a
  deleted model is never in the manifest
last_verified: 2026-07-24
evidence:
  - CI run on commit 32d1b9e8 ("promote fct_ revenue models to incremental int_", 2026-07-24) — agent-context-check FAIL; fct_revenue_active_users_totals_weekly/_monthly deleted in that commit flagged as unknown
  - scripts/agent_context/check.py — changed_model_files() feeds `git diff --name-only` output (which lists deletions) straight into the unknown-model block
  - enforced by partition_existing() in check.py + tests/test_policy_gates.py::TestDeletedModelHandling (lands in the same commit as this lesson)
---

## Symptom
A branch that deletes or renames any model fails the full-tier `agent-context-check`
with `changed model(s) unknown to the agent context`. The message's remedy ("run
dbt parse + build_agent_context.py, then re-run") never helps: the artifact is built
from the manifest, and a deleted model is not in the manifest by definition.

## Root cause
`changed_model_files()` collects `git diff --name-only` output, which includes
**deleted** paths. The unknown-model rule was written to fail closed on a *stale
artifact* (a changed model whose hazards can't be looked up); a deletion
pattern-matches the same condition but has no hazards left to check.

## Why it reached CI, not local validation
`agent-context-check` runs only in the FULL check tier. Local validation with
`run_all.py --fast` passes cleanly on a branch that full-tier CI then rejects.
For any model add/delete/rename, validate with
`python scripts/checks/run_all.py --base-ref <CI base>` (full tier) before pushing.

## Fix
`partition_existing()` in check.py splits the changed set: deleted paths are
*reported* but never block. Deletion risk is covered by the gates that actually
see it — `dbt parse` (dangling `ref()`) and the semantic-registry gate
(orphaned metrics; see [semantic-retirement-gate](semantic-retirement-gate.md)).

## Detection
BLOCKING line names models whose `.sql` files do not exist in the working tree.
