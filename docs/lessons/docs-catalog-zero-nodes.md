---
id: docs-catalog-zero-nodes
title: dbt docs generate writes a catalog with 0 model nodes here — expected, don't chase it
status: observed
scope: dbt docs generate / target/catalog.json on this project (dbt-clickhouse)
symptom: catalog.json contains only sources; every MODEL node count is zero
last_verified: 2026-07-17
evidence:
  - target/catalog.json (2026-07-16 build) — nodes = 0, sources = 92
  - requirements.txt:3 — dbt-clickhouse==1.9.1
  - note: the rationale (adapter get_catalog emits sources only) is known adapter behavior, not stated in a repo artifact — hence status observed
---

## Symptom
`dbt docs generate` succeeds but `catalog.json` has zero model entries; docs pages and
catalog-driven tooling show sources only.

## Root cause
The dbt-clickhouse adapter's catalog generation emits source relations only on this
setup. It is a known adapter behavior, not a project misconfiguration.

## Forbidden action
Don't burn time "fixing" the empty model catalog, and don't build tooling that assumes
model-level catalog entries exist — the semantic pipeline runs manifest-only for this
reason, and any committed overlay must match that assumption.

## Related gotcha
New decode models make `dbt docs generate` **crash at compile time** until they've been
built once (`--full-refresh` seed first, then docs).

## Detection
`python -c "import json; c=json.load(open('target/catalog.json')); print(len(c['nodes']), len(c['sources']))"`

## Enforcement
None — informational. Encoded here and in AGENTS.md verification caveats.
