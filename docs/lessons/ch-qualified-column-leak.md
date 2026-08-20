---
id: ch-qualified-column-leak
title: An unaliased ambiguous column ships with its qualifier IN the column name
status: remediated
scope: any SELECT over two or more joined relations that projects a column whose name
  exists in more than one of them without an explicit alias — marts and intermediates
  built from multiple CTEs are the exposure
symptom: the built relation has a column literally named `a.total_vp` / `s.phase` /
  `po.topic_id`; dbt reports the model green, and every consumer querying the intended
  name fails with Code 47 UNKNOWN_IDENTIFIER on a column that is plainly there in the SQL
last_verified: 2026-07-30
evidence:
  - models/governance/marts/api_governance_proposal_direction.sql — `a.total_vp` shipped
    (total_vp exists on both int_governance_proposals and the agg CTE); now aliased
  - models/governance/intermediate/int_governance_forum_post_phases.sql — `po.topic_id`
    shipped (topic_id also on int_governance_proposal_topic_links); now aliased
  - models/governance/marts/api_governance_discussion_phases.sql — `s.phase` and
    `s.proposal_id` shipped (both names present in all three CTEs); now aliased
  - models/governance/marts/api_governance_poll_vs_vote.sql — `d.proposal_id` and
    `p.topic_id`; the first surfaced as a FAILING not_null test, the rest were silent
  - four occurrences in one session, three of them silent; two survived a build that
    reported PASS=117 WARN=0 ERROR=0 and were only found by the sweep below
---

## Symptom
A model builds clean and its column is visibly named `total_vp` in the SQL, but

```sql
SELECT total_vp FROM playground_max.api_governance_proposal_direction
```

fails with `Code: 47 ... Unknown expression identifier 'total_vp'`, while

```sql
SELECT "a.total_vp" FROM playground_max.api_governance_proposal_direction
```

works. The qualifier is part of the column name in the warehouse.

## Root cause
ClickHouse resolves an ambiguous projected column by keeping its qualifier as the output
name. `SELECT a.total_vp` where `total_vp` exists on both joined relations yields a column
named `a.total_vp`, not `total_vp`. Unambiguous columns resolve normally, which is why this
hits one or two columns in a model and leaves the rest looking fine.

Sibling of [ch-alias-shadows-where](ch-alias-shadows-where.md): both are ClickHouse name
resolution behaving unlike other engines. There the alias wins where you wanted the column;
here the qualifier leaks where you wanted the bare name.

## Forbidden action
Don't project a column from a multi-relation join without an explicit alias when that name
exists on more than one side. Don't trust a green `dbt build` as evidence the column names
are right — nothing in the build checks them.

## Safe remediation / convention
Alias explicitly: `a.total_vp AS total_vp`. Do it for every projected column whose name
appears in more than one joined relation, not just the ones you expect to be ambiguous —
adding a CTE later can make a previously-unique name ambiguous without touching the SELECT.

## Detection
Manual, after writing or editing any model with joins:

```sql
SELECT table, name FROM system.columns
WHERE database = currentDatabase() AND name LIKE '%.%'
```

Empty is correct. A `not_null`/`unique` test on the affected column also catches it (as
Code 47 on the test, not a row failure) — but only for columns that happen to be tested,
which in practice means keys. Non-key columns on a mart have no automatic detector.

## Enforcement
None — informational, and deliberately so. A catalog-based CI gate was considered and
rejected: `target/catalog.json` carries zero model nodes on this project (see
[docs-catalog-zero-nodes](docs-catalog-zero-nodes.md)), column names live only in the
catalog, and that record explicitly forbids building tooling that assumes model-level
catalog entries. The semantic pipeline is manifest-only for the same reason. A gate would
therefore have to query the warehouse live, which no existing check does.

Note prose alone is a weak safeguard here: all four occurrences above happened in one
session, three of them *after* the first had been diagnosed and commented in-code. Run the
sweep; do not rely on remembering.
