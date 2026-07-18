---
id: ch-left-join-nulls
title: LEFT JOIN misses return defaults, not NULLs — set join_use_nulls in a pre_hook
status: enforced
scope: any model whose logic depends on NULL for unmatched LEFT JOIN rows
  (coalesce fallbacks, "is missing" flags, countIf on absence)
symptom: unmatched rows carry 0/''/epoch instead of NULL — absence checks silently
  match nothing (or everything)
last_verified: 2026-07-17
evidence:
  - ~19 models set pre_hook SET join_use_nulls = 1 with in-code rationale, e.g. models/execution/Circles/marts/fct_execution_circles_v2_inviters_ranking.sql:8,17; fct_execution_circles_v2_inviter_farm_quota.sql:8,20; int_execution_circles_v2_avatars.sql:10; int_execution_circles_v2_invite_funnel.sql:10; models/execution/gnosis_app/marts/fct_execution_gnosis_app_weekly_economically_active_users.sql:8
  - reset-to-default pair idiom where required: models/execution/pools/intermediate/int_execution_pools_swapr_v3_daily.sql:11-12 (pre/post SET join_use_nulls)
---

## Symptom
Logic keyed on "no match" misbehaves: a `WHERE joined_col IS NULL` never fires, a
`coalesce(joined_col, fallback)` never falls back, timestamps read 1970.

## Root cause
ClickHouse defaults `join_use_nulls = 0`: unmatched LEFT JOIN columns get type
defaults (0, '', epoch), not NULL.

## Forbidden action
Don't paper over it with `nullIf(col, 0)`-style workarounds — 0/''/epoch can be
legitimate values; the workaround corrupts real rows.

## Safe remediation / convention
`pre_hook = "SET join_use_nulls = 1"` on the model (the repo-wide idiom, ~19 models).
Add the paired `post_hook` reset only when the model must leave the session default
untouched for subsequent statements (the pools models' pre/post pair).

## Detection
Unmatched-row spot check: LEFT JOIN a key you know is absent and inspect the joined
columns.

## Enforcement
Convention established across the models above; called out in AGENTS.md. No static gate.
