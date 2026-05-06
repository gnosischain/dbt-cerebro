{{
  config(
    materialized='incremental',
    incremental_strategy=('append' if start_month else 'delete+insert'),
    engine='ReplacingMergeTree()',
    order_by='(date, funnel_name, user_pseudonym)',
    unique_key='(date, funnel_name, user_pseudonym)',
    partition_by='toStartOfMonth(date)',
    settings={'allow_nullable_key': 1},
    tags=['production', 'mta', 'execution', 'gnosis_app', 'mart'],
    pre_hook=["SET join_algorithm = 'grace_hash'"],
    post_hook=["SET join_algorithm = 'default'"]
  )
}}
{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

-- Daily funnel diagnostics over the unified events table. Uses ClickHouse
-- windowFunnel — the level returned per (user_pseudonym, funnel_name) is
-- the maximum step reached within window_seconds of the first event.
--
-- Funnels live in seed mta_funnels.csv. Add a row there to get a new
-- funnel; the model picks it up automatically via the cross join.
--
-- Two-step funnels (step_3_event_kind IS NULL) are supported by passing
-- the same step twice as a no-op extra condition.

WITH events AS (
  SELECT
    user_pseudonym,
    event_date,
    event_ts,
    event_kind
  FROM {{ ref('int_execution_gnosis_app_user_events_unified') }}
  WHERE event_date < today()
  {% if start_month and end_month %}
    AND toStartOfMonth(event_date) >= toDate('{{ start_month }}')
    AND toStartOfMonth(event_date) <= toDate('{{ end_month }}')
  {% else %}
    {{ apply_monthly_incremental_filter('event_date', 'date', add_and=True) }}
  {% endif %}
),

funnels AS (
  SELECT
    funnel_name,
    step_1_event_kind,
    step_2_event_kind,
    step_3_event_kind,
    window_seconds
  FROM {{ ref('mta_funnels') }}
)

SELECT
  e.event_date                                                     AS date,
  f.funnel_name                                                    AS funnel_name,
  e.user_pseudonym                                                 AS user_pseudonym,
  windowFunnel(f.window_seconds)(
    toUInt32(toUnixTimestamp(e.event_ts)),
    e.event_kind = f.step_1_event_kind,
    e.event_kind = f.step_2_event_kind,
    -- step_3 is optional; for two-step funnels we still pass a condition,
    -- but a NULL step_3 means windowFunnel sees `event_kind = NULL` which
    -- never matches → max level capped at 2 (correct semantics for 2-step).
    e.event_kind = f.step_3_event_kind
  )                                                                AS level,
  min(e.event_ts)                                                  AS first_event_ts,
  max(e.event_ts)                                                  AS last_event_ts
FROM events e
CROSS JOIN funnels f
WHERE e.event_kind IN (f.step_1_event_kind, f.step_2_event_kind, f.step_3_event_kind)
GROUP BY e.event_date, f.funnel_name, e.user_pseudonym
