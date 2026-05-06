{{
  config(
    materialized='incremental',
    incremental_strategy=('append' if start_month else 'delete+insert'),
    engine='ReplacingMergeTree()',
    order_by='(conversion_date, conversion_kind, identity_role)',
    unique_key='(conversion_date, conversion_kind, identity_role)',
    partition_by='toStartOfMonth(conversion_date)',
    settings={'allow_nullable_key': 1},
    tags=['production', 'mta', 'execution', 'gpay'],
    pre_hook=["SET join_algorithm = 'grace_hash'"],
    post_hook=["SET join_algorithm = 'default'"]
  )
}}
{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

-- Per-day, per-(conversion_kind, identity_role) coverage stats. Same
-- shape as the GA-side coverage_daily, with the role dimension added so
-- the persona can compare "owner-grain coverage" vs "treasury-grain
-- coverage" for the same conversion_kind.

WITH conv AS (
    SELECT *
    FROM {{ ref('int_execution_gpay_conversions') }}
    WHERE conversion_date < today()
    {% if start_month and end_month %}
      AND toStartOfMonth(conversion_date) >= toDate('{{ start_month }}')
      AND toStartOfMonth(conversion_date) <= toDate('{{ end_month }}')
    {% else %}
      {{ apply_monthly_incremental_filter('conversion_date', 'conversion_date', add_and=True) }}
    {% endif %}
),

events AS (
    SELECT user_pseudonym, identity_role, event_ts, event_kind
    FROM {{ ref('int_execution_gpay_user_events_unified') }}
    WHERE event_date >= (SELECT min(conversion_date) FROM conv) - INTERVAL 30 DAY
      AND event_date < today()
),

tracked AS (
    SELECT
        c.conversion_date,
        c.conversion_kind,
        c.identity_role,
        countDistinct(c.user_pseudonym, c.conversion_ts) AS tracked_conversions,
        uniqExact(c.user_pseudonym)                      AS tracked_users
    FROM conv c
    INNER JOIN events e
      ON  e.user_pseudonym = c.user_pseudonym
      AND e.identity_role  = c.identity_role
      AND e.event_ts       <  c.conversion_ts
      AND e.event_ts       >= c.conversion_ts - INTERVAL 30 DAY
      AND e.event_kind     != {{ conversion_kind_to_event_kind('c.conversion_kind') }}
    GROUP BY c.conversion_date, c.conversion_kind, c.identity_role
),

total AS (
    SELECT
        conversion_date,
        conversion_kind,
        identity_role,
        count()                  AS total_conversions,
        uniqExact(user_pseudonym) AS total_users
    FROM conv
    GROUP BY conversion_date, conversion_kind, identity_role
)

SELECT
    t.conversion_date,
    t.conversion_kind,
    t.identity_role,
    t.total_conversions,
    coalesce(tr.tracked_conversions, 0) AS tracked_conversions,
    t.total_users,
    coalesce(tr.tracked_users, 0)       AS tracked_users,
    coalesce(tr.tracked_conversions, 0) / nullIf(t.total_conversions, 0) AS tracked_conversion_coverage,
    coalesce(tr.tracked_users, 0)       / nullIf(t.total_users, 0)       AS tracked_user_coverage
FROM total t
LEFT JOIN tracked tr USING (conversion_date, conversion_kind, identity_role)
