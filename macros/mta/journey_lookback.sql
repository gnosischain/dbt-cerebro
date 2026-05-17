{#
  build_journey_lookback(lookback_days, sector)

  Generates the body of an `fct_<sector>_journeys_<N>d` mart from a single
  template. Pre-aggregates touchpoints at the (conversion × event_kind)
  grain so each conversion produces at most `distinct event_kinds touched`
  rows (typically ≤15), not "all events in the lookback window" (which
  for high-activity users like gpay treasury can be thousands).

  Each variant model is a 3-line wrapper, e.g.:

    -- models/execution/gpay/marts/fct_execution_gpay_journeys_30d.sql
    {{ build_journey_lookback(30, 'gpay') }}

  Output columns (per conversion × event_kind):

    user_pseudonym, [identity_role,]
    conversion_kind, conversion_ts, conversion_date,
    conversion_amount_usd, conversion_token,
    event_source, event_kind,
    n_touches               — count of touchpoints of this kind in the window
    first_touch_ts          — min touch_ts within this kind
    last_touch_ts           — max touch_ts within this kind
    td_sum                  — Σ exp(-lag_days / 7) over touchpoints of this kind
                              (7-day half-life hardcoded; matches downstream)

  Downstream attribution math (ties resolved at the event_kind level,
  which is the standard MTA semantic — multiple kinds touching at the
  exact same ts split credit equally):
    linear_credit(kind)      = n_touches(kind)        / Σ n_touches
    time_decay_credit(kind)  = td_sum(kind)           / Σ td_sum
    first_touch_credit(kind) = if first_touch_ts(kind) = overall_first_ts
                                 then 1 / (#kinds tied at first_ts)
                                 else 0
    last_touch_credit(kind)  = symmetric to first_touch_credit
#}
{% macro build_journey_lookback(lookback_days, sector) %}
{% set conversion_model = 'int_execution_' ~ sector ~ '_conversions' %}
{% set events_model = 'int_execution_' ~ sector ~ '_user_events_unified' %}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

{{ config(
    materialized='incremental',
    incremental_strategy=('append' if start_month else 'delete+insert'),
    engine='ReplacingMergeTree()',
    order_by=(
      '(conversion_date, conversion_kind, identity_role, user_pseudonym, conversion_ts, event_kind)'
      if sector == 'gpay'
      else '(conversion_date, conversion_kind, user_pseudonym, conversion_ts, event_kind)'
    ),
    unique_key=(
      '(conversion_ts, conversion_kind, user_pseudonym, identity_role, event_kind)'
      if sector == 'gpay'
      else '(conversion_ts, conversion_kind, user_pseudonym, event_kind)'
    ),
    partition_by='toStartOfMonth(conversion_date)',
    settings={'allow_nullable_key': 1},
    tags=['production', 'mta', 'execution', sector, 'mart'],
    pre_hook=[
        "SET join_algorithm = 'grace_hash'",
        "SET grace_hash_join_initial_buckets = 64"
    ],
    post_hook=[
        "SET join_algorithm = 'default'"
    ]
) }}

WITH conversions AS (
    SELECT *
    FROM {{ ref(conversion_model) }}
    WHERE 1=1
    {%- if sector == 'gpay' %}
      -- Exclude the delegate identity_role: it represents the shared
      -- gpay delegate wallet (system infrastructure), not a per-user
      -- journey participant. Every gpay user routes through it, so its
      -- 30d event pool averages ~14k touches per (conversion × event_kind)
      -- and dominates the join cost (~99.8% of intermediate rows in
      -- our May-2025 sample). Owner + safe_self preserve the real
      -- end-user journeys.
      AND identity_role != 'delegate'
    {%- endif %}
    {%- if start_month and end_month %}
      AND toStartOfMonth(conversion_date) >= toDate('{{ start_month }}')
      AND toStartOfMonth(conversion_date) <= toDate('{{ end_month }}')
    {%- else %}
      {{ apply_monthly_incremental_filter('conversion_date', 'conversion_date', add_and=True) }}
    {%- endif %}
),
active_users AS (
    SELECT DISTINCT
        user_pseudonym
        {%- if sector == 'gpay' %},
        identity_role
        {%- endif %}
    FROM conversions
),
events_window AS (
    SELECT e.*
    FROM {{ ref(events_model) }} e
    INNER JOIN active_users u
        ON e.user_pseudonym = u.user_pseudonym
        {%- if sector == 'gpay' %}
        AND e.identity_role = u.identity_role
        {%- endif %}
    WHERE 1=1
    {%- if start_month and end_month %}
      AND e.event_date >= addDays(toDate('{{ start_month }}'), -{{ lookback_days }})
      AND e.event_date <= toLastDayOfMonth(toDate('{{ end_month }}'))
    {%- else %}
      {{ apply_monthly_incremental_filter(
            'e.event_date',
            'conversion_date',
            add_and=True,
            lookback_days=lookback_days) }}
    {%- endif %}
),
joined AS (
    SELECT
        c.user_pseudonym                                              AS user_pseudonym,
        {%- if sector == 'gpay' %}
        c.identity_role                                               AS identity_role,
        {%- endif %}
        c.conversion_kind                                             AS conversion_kind,
        c.conversion_ts                                               AS conversion_ts,
        c.conversion_date                                             AS conversion_date,
        c.conversion_amount_usd                                       AS conversion_amount_usd,
        c.conversion_token                                            AS conversion_token,
        e.event_source                                                AS event_source,
        e.event_kind                                                  AS event_kind,
        e.event_ts                                                    AS touch_ts,
        dateDiff('day', toDate(e.event_ts), c.conversion_date)        AS lag_days
    FROM conversions c
    INNER JOIN events_window e
        ON  e.user_pseudonym = c.user_pseudonym
        {%- if sector == 'gpay' %}
        AND e.identity_role  = c.identity_role
        {%- endif %}
        AND e.event_ts       <  c.conversion_ts
        AND e.event_ts       >= c.conversion_ts - INTERVAL {{ lookback_days }} DAY
        AND e.event_kind     != {{ conversion_kind_to_event_kind('c.conversion_kind') }}
)
SELECT
    user_pseudonym,
    {%- if sector == 'gpay' %}
    identity_role,
    {%- endif %}
    conversion_kind,
    conversion_ts,
    conversion_date,
    any(conversion_amount_usd)                          AS conversion_amount_usd,
    any(conversion_token)                               AS conversion_token,
    any(event_source)                                   AS event_source,
    event_kind,
    count()                                             AS n_touches,
    min(touch_ts)                                       AS first_touch_ts,
    max(touch_ts)                                       AS last_touch_ts,
    sum(exp(-1.0 * lag_days / 7.0))                     AS td_sum
FROM joined
GROUP BY
    user_pseudonym,
    {%- if sector == 'gpay' %}
    identity_role,
    {%- endif %}
    conversion_kind,
    conversion_ts,
    conversion_date,
    event_kind
{% endmacro %}
