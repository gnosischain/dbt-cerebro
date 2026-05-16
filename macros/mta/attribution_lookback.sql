{#
  build_attribution_lookback(lookback_days, sector)

  Generates the body of an `fct_<sector>_attribution_<N>d` mart against a
  pre-aggregated journey table (one row per conversion × event_kind, see
  build_journey_lookback). Computes first / last / linear / time-decay
  attribution credits per (conversion_kind, [identity_role,] event_kind)
  over the last 180 days of conversions. Refreshed daily.

  The 6 attribution marts (gnosis_app × {7d, 30d, 60d} and gpay × {7d,
  30d, 60d}) are now 3-line wrappers — the per-lookback math is identical;
  only the journey ref name and the optional identity_role grain differ.

  Math is exact under the journey-table pre-aggregation:

    per conversion → sum over kinds of {first, last, linear, time_decay}
                     credit = 1.0 (verified algebraically — see commit
                     introducing this macro for the derivation).

  first_touch_credit(kind) = if first_touch_ts(kind) = overall_first_ts
                               then n_at_first_touch_ts(kind) / Σ ties
                               else 0
  last_touch_credit(kind)  = symmetric
  linear_credit(kind)      = n_touches(kind) / Σ n_touches
  time_decay_credit(kind)  = td_sum(kind)    / Σ td_sum
#}
{% macro build_attribution_lookback(lookback_days, sector) %}
{% set journeys_model    = 'fct_execution_' ~ sector ~ '_journeys_' ~ lookback_days ~ 'd' %}
{% set conversions_model = 'int_execution_' ~ sector ~ '_conversions' %}

{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=(
      '(conversion_kind, identity_role, event_kind)'
      if sector == 'gpay'
      else '(conversion_kind, event_kind)'
    ),
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

WITH journeys AS (
    SELECT
        user_pseudonym,
        {%- if sector == 'gpay' %}
        identity_role,
        {%- endif %}
        conversion_kind,
        conversion_ts,
        event_kind,
        n_touches,
        first_touch_ts,
        last_touch_ts,
        td_sum
    FROM {{ ref(journeys_model) }}
    WHERE conversion_date >= today() - INTERVAL 180 DAY
),

per_conv AS (
    SELECT
        user_pseudonym,
        {%- if sector == 'gpay' %}
        identity_role,
        {%- endif %}
        conversion_kind,
        conversion_ts,
        min(first_touch_ts) AS first_ts_overall,
        max(last_touch_ts)  AS last_ts_overall,
        sum(n_touches)      AS n_touches_total,
        sum(td_sum)         AS td_sum_total,
        arrayCount(t -> t = arrayMin(groupArray(first_touch_ts)), groupArray(first_touch_ts)) AS n_kinds_at_first,
        arrayCount(t -> t = arrayMax(groupArray(last_touch_ts)),  groupArray(last_touch_ts))  AS n_kinds_at_last
    FROM journeys
    GROUP BY
        user_pseudonym,
        {%- if sector == 'gpay' %}
        identity_role,
        {%- endif %}
        conversion_kind,
        conversion_ts
),

scored AS (
    SELECT
        j.conversion_kind                                                             AS conversion_kind,
        {%- if sector == 'gpay' %}
        j.identity_role                                                               AS identity_role,
        {%- endif %}
        j.event_kind                                                                  AS event_kind,
        j.user_pseudonym                                                              AS user_pseudonym,
        j.conversion_ts                                                               AS conversion_ts,
        if(j.first_touch_ts = pc.first_ts_overall,
           1.0 / nullIf(pc.n_kinds_at_first, 0),
           0.0)                                                                       AS first_touch_credit,
        if(j.last_touch_ts  = pc.last_ts_overall,
           1.0 / nullIf(pc.n_kinds_at_last,  0),
           0.0)                                                                       AS last_touch_credit,
        toFloat64(j.n_touches) / nullIf(pc.n_touches_total, 0)                        AS linear_credit,
        j.td_sum               / nullIf(pc.td_sum_total, 0)                           AS time_decay_credit
    FROM journeys j
    INNER JOIN per_conv pc
        ON  pc.user_pseudonym  = j.user_pseudonym
        {%- if sector == 'gpay' %}
        AND pc.identity_role   = j.identity_role
        {%- endif %}
        AND pc.conversion_kind = j.conversion_kind
        AND pc.conversion_ts   = j.conversion_ts
),

totals AS (
    SELECT
        conversion_kind,
        {%- if sector == 'gpay' %}
        identity_role,
        {%- endif %}
        count() AS total_conversions
    FROM {{ ref(conversions_model) }}
    WHERE conversion_date >= today() - INTERVAL 180 DAY
    GROUP BY
        conversion_kind
        {%- if sector == 'gpay' %},
        identity_role
        {%- endif %}
)

SELECT
    s.conversion_kind                                AS conversion_kind,
    {%- if sector == 'gpay' %}
    s.identity_role                                  AS identity_role,
    {%- endif %}
    s.event_kind                                     AS event_kind,
    uniqExact(s.user_pseudonym, s.conversion_ts)     AS conversions_with_touch,
    sum(first_touch_credit)                          AS first_touch,
    sum(last_touch_credit)                           AS last_touch,
    sum(linear_credit)                               AS linear,
    sum(time_decay_credit)                           AS time_decay_hl_7d,
    any(t.total_conversions)                         AS total_conversions,
    now()                                            AS computed_at
FROM scored s
LEFT JOIN totals t
    ON  t.conversion_kind = s.conversion_kind
    {%- if sector == 'gpay' %}
    AND t.identity_role   = s.identity_role
    {%- endif %}
GROUP BY
    s.conversion_kind,
    {%- if sector == 'gpay' %}
    s.identity_role,
    {%- endif %}
    s.event_kind
{% endmacro %}
