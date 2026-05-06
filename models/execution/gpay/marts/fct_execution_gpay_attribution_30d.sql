{{
  config(
    materialized='table',
    engine='MergeTree()',
    order_by='(conversion_kind, identity_role, event_kind)',
    settings={'allow_nullable_key': 1},
    tags=['production', 'mta', 'execution', 'gpay', 'mart']
  )
}}

-- Pre-computed attribution per (conversion_kind, identity_role,
-- event_kind). Same shape as the GA-side attribution mart, with
-- identity_role added so persona queries pick grain at runtime:
--   WHERE conversion_kind='gpay_payment' AND identity_role='initial_owner'
--     → owner-grain attribution
--   WHERE conversion_kind='gpay_payment' AND identity_role='safe_self'
--     → treasury-grain attribution

WITH paths AS (
  SELECT
    user_pseudonym,
    identity_role,
    conversion_kind,
    conversion_ts,
    arraySort(
      x -> (x.1, x.4),
      groupArray((touch_ts, event_kind, event_source, event_dedup_key))
    ) AS touches
  FROM {{ ref('fct_execution_gpay_journeys_30d') }}
  WHERE conversion_date >= today() - INTERVAL 180 DAY
  GROUP BY user_pseudonym, identity_role, conversion_kind, conversion_ts
),

n_paths AS (
  SELECT
    user_pseudonym,
    identity_role,
    conversion_kind,
    conversion_ts,
    touches,
    length(touches) AS n_touches
  FROM paths
  WHERE length(touches) > 0
),

exploded AS (
  SELECT
    user_pseudonym,
    identity_role,
    conversion_kind,
    conversion_ts,
    n_touches,
    touch.2                                                             AS event_kind,
    if(touch = touches[1],         1.0, 0.0)                            AS first_touch_credit,
    if(touch = touches[n_touches], 1.0, 0.0)                            AS last_touch_credit,
    1.0 / n_touches                                                     AS linear_credit,
    exp(-1.0 * dateDiff('day', toDate(touch.1), toDate(conversion_ts)) / 7.0)
                                                                        AS td_raw
  FROM n_paths
  ARRAY JOIN touches AS touch
),

td_normalized AS (
  SELECT
    user_pseudonym,
    identity_role,
    conversion_kind,
    conversion_ts,
    event_kind,
    first_touch_credit,
    last_touch_credit,
    linear_credit,
    td_raw / sum(td_raw) OVER (PARTITION BY user_pseudonym, identity_role, conversion_kind, conversion_ts)
      AS time_decay_credit
  FROM exploded
),

totals AS (
  SELECT
    conversion_kind,
    identity_role,
    count() AS total_conversions
  FROM {{ ref('int_execution_gpay_conversions') }}
  WHERE conversion_date >= today() - INTERVAL 180 DAY
  GROUP BY conversion_kind, identity_role
)

SELECT
  td.conversion_kind                               AS conversion_kind,
  td.identity_role                                 AS identity_role,
  td.event_kind                                    AS event_kind,
  uniqExact(td.user_pseudonym, td.conversion_ts)   AS conversions_with_touch,
  sum(first_touch_credit)                          AS first_touch,
  sum(last_touch_credit)                           AS last_touch,
  sum(linear_credit)                               AS linear,
  sum(time_decay_credit)                           AS time_decay_hl_7d,
  any(t.total_conversions)                         AS total_conversions,
  now()                                            AS computed_at
FROM td_normalized td
LEFT JOIN totals t USING (conversion_kind, identity_role)
GROUP BY td.conversion_kind, td.identity_role, td.event_kind
