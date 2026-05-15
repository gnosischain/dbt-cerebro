

-- GP attribution mart, 60-day lookback variant. Same pre-aggregate-then-JOIN
-- shape as the 30d mart. The time-decay half-life remains 7d (a per-customer
-- decay parameter, not a window parameter), so longer-lookback touches
-- contribute exponentially less credit but still appear in first/last/linear.

WITH journeys AS (
  SELECT
    user_pseudonym,
    identity_role,
    conversion_kind,
    conversion_ts,
    touch_ts,
    event_kind,
    event_dedup_key,
    exp(-1.0 * dateDiff('day', toDate(touch_ts), toDate(conversion_ts)) / 7.0) AS td_raw
  FROM `dbt`.`fct_execution_gpay_journeys_60d`
  WHERE conversion_date >= today() - INTERVAL 180 DAY
),

per_conv AS (
  SELECT
    user_pseudonym,
    identity_role,
    conversion_kind,
    conversion_ts,
    min(touch_ts) AS first_ts,
    max(touch_ts) AS last_ts,
    count()       AS n_touches,
    sum(td_raw)   AS td_sum
  FROM journeys
  GROUP BY user_pseudonym, identity_role, conversion_kind, conversion_ts
),

tie_counts AS (
  SELECT
    j.user_pseudonym,
    j.identity_role,
    j.conversion_kind,
    j.conversion_ts,
    countIf(j.touch_ts = pc.first_ts) AS n_first_ties,
    countIf(j.touch_ts = pc.last_ts)  AS n_last_ties
  FROM journeys j
  INNER JOIN per_conv pc
    ON  pc.user_pseudonym  = j.user_pseudonym
    AND pc.identity_role   = j.identity_role
    AND pc.conversion_kind = j.conversion_kind
    AND pc.conversion_ts   = j.conversion_ts
  GROUP BY j.user_pseudonym, j.identity_role, j.conversion_kind, j.conversion_ts
),

scored AS (
  SELECT
    j.user_pseudonym                                                    AS user_pseudonym,
    j.identity_role                                                     AS identity_role,
    j.conversion_kind                                                   AS conversion_kind,
    j.conversion_ts                                                     AS conversion_ts,
    j.event_kind                                                        AS event_kind,
    if(j.touch_ts = pc.first_ts, 1.0 / nullIf(tc.n_first_ties, 0), 0.0) AS first_touch_credit,
    if(j.touch_ts = pc.last_ts,  1.0 / nullIf(tc.n_last_ties,  0), 0.0) AS last_touch_credit,
    1.0 / pc.n_touches                                                  AS linear_credit,
    j.td_raw / nullIf(pc.td_sum, 0)                                     AS time_decay_credit
  FROM journeys j
  INNER JOIN per_conv pc
    ON  pc.user_pseudonym  = j.user_pseudonym
    AND pc.identity_role   = j.identity_role
    AND pc.conversion_kind = j.conversion_kind
    AND pc.conversion_ts   = j.conversion_ts
  INNER JOIN tie_counts tc
    ON  tc.user_pseudonym  = j.user_pseudonym
    AND tc.identity_role   = j.identity_role
    AND tc.conversion_kind = j.conversion_kind
    AND tc.conversion_ts   = j.conversion_ts
),

totals AS (
  SELECT
    conversion_kind,
    identity_role,
    count() AS total_conversions
  FROM `dbt`.`int_execution_gpay_conversions`
  WHERE conversion_date >= today() - INTERVAL 180 DAY
  GROUP BY conversion_kind, identity_role
)

SELECT
  s.conversion_kind                                AS conversion_kind,
  s.identity_role                                  AS identity_role,
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
  AND t.identity_role   = s.identity_role
GROUP BY s.conversion_kind, s.identity_role, s.event_kind