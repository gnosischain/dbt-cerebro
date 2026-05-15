

-- Pre-computed first/last/linear/time-decay attribution per
-- (conversion_kind, event_kind) over the last 180 days of conversions.
-- Refreshed daily.
--
-- Implementation history:
--   v1: groupArray + arraySort + ARRAY JOIN. OOM at 10 GiB on the 12M-row
--       journey set (~570 touches/conversion average).
--   v2: window functions partitioned by (user, conversion). Still OOM
--       because each PARTITION BY clause forces a 12M-row sort.
--   v3 (this): pre-aggregate per conversion (22k groups), then JOIN
--       back to flag first/last touches. Sort space drops from 12M to
--       ~22k. Time-decay credits computed in a second pass against the
--       per-conversion sum.
--
-- Pattern: pure GROUP BY + LEFT JOIN, no window functions, no array
-- materialisation. Scales linearly in journey rows.

WITH journeys AS (
  SELECT
    user_pseudonym,
    conversion_kind,
    conversion_ts,
    touch_ts,
    event_kind,
    event_dedup_key,
    exp(-1.0 * dateDiff('day', toDate(touch_ts), toDate(conversion_ts)) / 7.0) AS td_raw
  FROM `dbt`.`fct_execution_gnosis_app_journeys_60d`
  WHERE conversion_date >= today() - INTERVAL 180 DAY
),

-- Per-conversion aggregates: first touch ts, last touch ts, count of
-- touches, sum of time-decay weights. Only ~22k rows.
per_conv AS (
  SELECT
    user_pseudonym,
    conversion_kind,
    conversion_ts,
    min(touch_ts)            AS first_ts,
    max(touch_ts)            AS last_ts,
    count()                  AS n_touches,
    sum(td_raw)              AS td_sum
  FROM journeys
  GROUP BY user_pseudonym, conversion_kind, conversion_ts
),

-- Per-conversion tie counts: when two touches share the same first_ts
-- or last_ts, we split the credit so sum(first_touch_credit) per
-- conversion stays at exactly 1.0. Computed once, joined back in.
tie_counts AS (
  SELECT
    j.user_pseudonym,
    j.conversion_kind,
    j.conversion_ts,
    countIf(j.touch_ts = pc.first_ts) AS n_first_ties,
    countIf(j.touch_ts = pc.last_ts)  AS n_last_ties
  FROM journeys j
  INNER JOIN per_conv pc
    ON  pc.user_pseudonym  = j.user_pseudonym
    AND pc.conversion_kind = j.conversion_kind
    AND pc.conversion_ts   = j.conversion_ts
  GROUP BY j.user_pseudonym, j.conversion_kind, j.conversion_ts
),

-- Score each (user, conversion, touch). first/last credit splits across
-- ties so per-conversion sums = 1.0. No window functions = no large
-- partition-key sort.
scored AS (
  SELECT
    j.user_pseudonym                                                    AS user_pseudonym,
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
    AND pc.conversion_kind = j.conversion_kind
    AND pc.conversion_ts   = j.conversion_ts
  INNER JOIN tie_counts tc
    ON  tc.user_pseudonym  = j.user_pseudonym
    AND tc.conversion_kind = j.conversion_kind
    AND tc.conversion_ts   = j.conversion_ts
),

totals AS (
  SELECT
    conversion_kind,
    count() AS total_conversions
  FROM `dbt`.`int_execution_gnosis_app_conversions`
  WHERE conversion_date >= today() - INTERVAL 180 DAY
  GROUP BY conversion_kind
)

SELECT
  s.conversion_kind                                AS conversion_kind,
  s.event_kind                                     AS event_kind,
  uniqExact(s.user_pseudonym, s.conversion_ts)     AS conversions_with_touch,
  sum(first_touch_credit)                          AS first_touch,
  sum(last_touch_credit)                           AS last_touch,
  sum(linear_credit)                               AS linear,
  sum(time_decay_credit)                           AS time_decay_hl_7d,
  any(t.total_conversions)                         AS total_conversions,
  now()                                            AS computed_at
FROM scored s
LEFT JOIN totals t ON t.conversion_kind = s.conversion_kind
GROUP BY s.conversion_kind, s.event_kind