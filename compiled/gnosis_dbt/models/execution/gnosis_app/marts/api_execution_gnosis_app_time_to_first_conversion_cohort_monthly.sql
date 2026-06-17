

-- Cohort by month-of-onboard × conversion_kind. Reports median + p25/p75
-- days from onboard to the user's first event of each conversion kind.
-- Long-format output (one row per (cohort_month, conversion_kind)) so the
-- dashboard can render a per-conversion-kind chart with a series toggle.

WITH long_form AS (
    SELECT
        toStartOfMonth(first_seen_at)                                        AS cohort_month,
        'topup'              AS conversion_kind,
        first_seen_at,
        first_topup_at                                                       AS first_conversion_at
    FROM `dbt`.`int_execution_gnosis_app_first_conversion`
    UNION ALL
    SELECT
        toStartOfMonth(first_seen_at)                                        AS cohort_month,
        'swap_filled'        AS conversion_kind,
        first_seen_at,
        first_swap_filled_at                                                 AS first_conversion_at
    FROM `dbt`.`int_execution_gnosis_app_first_conversion`
    UNION ALL
    SELECT
        toStartOfMonth(first_seen_at)                                        AS cohort_month,
        'marketplace_buy'    AS conversion_kind,
        first_seen_at,
        first_marketplace_buy_at                                             AS first_conversion_at
    FROM `dbt`.`int_execution_gnosis_app_first_conversion`
    UNION ALL
    SELECT
        toStartOfMonth(first_seen_at)                                        AS cohort_month,
        'token_offer_claim'  AS conversion_kind,
        first_seen_at,
        first_token_offer_claim_at                                           AS first_conversion_at
    FROM `dbt`.`int_execution_gnosis_app_first_conversion`
)

SELECT
    cohort_month                                                              AS cohort_month,
    conversion_kind                                                           AS conversion_kind,
    count()                                                                   AS n_in_cohort,
    countIf(first_conversion_at IS NOT NULL)                                  AS n_converted,
    round(countIf(first_conversion_at IS NOT NULL) / count() * 100, 1)        AS pct_converted,
    -- greatest(..., 0): the onboard date is heuristic-derived and can land
    -- after a user's real first action, yielding a negative day-count for a
    -- few edge users; floor at 0 so the latency percentiles stay non-negative.
    quantileExactIf(0.5)(greatest(dateDiff('day', first_seen_at, first_conversion_at), 0),
                          first_conversion_at IS NOT NULL)                    AS median_days,
    quantileExactIf(0.25)(greatest(dateDiff('day', first_seen_at, first_conversion_at), 0),
                           first_conversion_at IS NOT NULL)                   AS p25_days,
    quantileExactIf(0.75)(greatest(dateDiff('day', first_seen_at, first_conversion_at), 0),
                           first_conversion_at IS NOT NULL)                   AS p75_days
FROM long_form
WHERE cohort_month < toStartOfMonth(today())
GROUP BY cohort_month, conversion_kind
ORDER BY cohort_month DESC, conversion_kind