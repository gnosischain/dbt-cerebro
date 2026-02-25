{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(week, unit)',
    tags=['production','execution','gpay']
  )
}}

WITH per_wallet_weekly AS (
    SELECT
        toStartOfWeek(date, 1) AS week,
        wallet_address,
        sum(amount)            AS total_gno,
        sum(amount_usd)        AS total_usd
    FROM {{ ref('int_execution_gpay_cashback_daily') }}
    WHERE toStartOfWeek(date, 1) < toStartOfWeek(today(), 1)
    GROUP BY week, wallet_address
),

quantiles_gno AS (
    SELECT
        week,
        'native' AS unit,
        q[1] AS q05, q[2] AS q10, q[3] AS q25, q[4] AS q50,
        q[5] AS q75, q[6] AS q90, q[7] AS q95,
        avg_val AS average
    FROM (
        SELECT
            week,
            quantilesTDigest(0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95)(total_gno) AS q,
            avg(total_gno) AS avg_val
        FROM per_wallet_weekly
        GROUP BY week
    )
),

quantiles_usd AS (
    SELECT
        week,
        'usd' AS unit,
        q[1] AS q05, q[2] AS q10, q[3] AS q25, q[4] AS q50,
        q[5] AS q75, q[6] AS q90, q[7] AS q95,
        avg_val AS average
    FROM (
        SELECT
            week,
            quantilesTDigest(0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95)(total_usd) AS q,
            avg(total_usd) AS avg_val
        FROM per_wallet_weekly
        GROUP BY week
    )
)

SELECT week, unit,
    round(toFloat64(q05), 6) AS q05,
    round(toFloat64(q10), 6) AS q10,
    round(toFloat64(q25), 6) AS q25,
    round(toFloat64(q50), 6) AS q50,
    round(toFloat64(q75), 6) AS q75,
    round(toFloat64(q90), 6) AS q90,
    round(toFloat64(q95), 6) AS q95,
    round(toFloat64(average), 6) AS average
FROM quantiles_gno

UNION ALL

SELECT week, unit,
    round(toFloat64(q05), 2) AS q05,
    round(toFloat64(q10), 2) AS q10,
    round(toFloat64(q25), 2) AS q25,
    round(toFloat64(q50), 2) AS q50,
    round(toFloat64(q75), 2) AS q75,
    round(toFloat64(q90), 2) AS q90,
    round(toFloat64(q95), 2) AS q95,
    round(toFloat64(average), 2) AS average
FROM quantiles_usd

ORDER BY week, unit
