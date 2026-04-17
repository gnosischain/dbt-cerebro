{{
    config(
        materialized='view',
        tags=['production','execution','tier0','api:execution_lending_lenders_count', 'granularity:latest']
    )
}}

-- "Active lenders" = unique wallets currently holding a positive supply balance in a
-- lending market on Gnosis (Aave V3, SparkLend). This is a STOCK measure (point-in-time
-- count), not a flow measure (users who supplied within a window). Protocol-scoped rows
-- plus an ALL-protocols aggregate row.
--
-- The card's change_pct compares today's active count vs the count on the same token
-- balances from 7 days ago (users whose balance was positive then, regardless of whether
-- they're still active now).

WITH

latest_date AS (
    SELECT max(date) AS max_date
    FROM {{ ref('int_execution_lending_aave_user_balances_daily') }}
    WHERE date < today()
),

curr AS (
    SELECT b.protocol, countDistinct(b.user_address) AS value
    FROM {{ ref('int_execution_lending_aave_user_balances_daily') }} b
    CROSS JOIN latest_date d
    WHERE b.date = d.max_date AND b.balance > 0
    GROUP BY b.protocol
),

prev AS (
    SELECT b.protocol, countDistinct(b.user_address) AS value
    FROM {{ ref('int_execution_lending_aave_user_balances_daily') }} b
    CROSS JOIN latest_date d
    WHERE b.date = subtractDays(d.max_date, 7) AND b.balance > 0
    GROUP BY b.protocol
),

combined AS (
    -- Per-protocol rows
    SELECT
        'ALL' AS token,
        c.protocol,
        toFloat64(COALESCE(c.value, 0)) AS value,
        CASE WHEN p.value IS NULL OR p.value = 0 THEN NULL
             ELSE ROUND((toFloat64(c.value) / toFloat64(p.value) - 1) * 100, 1) END AS change_pct
    FROM curr c
    LEFT JOIN prev p ON p.protocol = c.protocol

    UNION ALL

    -- All-protocols aggregate (distinct across both markets)
    SELECT
        'ALL' AS token,
        'ALL' AS protocol,
        toFloat64(COALESCE(sum_curr.value, 0)) AS value,
        CASE WHEN sum_prev.value IS NULL OR sum_prev.value = 0 THEN NULL
             ELSE ROUND((toFloat64(sum_curr.value) / toFloat64(sum_prev.value) - 1) * 100, 1) END AS change_pct
    FROM (
        SELECT countDistinct(b.user_address) AS value
        FROM {{ ref('int_execution_lending_aave_user_balances_daily') }} b
        CROSS JOIN latest_date d
        WHERE b.date = d.max_date AND b.balance > 0
    ) sum_curr
    CROSS JOIN (
        SELECT countDistinct(b.user_address) AS value
        FROM {{ ref('int_execution_lending_aave_user_balances_daily') }} b
        CROSS JOIN latest_date d
        WHERE b.date = subtractDays(d.max_date, 7) AND b.balance > 0
    ) sum_prev
)

SELECT token, protocol, value, change_pct FROM combined
