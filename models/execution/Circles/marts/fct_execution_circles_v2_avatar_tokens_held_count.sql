{{
    config(
        materialized='table',
        tags=['production','execution','circles','v2','avatar','mart']
    )
}}

-- Per-avatar count of distinct CRC tokens currently held with a balance
-- above the 0.001 CRC dust threshold (1e15 raw wei). Materialised daily
-- from the per-day balance fact; the matching api_ is a thin passthrough.

WITH latest AS (
    SELECT max(date) AS d
    FROM {{ ref('int_execution_circles_v2_balances_daily') }}
    WHERE date < today()
)
SELECT
    b.account                       AS avatar,
    uniqExact(b.token_address)      AS tokens_held_count
FROM {{ ref('int_execution_circles_v2_balances_daily') }} b
CROSS JOIN latest
WHERE b.date = latest.d
  AND b.balance_raw > pow(10, 15)
GROUP BY b.account
