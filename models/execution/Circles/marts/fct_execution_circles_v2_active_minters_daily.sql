{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='date',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles_v2', 'minters']
    )
}}

-- Active Minters per day: avatars that minted on each of the last 14 days
-- AND whose 14-day mint sum is at least 80% of the theoretical maximum.
--
--   theoretical_max  = 24 / hr * 14 days  = 336 CRC
--   active threshold = 0.8 * 336          = 268.8 CRC
--
-- This is the canonical "Active Minters" KPI shown on the Dune circles-v2-kpis
-- board (queried in daily_active_minters CTE there). Blacklisted avatars are
-- intentionally not filtered here so the fact remains usable for both flagged
-- and unflagged variants downstream.

SELECT
    date,
    count(DISTINCT avatar) AS active_minters
FROM {{ ref('int_execution_circles_v2_mint_activity_daily') }}
WHERE mint_days_14dw = 14
  AND mint_14dw      >= 0.8 * 336
GROUP BY date
ORDER BY date
