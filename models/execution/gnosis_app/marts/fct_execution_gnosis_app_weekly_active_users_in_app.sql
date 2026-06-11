{{
  config(
    materialized='table',
    engine='MergeTree()',
    order_by='(week, is_blacklisted)',
    settings={'allow_nullable_key': 1},
    tags=['production','execution','gnosis_app','wau','mart'],
    pre_hook=["SET join_use_nulls = 1"],
    post_hook=["SET join_use_nulls = 0"]
  )
}}

-- Gnosis App Weekly Active Users, IN-APP ONLY — distinct addresses active
-- that week via actions taken in the Gnosis App (Cometh-relayed circles
-- heuristics, swaps, topups, marketplace, token offers). Companion to
-- fct_execution_gnosis_app_weekly_active_users, which is ecosystem-wide
-- (its Circles leg counts mints/trusts made through ANY app). By
-- construction in_app cnt <= ecosystem cnt for every week.

WITH base AS (
    SELECT
        s.week,
        s.address,
        b.address IS NOT NULL AS is_blacklisted
    FROM {{ ref('int_execution_gnosis_app_weekly_signals_in_app') }} s
    LEFT JOIN {{ ref('stg_crawlers_data__circles_blacklisted') }} b
        ON b.address = s.address
)

SELECT
    week,
    is_blacklisted,
    count(DISTINCT address) AS cnt
FROM base
GROUP BY week, is_blacklisted
ORDER BY week, is_blacklisted
