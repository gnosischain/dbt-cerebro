{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(week, earning_kind)',
    settings={'allow_nullable_key': 1},
    tags=['production','execution','circles_v2','weau','weekly']
  )
}}

-- Weekly counts of economically active Circles avatars, ecosystem-wide,
-- from the circles-first layer. Per earning_kind plus an 'any' rollup
-- (distinct avatars across kinds). avatars_in_app_tx counts the subset
-- whose contributing events came through a Gnosis App relayer tx
-- (inviter fees only — cashback has no tx-origin attribution).

WITH base AS (
    SELECT week, avatar, earning_kind, any_in_app_tx
    FROM {{ ref('int_execution_circles_v2_economically_active_avatars_weekly') }}
)

SELECT
    week,
    earning_kind,
    count(DISTINCT avatar)                                        AS avatars,
    count(DISTINCT if(any_in_app_tx = 1, avatar, NULL))           AS avatars_in_app_tx
FROM base
GROUP BY week, earning_kind

UNION ALL

SELECT
    week,
    'any'                                                         AS earning_kind,
    count(DISTINCT avatar)                                        AS avatars,
    count(DISTINCT if(any_in_app_tx = 1, avatar, NULL))           AS avatars_in_app_tx
FROM base
GROUP BY week
