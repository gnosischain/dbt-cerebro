{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(action, date, token)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','celo','gpay']
  )
}}

-- Per action x day x token volume (native + USD) and activity count, with
-- running cumulatives. Mirrors fct_execution_gpay_actions_by_token_daily.
-- Token set is USDC / USDT only (see int_celo_gpay_activity).
SELECT
    action,
    date,
    token,
    volume,
    volume_usd,
    activity_count,
    SUM(volume)         OVER (PARTITION BY action, token ORDER BY date) AS volume_cumulative,
    SUM(volume_usd)     OVER (PARTITION BY action, token ORDER BY date) AS volume_usd_cumulative,
    SUM(activity_count) OVER (PARTITION BY action, token ORDER BY date) AS activity_count_cumulative
FROM (
    SELECT
        action,
        date,
        token_symbol        AS token,
        sum(amount)         AS volume,
        sum(amount_usd)     AS volume_usd,
        sum(activity_count) AS activity_count
    FROM {{ ref('int_celo_gpay_activity_daily') }}
    WHERE date < today()
    GROUP BY action, date, token
)
ORDER BY action, date, token
