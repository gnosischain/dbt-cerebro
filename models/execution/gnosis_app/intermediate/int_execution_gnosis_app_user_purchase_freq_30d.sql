{{
  config(
    materialized='table',
    engine='MergeTree()',
    order_by='(address)',
    settings={'allow_nullable_key': 1},
    tags=['production','execution','gnosis_app','purchase_freq']
  )
}}

-- Rolling 30-day per-user purchase-event count. Full rebuild on every
-- run; small (one row per active GA user in last 30d).
--
-- Counts both swap_filled and marketplace_buy events, matching the
-- definition of "repeat purchase" used by api_execution_gnosis_app_kpi_repeat_purchase_rate_latest.

SELECT
    address                                         AS address,
    sum(n_events)                                   AS n_purchases
FROM {{ ref('int_execution_gnosis_app_user_activity_daily') }}
WHERE activity_kind IN ('swap_filled', 'marketplace_buy')
  AND date >= today() - 30
  AND date <  today()
GROUP BY address
HAVING n_purchases >= 1
