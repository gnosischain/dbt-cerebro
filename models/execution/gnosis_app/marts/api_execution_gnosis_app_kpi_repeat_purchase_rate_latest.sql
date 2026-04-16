{{
  config(
    materialized='view',
    tags=['production','execution','gnosis_app','kpi','tier0',
          'api:gnosis_app_kpi_repeat_purchase_rate','granularity:last_30d']
  )
}}

WITH base AS (
    SELECT
        address,
        sum(n_events) AS purchases
    FROM {{ ref('int_execution_gnosis_app_user_activity_daily') }}
    WHERE activity_kind IN ('swap_filled','marketplace_buy')
      AND date >= today() - INTERVAL 30 DAY
      AND date < today()
    GROUP BY address
)
SELECT
    round(
        countIf(purchases >= 2)
        / nullIf(count(*), 0)
        * 100,
        1
    )                                                 AS value,
    CAST(NULL AS Nullable(Float64))                   AS change_pct
FROM base
