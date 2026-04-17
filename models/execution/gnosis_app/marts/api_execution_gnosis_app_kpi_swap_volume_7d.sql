{{
  config(
    materialized='view',
    tags=['production','execution','gnosis_app','kpi','tier0',
          'api:gnosis_app_kpi_swap_volume','granularity:last_7d']
  )
}}

WITH days AS (
    SELECT date, volume_usd_filled FROM {{ ref('fct_execution_gnosis_app_swaps_daily') }}
),
recent AS (SELECT sum(volume_usd_filled) AS v FROM days
           WHERE date >= today() - INTERVAL 7 DAY AND date < today()),
prior  AS (SELECT sum(volume_usd_filled) AS v FROM days
           WHERE date >= today() - INTERVAL 14 DAY AND date < today() - INTERVAL 7 DAY)
SELECT
    round(toFloat64((SELECT v FROM recent)), 2)                              AS value,
    round(((SELECT v FROM recent) - (SELECT v FROM prior))
          / nullIf((SELECT v FROM prior), 0) * 100, 1)                        AS change_pct
