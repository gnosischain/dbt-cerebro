{{
  config(
    materialized='view',
    tags=['production','execution','cow','kpi','tier0',
          'api:cow_kpi_fees_7d','granularity:last_7d']
  )
}}

WITH
recent AS (
    SELECT sum(fees_usd) AS v
    FROM {{ ref('fct_execution_cow_daily') }}
    WHERE date >= today() - INTERVAL 7 DAY AND date < today()
),
prior AS (
    SELECT sum(fees_usd) AS v
    FROM {{ ref('fct_execution_cow_daily') }}
    WHERE date >= today() - INTERVAL 14 DAY AND date < today() - INTERVAL 7 DAY
)
SELECT
    round((SELECT v FROM recent), 2)                                             AS value,
    round(((SELECT v FROM recent) - (SELECT v FROM prior))
          / nullIf((SELECT v FROM prior), 0) * 100, 1)                           AS change_pct
