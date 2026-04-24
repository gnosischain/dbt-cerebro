{{
  config(
    materialized='view',
    tags=['production','execution','cow','kpi','tier0',
          'api:cow_kpi_traders_7d','granularity:last_7d']
  )
}}

WITH
recent AS (
    SELECT uniqExact(taker) AS v
    FROM {{ ref('fct_execution_cow_trades') }}
    WHERE toDate(block_timestamp) >= today() - INTERVAL 7 DAY
      AND toDate(block_timestamp) < today()
),
prior AS (
    SELECT uniqExact(taker) AS v
    FROM {{ ref('fct_execution_cow_trades') }}
    WHERE toDate(block_timestamp) >= today() - INTERVAL 14 DAY
      AND toDate(block_timestamp) < today() - INTERVAL 7 DAY
)
SELECT
    (SELECT v FROM recent)                                                       AS value,
    round(((SELECT v FROM recent) - (SELECT v FROM prior))
          / nullIf((SELECT v FROM prior), 0) * 100, 1)                           AS change_pct
