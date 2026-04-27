{{
  config(
    materialized='view',
    tags=['production','execution','cow','kpi','tier0',
          'api:cow_kpi_active_solvers','granularity:last_7d']
  )
}}

WITH
recent AS (
    SELECT countDistinct(solver) AS v
    FROM {{ ref('fct_execution_cow_solvers_daily') }}
    WHERE date >= today() - INTERVAL 7 DAY
      AND date < today()
      AND num_batches > 0
),
prior AS (
    SELECT countDistinct(solver) AS v
    FROM {{ ref('fct_execution_cow_solvers_daily') }}
    WHERE date >= today() - INTERVAL 14 DAY
      AND date < today() - INTERVAL 7 DAY
      AND num_batches > 0
)
SELECT
    (SELECT v FROM recent)                                                       AS value,
    round(((SELECT v FROM recent) - (SELECT v FROM prior))
          / nullIf((SELECT v FROM prior), 0) * 100, 1)                           AS change_pct
