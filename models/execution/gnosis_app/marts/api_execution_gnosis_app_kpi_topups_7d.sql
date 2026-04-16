{{
  config(
    materialized='view',
    tags=['production','execution','gnosis_app','kpi','tier0',
          'api:gnosis_app_kpi_topups','granularity:last_7d']
  )
}}

WITH days AS (
    SELECT date, sum(n_topups) AS n_topups
    FROM {{ ref('fct_execution_gnosis_app_gpay_topups_by_token_daily') }}
    GROUP BY date
),
recent AS (SELECT sum(n_topups) AS v FROM days
           WHERE date >= today() - INTERVAL 7 DAY AND date < today()),
prior  AS (SELECT sum(n_topups) AS v FROM days
           WHERE date >= today() - INTERVAL 14 DAY AND date < today() - INTERVAL 7 DAY)
SELECT
    (SELECT v FROM recent)                                                AS value,
    round(((SELECT v FROM recent) - (SELECT v FROM prior))
          / nullIf((SELECT v FROM prior), 0) * 100, 1)                    AS change_pct
