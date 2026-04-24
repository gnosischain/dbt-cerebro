{{
  config(
    materialized='view',
    tags=['production','execution','cow','tier1',
          'api:cow_solvers_volume_ts','granularity:daily']
  )
}}

WITH top_solvers AS (
    SELECT solver
    FROM {{ ref('fct_execution_cow_solvers_daily') }}
    WHERE date >= today() - INTERVAL 180 DAY
    GROUP BY solver
    ORDER BY sum(volume_usd) DESC
    LIMIT 6
)
SELECT
    date,
    CASE
        WHEN solver IN (SELECT solver FROM top_solvers)
        THEN concat(substring(solver, 1, 6), '..', substring(solver, length(solver) - 3, 4))
        ELSE 'Other'
    END                                                                          AS label,
    sum(volume_usd)                                                              AS value
FROM {{ ref('fct_execution_cow_solvers_daily') }}
WHERE date < today()
GROUP BY date, label
ORDER BY date, label
