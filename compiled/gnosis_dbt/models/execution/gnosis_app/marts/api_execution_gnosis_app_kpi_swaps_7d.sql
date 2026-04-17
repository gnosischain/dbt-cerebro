

WITH days AS (
    SELECT date, n_swaps FROM `dbt`.`fct_execution_gnosis_app_swaps_daily`
),
recent AS (SELECT sum(n_swaps) AS v FROM days
           WHERE date >= today() - INTERVAL 7 DAY AND date < today()),
prior  AS (SELECT sum(n_swaps) AS v FROM days
           WHERE date >= today() - INTERVAL 14 DAY AND date < today() - INTERVAL 7 DAY)
SELECT
    (SELECT v FROM recent)                                                AS value,
    round(((SELECT v FROM recent) - (SELECT v FROM prior))
          / nullIf((SELECT v FROM prior), 0) * 100, 1)                    AS change_pct