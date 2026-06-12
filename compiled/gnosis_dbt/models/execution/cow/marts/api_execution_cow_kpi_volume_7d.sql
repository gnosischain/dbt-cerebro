

WITH
recent AS (
    SELECT sum(volume_usd) AS v
    FROM `dbt`.`fct_execution_cow_daily`
    WHERE date >= today() - INTERVAL 7 DAY AND date < today()
),
prior AS (
    SELECT sum(volume_usd) AS v
    FROM `dbt`.`fct_execution_cow_daily`
    WHERE date >= today() - INTERVAL 14 DAY AND date < today() - INTERVAL 7 DAY
)
SELECT
    round((SELECT v FROM recent), 0)                                             AS value,
    round(((SELECT v FROM recent) - (SELECT v FROM prior))
          / nullIf((SELECT v FROM prior), 0) * 100, 1)                           AS change_pct