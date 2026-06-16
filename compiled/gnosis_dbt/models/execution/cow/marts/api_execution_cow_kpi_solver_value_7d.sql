

-- 7-day gross solver value (USD) with week-over-week change.
-- Covers priceImprovement and surplus fee policies (Sep 2024+).

WITH
recent AS (
    SELECT sum(solver_value_usd) AS v
    FROM `dbt`.`fct_execution_cow_trades`
    WHERE toDate(block_timestamp) >= today() - INTERVAL 7 DAY
      AND toDate(block_timestamp) < today()
      AND fee_source = 'api'
),
prior AS (
    SELECT sum(solver_value_usd) AS v
    FROM `dbt`.`fct_execution_cow_trades`
    WHERE toDate(block_timestamp) >= today() - INTERVAL 14 DAY
      AND toDate(block_timestamp) < today() - INTERVAL 7 DAY
      AND fee_source = 'api'
)
SELECT
    round((SELECT v FROM recent), 2)                                             AS value,
    round(((SELECT v FROM recent) - (SELECT v FROM prior))
          / nullIf((SELECT v FROM prior), 0) * 100, 1)                           AS change_pct,
    today() - 1                                                                  AS as_of_date