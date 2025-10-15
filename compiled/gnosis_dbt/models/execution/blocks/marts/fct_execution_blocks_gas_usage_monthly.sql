

WITH monthly AS (
  SELECT
    date_trunc('month', date) AS month,
    SUM(gas_used_sum)         AS gas_used_sum_monthly,
    SUM(gas_limit_sum)        AS gas_limit_sum_monthly
  FROM `dbt`.`int_execution_blocks_gas_usage_daily`
  GROUP BY month
)
SELECT
  month,
  gas_used_sum_monthly,
  gas_limit_sum_monthly,
  gas_used_sum_monthly / NULLIF(gas_limit_sum_monthly, 0) AS used
FROM monthly