{{
  config(materialized='view', tags=['production','execution','transactions','gas'])
}}

SELECT
  date_trunc('month', day)                           AS month,
  SUM(gas_used_sum)                                  AS gas_used_sum,
  SUM(gas_limit_sum)                                 AS gas_limit_sum,
  SUM(gas_used_sum) / NULLIF(SUM(gas_limit_sum), 0)  AS used
FROM {{ ref('int_execution_blocks_gas_usage_daily') }}
GROUP BY month