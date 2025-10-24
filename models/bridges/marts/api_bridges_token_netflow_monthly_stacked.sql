{{ config(materialized='view', tags=['production','bridges','api']) }}

SELECT
  month AS date,
  token AS series,
  netflow_usd_month AS value
FROM {{ ref('fct_bridges_token_netflow_monthly') }}
WHERE netflow_usd_month != 0
ORDER BY date, series