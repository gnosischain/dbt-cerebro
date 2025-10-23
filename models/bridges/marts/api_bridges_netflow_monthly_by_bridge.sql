{{ config(materialized='view', tags=['production','bridges','api']) }}

SELECT
  month AS date,
  bridge AS series,
  netflow_usd_month AS value
FROM {{ ref('fct_bridges_netflow_monthly_by_bridge') }}
ORDER BY date, series