{{ config(materialized='view', tags=['production','bridges','api']) }}

SELECT
  week  AS date,
  bridge AS series,
  cum_netflow_usd AS value
FROM {{ ref('int_bridges_netflow_weekly_by_bridge') }}
ORDER BY date, series