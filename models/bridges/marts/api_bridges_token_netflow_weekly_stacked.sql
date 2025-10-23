{{ config(materialized='view', tags=['production','bridges','api']) }}

SELECT
  week  AS date,
  token AS series,
  netflow_usd_week AS value
FROM {{ ref('fct_bridges_token_netflow_weekly') }}
WHERE netflow_usd_week != 0
ORDER BY date, series