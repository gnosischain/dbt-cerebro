{{ config(materialized='view', tags=['production','bridges', 'tier1', 'api: cum_netflow_by_bridge_w', ]) }}

SELECT
  week  AS date,
  bridge AS series,
  cum_netflow_usd AS value
FROM {{ ref('fct_bridges_netflow_weekly_by_bridge') }}
ORDER BY date, series