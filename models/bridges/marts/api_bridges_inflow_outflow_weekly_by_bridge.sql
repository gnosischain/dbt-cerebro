{{ config(materialized='view', tags=['production','bridges','api']) }}

SELECT week AS date, bridge, 'inflow'  AS series, inflow_usd  AS value
FROM {{ ref('int_bridges_in_out_weekly_by_bridge') }}
UNION ALL
SELECT week AS date, bridge, 'outflow' AS series, outflow_usd AS value
FROM {{ ref('int_bridges_in_out_weekly_by_bridge') }}
ORDER BY date, bridge, series