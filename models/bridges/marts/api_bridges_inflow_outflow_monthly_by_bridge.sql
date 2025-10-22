{{ config(materialized='view', tags=['production','bridges','api']) }}

SELECT month AS date, bridge, 'inflow'  AS series, inflow_usd_month  AS value
FROM {{ ref('int_bridges_in_out_monthly_by_bridge') }}
UNION ALL
SELECT month AS date, bridge, 'outflow' AS series, outflow_usd_month AS value
FROM {{ ref('int_bridges_in_out_monthly_by_bridge') }}
ORDER BY date, bridge, series