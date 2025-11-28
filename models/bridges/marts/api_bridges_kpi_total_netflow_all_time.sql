{{ config(materialized='view', tags=['production','bridges', 'tier0', 'api: netflow_total']) }}
SELECT round(cum_net_usd, 2) AS value
FROM {{ ref('fct_bridges_kpis_snapshot') }}
ORDER BY as_of_date DESC
LIMIT 1