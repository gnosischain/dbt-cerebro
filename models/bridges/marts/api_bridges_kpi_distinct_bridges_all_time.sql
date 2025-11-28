{{ config(materialized='view', tags=['production','bridges', 'tier0', 'api: distinct_bridges_total']) }}
SELECT distinct_bridges AS value
FROM {{ ref('fct_bridges_kpis_snapshot') }}
ORDER BY as_of_date DESC
LIMIT 1