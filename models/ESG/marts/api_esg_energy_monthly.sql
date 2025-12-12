{{ 
    config(
        materialized='view',
        tags=['production','esg', 'tier1', 'api:energy_consumption', 'granularity:monthly']
    )
}}


SELECT toStartOfMonth(date) AS date, SUM(daily_energy_kwh_total) AS value
FROM {{ ref('fct_esg_carbon_footprint_uncertainty') }}
WHERE toStartOfMonth(date) < toStartOfMonth(today())
GROUP BY 1
ORDER BY date