{{
    config(
        materialized='view',
        tags=['production','execution','tier1','api:circles_v2_supply_by_holder_type', 'granularity:daily']
    )
}}

SELECT
    date,
    holder_type AS label,
    supply AS value,
    demurraged_supply AS value_demurraged,
    holder_count
FROM {{ ref('fct_execution_circles_v2_supply_by_holder_type_daily') }}
WHERE date < today()
ORDER BY date DESC, label
