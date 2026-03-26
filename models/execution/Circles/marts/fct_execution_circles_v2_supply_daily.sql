{{
    config(
        materialized='view',
        tags=['production', 'execution', 'circles', 'supply']
    )
}}

SELECT
    date,
    token_address,
    token_id,
    supply_delta_raw,
    total_supply_raw
FROM {{ ref('int_execution_circles_supply_daily') }}
WHERE version = 2
