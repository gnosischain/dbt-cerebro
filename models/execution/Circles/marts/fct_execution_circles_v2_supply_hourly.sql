{{
    config(
        materialized='view',
        tags=['production', 'execution', 'circles', 'supply']
    )
}}

SELECT
    hour,
    token_address,
    token_id,
    supply_delta_raw,
    total_supply_raw
FROM {{ ref('int_execution_circles_supply_hourly') }}
WHERE version = 2
