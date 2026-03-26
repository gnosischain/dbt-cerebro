{{
    config(
        materialized='view',
        tags=['production','execution','tier1','api:yields_lending_activity_volumes', 'granularity:weekly']
    )
}}

SELECT
    week AS date,
    symbol AS token,
    token_class,
    protocol AS label,
    'Deposits' AS volume_type,
    deposits_volume_weekly AS value
FROM {{ ref('fct_execution_yields_lending_weekly') }}
WHERE deposits_volume_weekly > 0

UNION ALL

SELECT
    week AS date,
    symbol AS token,
    token_class,
    protocol AS label,
    'Borrows' AS volume_type,
    borrows_volume_weekly AS value
FROM {{ ref('fct_execution_yields_lending_weekly') }}
WHERE borrows_volume_weekly > 0

ORDER BY date DESC, token, label, volume_type
