{{
    config(
        materialized='view',
        tags=['production', 'execution', 'tier1', 'api:circles_v2_group_token_supply', 'granularity:daily']
    )
}}

-- Long-format time-series view over fct_execution_circles_v2_group_token_supply_daily.
-- One row per (date, label) so the dashboard area chart can stack native ERC-1155
-- and wrapped ERC-20 as two series via seriesField='label'.

SELECT date, 'ERC-1155 (native)' AS label,  supply_native_erc1155 AS value, supply_native_demurraged AS value_demurraged
FROM {{ ref('fct_execution_circles_v2_group_token_supply_daily') }}
WHERE date < today()
UNION ALL
SELECT date, 'ERC-20 (wrapped)' AS label, supply_wrapped_erc20 AS value, supply_wrapped_demurraged AS value_demurraged
FROM {{ ref('fct_execution_circles_v2_group_token_supply_daily') }}
WHERE date < today()
ORDER BY date, label
