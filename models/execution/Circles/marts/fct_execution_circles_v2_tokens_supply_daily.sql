{{
    config(
        materialized='view',
        tags=['production', 'execution', 'circles_v2', 'supply_daily']
    )
}}

-- Compatibility view over int_execution_circles_v2_tokens_supply_daily.
-- The historical `table` materialization fully rebuilt every prod run from
-- ~170k-rows-per-day source; the int_ model is now incremental and the mart
-- exposes the same shape with no rebuild cost. Downstream consumers (Cerebro
-- dashboards, the `fct_execution_circles_v2_total_supply_daily` mart, etc.)
-- continue to ref this name.

SELECT date, token_address, supply_raw, supply, demurraged_supply
FROM {{ ref('int_execution_circles_v2_tokens_supply_daily') }}
