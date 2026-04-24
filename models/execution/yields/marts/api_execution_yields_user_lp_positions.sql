{{
    config(
        materialized='view',
        tags=['production','execution','yields','api:yields_user_lp_positions']
    )
}}

SELECT
    provider,
    pool_address,
    protocol,
    tick_lower,
    tick_upper,
    capital_in_usd,
    capital_out_usd,
    fees_collected_usd,
    is_active,
    is_in_range,
    pool_current_tick,
    has_unpriced_tokens,
    entry_date,
    last_action_date
FROM {{ ref('int_execution_yields_user_lp_positions') }}
