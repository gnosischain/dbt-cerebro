{{
    config(
        materialized='view',
        tags=['dev', 'execution', 'pools', 'trades', 'api']
    )
}}

SELECT
    lifetime_volume_usd,
    lifetime_trade_count,
    lifetime_unique_traders
FROM {{ ref('fct_execution_trades_lifetime') }}
