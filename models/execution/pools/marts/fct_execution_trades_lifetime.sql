{{
    config(
        materialized='table',
        tags=['production', 'execution', 'pools', 'trades', 'fct']
    )
}}

-- One-row lifetime summary for the Trades → Stats dashboard tiles.
-- Volume is summed at swap (hop) grain to match the volume timeseries convention:
-- each pool leg in a multi-hop route contributes its amount_usd independently.
-- Trade count and unique traders stay on tx grain (multi-hop = one trade).

SELECT
    (
        SELECT round(sum(amount_usd), 0)
        FROM {{ ref('int_execution_pools_dex_trades') }}
    )                                           AS lifetime_volume_usd,
    count()                                     AS lifetime_trade_count,
    uniqExact(tx_from)                          AS lifetime_unique_traders
FROM {{ ref('int_execution_trades_by_tx') }}
