{{
    config(
        materialized='view',
        tags=['dev', 'execution', 'pools', 'trades', 'api']
    )
}}

-- One-row lifetime summary of DEX trading on Gnosis Chain. Feeds the four
-- fixed tiles at the bottom of the Trades → Stats tab (always all-time,
-- never affected by the window selector).

SELECT
    round(sum(amount_usd), 0)                       AS lifetime_volume_usd,
    uniqExact(transaction_hash)                     AS lifetime_trade_count,
    uniqExact(tx_from)                              AS lifetime_unique_traders,
    toDate(min(block_timestamp))                    AS first_trade_date
FROM {{ ref('int_execution_pools_dex_trades') }}
WHERE block_timestamp < today()
