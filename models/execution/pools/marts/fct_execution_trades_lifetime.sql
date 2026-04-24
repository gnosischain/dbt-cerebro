{{
    config(
        materialized='table',
        tags=['dev', 'execution', 'pools', 'trades', 'fct']
    )
}}

-- One-row lifetime summary for the Trades → Stats dashboard tiles.
-- Full rebuild on every dbt run (cheap: tx-grain source is ~10M rows,
-- no joins, single-pass aggregation).

SELECT
    round(sum(trade_usd), 0)                    AS lifetime_volume_usd,
    count()                                     AS lifetime_trade_count,
    uniqExact(tx_from)                          AS lifetime_unique_traders
FROM {{ ref('int_execution_trades_by_tx') }}
