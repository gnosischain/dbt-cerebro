{{
    config(
        materialized='view',
        tags=['dev', 'live', 'execution', 'pools', 'trades', 'api']
    )
}}

WITH

hwm AS (
    SELECT max(block_timestamp) AS ts
    FROM {{ ref('int_live__dex_trades_raw') }}
),

recent AS (
    SELECT transaction_hash, block_number, amount_usd
    FROM {{ ref('int_live__dex_trades_raw') }} FINAL
    WHERE block_timestamp >= (SELECT ts FROM hwm) - INTERVAL 30 MINUTE
      AND block_timestamp <= (SELECT ts FROM hwm) - INTERVAL 60 SECOND
),

recent_tx AS (
    SELECT
        transaction_hash,
        block_number,
        from_address,
        to_address
    FROM {{ source('execution_live', 'transactions') }}
    WHERE block_timestamp >= (SELECT ts FROM hwm) - INTERVAL 30 MINUTE
      AND block_timestamp <= (SELECT ts FROM hwm) - INTERVAL 60 SECOND
),

tx_summary AS (
    SELECT
        transaction_hash,
        min(block_number)   AS block_number,
        max(amount_usd)     AS trade_usd,
        count()             AS hops
    FROM recent
    GROUP BY transaction_hash
)

SELECT
    count()                                                                          AS trade_count,
    round(sum(s.trade_usd), 0)                                                       AS volume_usd,
    uniqExact(tx.from_address)                                                       AS unique_traders,
    round(100.0 * countIf(lbl.project != '')        / nullIf(count(), 0), 1)         AS aggregator_share_pct,
    round(100.0 * countIf(s.hops > 1)              / nullIf(count(), 0), 1)         AS multihop_share_pct
FROM tx_summary s
LEFT JOIN recent_tx tx
    ON  tx.transaction_hash = s.transaction_hash
    AND tx.block_number     = s.block_number
LEFT JOIN {{ ref('int_crawlers_data_labels_dex') }} lbl
    ON lbl.address = concat('0x', lower(replaceAll(coalesce(tx.to_address, ''), '0x', '')))
