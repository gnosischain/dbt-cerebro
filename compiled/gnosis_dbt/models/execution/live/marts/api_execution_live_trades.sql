WITH

hwm AS (
    SELECT max(block_timestamp) AS ts
    FROM `dbt`.`int_live__dex_trades_raw`
),

recent AS (
    SELECT *
    FROM `dbt`.`int_live__dex_trades_raw`
    WHERE block_timestamp >= (SELECT ts FROM hwm) - INTERVAL 30 MINUTE
      AND block_timestamp <= (SELECT ts FROM hwm) - INTERVAL 60 SECOND
),

recent_tx AS (
    SELECT
        transaction_hash,
        block_number,
        from_address,
        to_address
    FROM `execution_live`.`transactions`
    WHERE block_timestamp >= (SELECT ts FROM hwm) - INTERVAL 30 MINUTE
      AND block_timestamp <= (SELECT ts FROM hwm) - INTERVAL 60 SECOND
),

tx_summary AS (
    SELECT
        transaction_hash,
        min(block_timestamp)                                                AS block_timestamp,
        min(block_number)                                                   AS block_number,
        arrayStringConcat(
            arrayFilter(x -> x != '', groupUniqArray(protocol)), ', '
        )                                                                   AS via,
        count()                                                             AS hops,
        argMin(token_sold_symbol,   log_index)                              AS token_sold,
        argMin(amount_sold,         log_index)                              AS amount_sold,
        argMax(token_bought_symbol, log_index)                              AS token_bought,
        argMax(amount_bought,       log_index)                              AS amount_bought,
        max(amount_usd)                                                     AS trade_usd
    FROM recent
    GROUP BY transaction_hash
)

SELECT
    s.block_timestamp            AS block_timestamp,
    s.block_number               AS block_number,
    s.transaction_hash           AS transaction_hash,
    s.token_sold                 AS token_sold,
    round(s.amount_sold, 6)      AS amount_sold,
    s.token_bought               AS token_bought,
    round(s.amount_bought, 6)    AS amount_bought,
    round(s.trade_usd, 2)        AS trade_usd,
    s.via                        AS via,
    s.hops                       AS hops,
    tx.from_address              AS trader,
    lbl.project                  AS aggregator
FROM tx_summary s
LEFT JOIN recent_tx tx
    ON tx.transaction_hash = s.transaction_hash
    AND tx.block_number    = s.block_number
LEFT JOIN `dbt`.`int_crawlers_data_labels` lbl
    ON lbl.address = concat('0x', lower(replaceAll(coalesce(tx.to_address, ''), '0x', '')))
WHERE (s.token_sold != '' OR s.token_bought != '')
  AND (s.trade_usd IS NULL OR s.trade_usd >= 1)
ORDER BY s.block_timestamp DESC