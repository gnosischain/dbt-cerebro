

SELECT
    transaction_hash,
    wallet_address,
    block_timestamp AS timestamp,
    date,
    action,
    symbol,
    direction,
    round(toFloat64(amount), 6)     AS amount,
    round(toFloat64(amount_usd), 2) AS amount_usd,
    counterparty
FROM `dbt`.`int_execution_gpay_activity`
ORDER BY block_timestamp DESC