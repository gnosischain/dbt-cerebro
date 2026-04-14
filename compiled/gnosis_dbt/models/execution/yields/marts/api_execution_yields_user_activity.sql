

SELECT
    block_timestamp,
    toDate(block_timestamp) AS date,
    transaction_hash,
    protocol,
    position_address,
    wallet_address,
    action,
    token_symbol,
    token_address,
    amount,
    amount_usd,
    source
FROM `dbt`.`int_execution_yields_user_activity`