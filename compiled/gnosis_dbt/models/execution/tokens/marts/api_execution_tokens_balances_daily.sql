

SELECT
    date,
    token_address,
    symbol,
    address,
    balance,
    balance_usd
FROM `dbt`.`int_rpc_state_indexer_token_balances_priced_daily`