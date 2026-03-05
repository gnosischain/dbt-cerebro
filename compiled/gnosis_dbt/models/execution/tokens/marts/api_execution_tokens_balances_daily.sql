

SELECT
    date,
    token_address,
    symbol,
    address,
    balance,
    balance_usd
FROM `dbt`.`int_execution_tokens_balances_daily`