

SELECT
  address,
  date,
  token_address,
  symbol,
  token_class,
  balance_raw,
  balance,
  balance_usd
FROM `dbt`.`fct_execution_account_token_balances_latest`