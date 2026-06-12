

SELECT sub.*, (SELECT toDate(max(date)) FROM `dbt`.`int_execution_tokens_balances_daily`) AS as_of_date
FROM (
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
) AS sub