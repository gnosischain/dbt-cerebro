

SELECT
  wallet_address,
  token,
  value_usd,
  value_native,
  date
FROM `dbt`.`fct_execution_gpay_user_balances_latest`