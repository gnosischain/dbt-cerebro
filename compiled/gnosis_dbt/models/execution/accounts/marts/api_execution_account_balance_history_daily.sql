

SELECT
  address,
  date,
  total_balance_usd,
  tokens_held,
  native_or_wrapped_xdai_balance,
  priced_balance_usd,
  priced_tokens_held
FROM `dbt`.`fct_execution_account_balance_history_daily`