

SELECT
  date,
  token_address,
  symbol,
  token_class,
  address,
  counterparty,
  direction,
  net_amount_raw,
  gross_amount_raw,
  transfer_count
FROM `dbt`.`fct_execution_account_token_movements_daily`