

SELECT
  date,
  address,
  counterparty,
  symbol,
  token_address,
  direction,
  transfer_count,
  net_amount_raw,
  gross_amount_raw
FROM `dbt`.`fct_execution_account_token_movements_daily`