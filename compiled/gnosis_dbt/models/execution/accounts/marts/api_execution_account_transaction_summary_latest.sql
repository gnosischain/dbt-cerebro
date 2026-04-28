

SELECT
  address,
  first_activity_date,
  last_activity_date,
  active_days,
  token_transfer_count,
  inbound_transfer_count,
  outbound_transfer_count,
  counterparty_count,
  token_count_moved,
  inbound_gross_amount_raw,
  outbound_gross_amount_raw
FROM `dbt`.`fct_execution_account_transaction_summary_latest`