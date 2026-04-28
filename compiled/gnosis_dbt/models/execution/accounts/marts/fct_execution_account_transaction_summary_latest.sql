

SELECT
  address,
  min(date) AS first_activity_date,
  max(date) AS last_activity_date,
  uniqExact(date) AS active_days,
  sum(transfer_count) AS token_transfer_count,
  sumIf(transfer_count, direction = 'in') AS inbound_transfer_count,
  sumIf(transfer_count, direction = 'out') AS outbound_transfer_count,
  uniqExact(counterparty) AS counterparty_count,
  uniqExact(token_address) AS token_count_moved,
  sumIf(gross_amount_raw, direction = 'in') AS inbound_gross_amount_raw,
  sumIf(gross_amount_raw, direction = 'out') AS outbound_gross_amount_raw
FROM `dbt`.`fct_execution_account_token_movements_daily`
GROUP BY address