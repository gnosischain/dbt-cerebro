

SELECT
  label,
  block_date AS date,
  supply
FROM `dbt`.`int_rpc_state_indexer_gno_supply_daily`
ORDER BY date, label