

SELECT
  source,
  target,
  source AS source_name,
  target AS target_name,
  edge_type,
  weight,
  raw_volume,
  last_seen_date
FROM `dbt`.`fct_execution_account_counterparty_edges_latest`