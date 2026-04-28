

SELECT
  root_address,
  entity_type,
  entity_id,
  entity_address,
  relation,
  display_label,
  value_count,
  last_seen_at
FROM `dbt`.`fct_execution_account_linked_entities_latest`