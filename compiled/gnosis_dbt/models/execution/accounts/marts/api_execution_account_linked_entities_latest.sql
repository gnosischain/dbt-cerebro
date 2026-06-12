

SELECT sub.*, (SELECT toDate(max(block_timestamp)) FROM `dbt`.`int_execution_safes_owner_events`) AS as_of_date
FROM (
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
) AS sub