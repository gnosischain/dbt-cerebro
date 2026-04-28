

SELECT
  search_key,
  result_type,
  address,
  display_label,
  subtitle,
  badges,
  validator_index,
  withdrawal_credentials,
  score_base
FROM `dbt`.`fct_execution_account_search_index`