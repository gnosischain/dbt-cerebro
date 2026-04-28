

SELECT
  validator_index,
  pubkey,
  withdrawal_credentials,
  withdrawal_address,
  status,
  slashed,
  balance_gno,
  effective_balance_gno,
  consensus_income_amount_30d_gno,
  total_income_estimated_gno,
  proposed_blocks_count_lifetime,
  latest_date
FROM `dbt`.`fct_consensus_validators_explorer_members_table`