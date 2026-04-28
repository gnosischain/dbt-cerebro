

SELECT
    i.date AS date
    ,i.validator_index AS validator_index
    ,s.status AS status
    ,s.pubkey AS pubkey
    ,s.withdrawal_credentials AS withdrawal_credentials
    ,s.withdrawal_address AS withdrawal_address
    ,i.balance_gno AS balance_gno
    ,i.balance_prev_gno AS balance_prev_gno
    ,i.effective_balance_gno AS effective_balance_gno
    ,i.deposits_amount_gno AS deposits_amount_gno
    ,i.deposits_count AS deposits_count
    ,i.withdrawals_amount_gno AS withdrawals_amount_gno
    ,i.withdrawals_count AS withdrawals_count
    ,i.consolidation_inflow_gno AS consolidation_inflow_gno
    ,i.consolidation_outflow_gno AS consolidation_outflow_gno
    ,i.consensus_income_amount_gno AS consensus_income_amount_gno
    ,i.daily_rate AS daily_rate
    ,i.apy AS apy
    ,i.cumulative_deposits_gno AS cumulative_deposits_gno
    ,i.cumulative_withdrawals_gno AS cumulative_withdrawals_gno
    ,i.cumulative_consolidation_inflow_gno AS cumulative_consolidation_inflow_gno
    ,i.cumulative_consolidation_outflow_gno AS cumulative_consolidation_outflow_gno
    ,i.total_income_estimated_gno AS total_income_estimated_gno
    ,COALESCE(p.proposed_blocks_count, 0) AS proposed_blocks_count
    ,COALESCE(p.proposer_reward_total_gno, 0) AS proposer_reward_total_gno
    ,COALESCE(p.proposer_reward_attestations_gno, 0) AS proposer_reward_attestations_gno
    ,COALESCE(p.proposer_reward_sync_aggregate_gno, 0) AS proposer_reward_sync_aggregate_gno
    ,COALESCE(p.proposer_reward_proposer_slashings_gno, 0) AS proposer_reward_proposer_slashings_gno
    ,COALESCE(p.proposer_reward_attester_slashings_gno, 0) AS proposer_reward_attester_slashings_gno
FROM `dbt`.`int_consensus_validators_income_daily` i
LEFT JOIN `dbt`.`int_consensus_validators_snapshots_daily` s
    ON s.date = i.date AND s.validator_index = i.validator_index
LEFT JOIN `dbt`.`int_consensus_validators_proposer_rewards_daily` p
    ON p.date = i.date AND p.validator_index = i.validator_index