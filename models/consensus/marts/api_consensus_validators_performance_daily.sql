{{
    config(
        materialized='view',
        tags=["production", "consensus", 'tier1', 'api:validators_performance', 'granularity:daily'],
        meta={
            "api": {
                "methods": ["GET", "POST"],
                "allow_unfiltered": false,
                "require_any_of": ["validator_index", "pubkey", "withdrawal_credentials", "withdrawal_address"],
                "parameters": [
                    {
                        "name": "validator_index",
                        "column": "validator_index",
                        "operator": "IN",
                        "type": "integer_list",
                        "max_items": 200,
                        "description": "Validator index / indices"
                    },
                    {
                        "name": "pubkey",
                        "column": "pubkey",
                        "operator": "IN",
                        "type": "string_list",
                        "case": "lower",
                        "max_items": 200,
                        "description": "Validator public key(s)"
                    },
                    {
                        "name": "withdrawal_credentials",
                        "column": "withdrawal_credentials",
                        "operator": "IN",
                        "type": "string_list",
                        "case": "lower",
                        "max_items": 200,
                        "description": "Withdrawal credential value(s)"
                    },
                    {
                        "name": "withdrawal_address",
                        "column": "withdrawal_address",
                        "operator": "IN",
                        "type": "string_list",
                        "case": "lower",
                        "max_items": 200,
                        "description": "20-byte withdrawal address(es) (derived from 0x01/0x02 credentials)"
                    },
                    {
                        "name": "date_from",
                        "column": "date",
                        "operator": ">=",
                        "type": "date",
                        "description": "Inclusive lower bound on date"
                    },
                    {
                        "name": "date_to",
                        "column": "date",
                        "operator": "<=",
                        "type": "date",
                        "description": "Inclusive upper bound on date"
                    }
                ],
                "pagination": {
                    "enabled": true,
                    "default_limit": 100,
                    "max_limit": 5000,
                    "response": "envelope"
                },
                "sort": [
                    {"column": "date", "direction": "DESC"},
                    {"column": "validator_index", "direction": "ASC"}
                ],
                "sortable_fields": [
                    "date",
                    "validator_index",
                    "balance_gno",
                    "consensus_income_amount_gno",
                    "apy",
                    "proposer_reward_total_gno",
                    "proposed_blocks_count"
                ]
            }
        }
    )
}}

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
FROM {{ ref('int_consensus_validators_income_daily') }} i
LEFT JOIN {{ ref('int_consensus_validators_snapshots_daily') }} s
    ON s.date = i.date AND s.validator_index = i.validator_index
LEFT JOIN {{ ref('int_consensus_validators_proposer_rewards_daily') }} p
    ON p.date = i.date AND p.validator_index = i.validator_index
