{{
    config(
        materialized='view',
        tags=["production", "consensus", 'tier1', 'api:validators_performance', 'granularity:latest'],
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
                        "description": "20-byte withdrawal address(es)"
                    }
                ],
                "pagination": {
                    "enabled": true,
                    "default_limit": 100,
                    "max_limit": 5000,
                    "response": "envelope"
                },
                "sort": [
                    {"column": "validator_index", "direction": "ASC"}
                ],
                "sortable_fields": [
                    "validator_index",
                    "balance_gno",
                    "effective_balance_gno",
                    "total_income_estimated_gno",
                    "consensus_income_amount_30d_gno",
                    "proposer_reward_total_30d_gno",
                    "proposed_blocks_count_lifetime"
                ]
            }
        }
    )
}}

WITH

latest_income AS (
    SELECT
        i.*
        ,s.status
        ,s.pubkey
        ,s.withdrawal_credentials
        ,s.withdrawal_address
    FROM {{ ref('int_consensus_validators_income_daily') }} i
    LEFT JOIN {{ ref('int_consensus_validators_snapshots_daily') }} s
        ON s.date = i.date AND s.validator_index = i.validator_index
    WHERE i.date = (SELECT MAX(date) FROM {{ ref('int_consensus_validators_income_daily') }})
),

income_30d AS (
    SELECT
        validator_index
        ,SUM(consensus_income_amount_gno) AS consensus_income_amount_30d_gno
    FROM {{ ref('int_consensus_validators_income_daily') }}
    WHERE date >= (SELECT MAX(date) FROM {{ ref('int_consensus_validators_income_daily') }}) - INTERVAL 30 DAY
    GROUP BY validator_index
),

proposer_30d AS (
    SELECT
        validator_index
        ,SUM(proposer_reward_total_gno) AS proposer_reward_total_30d_gno
    FROM {{ ref('int_consensus_validators_proposer_rewards_daily') }}
    WHERE date >= (SELECT MAX(date) FROM {{ ref('int_consensus_validators_income_daily') }}) - INTERVAL 30 DAY
    GROUP BY validator_index
),

proposer_lifetime AS (
    SELECT
        validator_index
        ,SUM(proposed_blocks_count) AS proposed_blocks_count_lifetime
        ,SUM(proposer_reward_total_gno) AS proposer_reward_total_lifetime_gno
    FROM {{ ref('int_consensus_validators_proposer_rewards_daily') }}
    GROUP BY validator_index
)

SELECT
    l.date AS latest_date
    ,l.validator_index AS validator_index
    ,l.status AS status
    ,l.pubkey AS pubkey
    ,l.withdrawal_credentials AS withdrawal_credentials
    ,l.withdrawal_address AS withdrawal_address
    ,l.balance_gno AS balance_gno
    ,l.effective_balance_gno AS effective_balance_gno
    ,l.cumulative_deposits_gno AS cumulative_deposits_gno
    ,l.cumulative_withdrawals_gno AS cumulative_withdrawals_gno
    ,l.cumulative_consolidation_inflow_gno AS cumulative_consolidation_inflow_gno
    ,l.cumulative_consolidation_outflow_gno AS cumulative_consolidation_outflow_gno
    ,l.total_income_estimated_gno AS total_income_estimated_gno
    ,COALESCE(i30.consensus_income_amount_30d_gno, 0) AS consensus_income_amount_30d_gno
    ,COALESCE(p30.proposer_reward_total_30d_gno, 0) AS proposer_reward_total_30d_gno
    ,COALESCE(pl.proposed_blocks_count_lifetime, 0) AS proposed_blocks_count_lifetime
    ,COALESCE(pl.proposer_reward_total_lifetime_gno, 0) AS proposer_reward_total_lifetime_gno
FROM latest_income l
LEFT JOIN income_30d i30 ON i30.validator_index = l.validator_index
LEFT JOIN proposer_30d p30 ON p30.validator_index = l.validator_index
LEFT JOIN proposer_lifetime pl ON pl.validator_index = l.validator_index
