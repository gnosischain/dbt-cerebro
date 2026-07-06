{{
    config(
        materialized='view',
        tags=["production", "consensus", 'tier1', 'api:validators_balances', 'granularity:daily']
    )
}}

-- NOTE int_consensus_validators_balances_daily's balance/effective_balance are
-- actually mGNO-denominated (Gnosis Beacon Chain mirrors Ethereum's 32-unit-per-
-- validator convention; 32 mGNO = 1 real GNO). Same source api_consensus_staked_daily
-- and fct_consensus_info_latest already correctly divide by 32; this model did not.
SELECT
    date
    ,label
    ,value
FROM (
    SELECT
        date
        ,'balance' AS label
        ,balance / 32 AS value
    FROM {{ ref('int_consensus_validators_balances_daily') }}

    UNION ALL

    SELECT
        date
        ,'eff. balance' AS label
        ,effective_balance / 32 AS value
    FROM {{ ref('int_consensus_validators_balances_daily') }}
)
ORDER BY date, label