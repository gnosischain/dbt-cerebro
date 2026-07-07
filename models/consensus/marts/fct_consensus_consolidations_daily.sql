{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='(date, role)',
        tags=["production", "consensus", "fct:consolidations", "granularity:daily"]
    )
}}

-- Materialized as a table so the dashboard API returns fast. Source is small
-- (~5k rows) so rebuild is instant.

-- Daily EIP-7251 consolidation event counts + transferred amounts, stacked by role
-- ('self', 'source', 'target'). See int_consensus_validators_consolidations_daily for
-- the application-day inference and amount derivation.
-- NOTE: int_consensus_validators_consolidations_daily's "_gno" columns are actually
-- mGNO-denominated (Gnosis Beacon Chain mirrors Ethereum's 32-unit-per-validator
-- convention; 32 mGNO = 1 real GNO). Divided by 32 below to convert to real GNO.
SELECT
    date
    ,role
    ,SUM(cnt) AS cnt
    ,SUM(transferred_amount_gno) / 32 AS transferred_amount_gno
FROM {{ ref('int_consensus_validators_consolidations_daily') }}
GROUP BY date, role
ORDER BY date, role
