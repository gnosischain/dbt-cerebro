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
SELECT
    date
    ,role
    ,SUM(cnt) AS cnt
    ,SUM(transferred_amount_gno) AS transferred_amount_gno
FROM {{ ref('int_consensus_validators_consolidations_daily') }}
GROUP BY date, role
ORDER BY date, role
