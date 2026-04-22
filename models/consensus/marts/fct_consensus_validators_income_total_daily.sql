{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='(date)',
        tags=["production", "consensus", "fct:validators_income", "granularity:daily"]
    )
}}

-- Materialized as a physical table (not a view) so the dashboard API query
-- returns in milliseconds. Source fact has 200M+ rows; running SUM GROUP BY on
-- every dashboard request would 30s-timeout Vercel functions. Rebuild cost is
-- trivial (1568 output rows) and hooks naturally into the dbt DAG: each time
-- the upstream int_consensus_validators_income_daily runs, this rebuilds too.

-- Network-wide daily consensus income in GNO (sum across every validator, including
-- exited / zero-balance). Derived from int_consensus_validators_income_daily, which
-- now spec-bounds income per-validator (see the block comment at the top of that model
-- for the beacon reward-cap derivation), so no aggregate-level outlier filter is
-- required — the input is already clean.
SELECT
    date
    ,SUM(consensus_income_amount_gno) AS income_gno
FROM {{ ref('int_consensus_validators_income_daily') }}
GROUP BY date
ORDER BY date
