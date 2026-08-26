{{
  config(
    materialized='view',
    tags=['production','governance','tier2','api:governance_delegation_activity','granularity:monthly']
  )
}}

-- Monthly DelegateRegistry activity in long format (date, metric, value).
-- Churn classification is the canonical definition shared with the governance
-- mini-app: row-numbered per (chain_id, delegator) ordered by
-- (block_number, log_index) -- NOT block_time, which ties within one tx when a
-- delegator clears and re-sets (same rationale as
-- int_governance_current_delegations). First set = new_delegator, later set =
-- repointed. Chains are collapsed (both registries summed); per-chain depth
-- stays in the mini-app. `active_delegators` is the running net (sets minus
-- clears) through each month. `value` is Int64 -- net_change can be negative.

WITH events AS (
    SELECT
        toStartOfMonth(block_time) AS date,
        action,
        row_number() OVER (
            PARTITION BY chain_id, delegator
            ORDER BY block_number, log_index
        ) AS rn
    FROM {{ ref('stg_governance__snapshot_delegations') }}
),

monthly AS (
    SELECT
        date,
        countIf(action = 'set')                        AS set_events,
        countIf(action = 'clear')                      AS clear_events,
        countIf(action = 'set') - countIf(action = 'clear') AS net_change,
        countIf(action = 'set' AND rn = 1)             AS new_delegators,
        countIf(action = 'set' AND rn > 1)             AS repointed
    FROM events
    GROUP BY date
),

with_cumulative AS (
    SELECT
        date,
        set_events,
        clear_events,
        net_change,
        new_delegators,
        repointed,
        sum(net_change) OVER (ORDER BY date) AS active_delegators
    FROM monthly
)

SELECT date, metric, value
FROM with_cumulative
ARRAY JOIN
    ['set_events', 'clear_events', 'net_change', 'new_delegators', 'repointed', 'active_delegators'] AS metric,
    [toInt64(set_events), toInt64(clear_events), toInt64(net_change), toInt64(new_delegators), toInt64(repointed), toInt64(active_delegators)] AS value
ORDER BY date, metric
