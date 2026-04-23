{% set start_month = var('start_month', none) %}
{% set end_month = var('end_month', none) %}

{{
    config(
        materialized='incremental',
        incremental_strategy=('append' if start_month else 'delete+insert'),
        engine='ReplacingMergeTree()',
        order_by='(withdrawal_credentials, date)',
        unique_key='(date, withdrawal_credentials)',
        partition_by='toStartOfMonth(date)',
        tags=["production", "consensus", "fct:validators_explorer", "granularity:daily"]
    )
}}

-- Per-operator (withdrawal_credentials) daily roll-up feeding the five daily charts on
-- the Validator Explorer tab. Materialised as a physical table (sorted on
-- withdrawal_credentials) so the API's `WHERE withdrawal_credentials = 'x'` filter
-- prunes at read time via the primary index. Rebuilt incrementally month-by-month to
-- stay under the cluster's 10.8 GiB memory cap during backfill.
--
-- v3: drop post-exit rows. Without this, an exited validator keeps emitting
-- zero-income/zero-balance rows from int_consensus_validators_income_daily
-- forever, which the UI renders as a flat tail on the credential's balance line
-- after the validator has really left the set. We join time_helpers + status_latest
-- and filter `date > exit_date` here so every chart downstream stops at the exit.

WITH time_helpers AS (
    SELECT genesis_time_unix, seconds_per_slot, slots_per_epoch
    FROM {{ ref('stg_consensus__time_helpers') }}
    LIMIT 1
)

SELECT
    i.date AS date
    ,wl.withdrawal_credentials AS withdrawal_credentials
    ,SUM(i.balance_gno) AS balance_gno
    ,SUM(i.effective_balance_gno) AS effective_balance_gno
    ,SUM(i.consensus_income_amount_gno) AS consensus_income_amount_gno
    -- Balance-weighted APY (drops apy=0 idle validators); see api_consensus_validators_apy_mean_daily
    -- for the rationale. Falls back to simple mean when balance_prev sum is 0.
    ,IF(
        SUMIf(i.balance_prev_gno, i.apy > 0 AND i.apy < 200 AND i.balance_prev_gno > 0) > 0,
        SUMIf(i.apy * i.balance_prev_gno, i.apy > 0 AND i.apy < 200 AND i.balance_prev_gno > 0)
          / SUMIf(i.balance_prev_gno, i.apy > 0 AND i.apy < 200 AND i.balance_prev_gno > 0),
        0
    ) AS apy
    ,SUM(i.deposits_amount_gno) AS deposits_amount_gno
    ,SUM(i.withdrawals_amount_gno) AS withdrawals_amount_gno
    ,SUM(i.consolidation_inflow_gno) AS consolidation_inflow_gno
    ,SUM(i.consolidation_outflow_gno) AS consolidation_outflow_gno
    ,SUM(COALESCE(p.proposer_reward_total_gno, 0)) AS proposer_reward_total_gno
    ,SUM(COALESCE(p.proposed_blocks_count, 0)) AS proposed_blocks_count
    -- v2 (2026-04): count of distinct active validators under the credential on the date.
    -- Dashboard uses this to decide whether to show quantile bands (N>1) or collapse to
    -- the rolling-median line (N=1) on the per-credential APY chart.
    ,uniqExact(i.validator_index) AS validator_count_active
FROM {{ ref('int_consensus_validators_income_daily') }} i
INNER JOIN {{ ref('fct_consensus_validators_status_latest') }} wl
    ON wl.validator_index = i.validator_index
CROSS JOIN time_helpers th
LEFT JOIN {{ ref('int_consensus_validators_proposer_rewards_daily') }} p
    ON p.date = i.date AND p.validator_index = i.validator_index
WHERE 1=1
{% if start_month and end_month %}
    AND toStartOfMonth(i.date) >= toDate('{{ start_month }}')
    AND toStartOfMonth(i.date) <= toDate('{{ end_month }}')
{% else %}
    {{ apply_monthly_incremental_filter('i.date', 'date', 'true', lookback_days=3) }}
{% endif %}
    -- Drop rows past a validator's exit_date. FAR_FUTURE_EPOCH (2^64-1) means
    -- "not exited" and is always kept.
    AND (
        wl.exit_epoch >= toUInt64(18446744073709551615)
        OR i.date <= toDate(toDateTime(th.genesis_time_unix + wl.exit_epoch * th.slots_per_epoch * th.seconds_per_slot))
    )
GROUP BY i.date, wl.withdrawal_credentials
