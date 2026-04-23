{% set start_month = var('start_month', none) %}
{% set end_month = var('end_month', none) %}
{% set validator_index_start = var('validator_index_start', none) %}
{% set validator_index_end = var('validator_index_end', none) %}

{{
    config(
        materialized='incremental',
        incremental_strategy=('append' if start_month else 'delete+insert'),
        engine='ReplacingMergeTree()',
        order_by='(date, validator_index)',
        unique_key='(date, validator_index)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=["production", "consensus", "validators_income"]
    )
}}

-- =============================================================================
-- int_consensus_validators_income_daily — per-(date, validator_index) income fact
-- =============================================================================
--
-- GRAIN: one row per (date, validator_index), including exited / zero-balance.
--
-- COLUMNS emitted downstream:
--   * Raw mass-flow columns (reported exactly as they appear in the source):
--     - deposits_amount_gno            — what the execution layer REQUESTED
--     - withdrawals_amount_gno         — what the beacon chain DID withdraw
--     - consolidation_inflow_gno       — EIP-7251 consolidation credit to this validator
--     - consolidation_outflow_gno      — EIP-7251 consolidation debit from this validator
--   * Spec-derived effective-credit column (NEW in v2):
--     - effective_deposits_credited_gno — how much deposit ACTUALLY hit the balance today
--   * Income + derivatives:
--     - consensus_income_amount_gno, daily_rate, apy
--   * Cumulatives (used for total_income_estimated_gno):
--     - cumulative_deposits_gno        — running sum of effective credits (NOT reported)
--     - cumulative_reported_deposits_gno — running sum of raw reported requests (for audit)
--     - cumulative_withdrawals_gno, cumulative_consolidation_{in,out}flow_gno
--   * total_income_estimated_gno       — uses effective (not reported) cumulatives
--
-- =============================================================================
-- WHY TWO DEPOSIT NUMBERS? (the post-Pectra accounting problem)
-- =============================================================================
-- Pre-Pectra: deposits went through the beacon-chain deposit queue. The crawler's
--   `consensus.deposits` source records deposits as they were CREDITED to validators,
--   one row per 32-GNO chunk. Request == credit, same day.
-- Post-Pectra (EIP-7002 / EIP-7251 — MaxEB): deposits go through execution-layer
--   deposit-request events (`consensus.execution_requests.payload.deposits`). A single
--   execution-request event can request up to MAX_EFFECTIVE_BALANCE_ELECTRA worth of
--   stake (2048 ETH on Ethereum, 2048 GNO on Gnosis) for a single validator. The
--   beacon chain then CREDITS this amount gradually via the `pending_deposits` queue
--   bounded by the activation churn limit. Observed in practice: a single 1767-GNO
--   request shows up in execution_requests on day T, but the validator's balance only
--   grows by ~0.01 GNO that day and ~1 GNO/day thereafter for weeks.
--
-- The naive income formula
--     income = balance_delta - reported_deposit + withdrawals - cons_net
-- produces catastrophic per-day errors post-Pectra: on the REQUEST day it reads
--     income = 0.01 − 1767 ≈ −1767 GNO
-- and on each subsequent drain-day
--     income = +balance_delta  (no deposit event that day; spuriously high)
-- even though over the full drain window income netted out to tens of GNO of real rewards.
--
-- =============================================================================
-- FIX: spec-bounded effective-credit
-- =============================================================================
-- We observe authoritatively: balance[t], balance[t-1], withdrawals, consolidations.
-- We have a noisy signal: reported_deposit (request amount, possibly ≫ credited today).
-- We want: income[t], which must satisfy the ledger identity
--     balance[t] - balance[t-1] = income[t] + deposits_credited[t]
--                               - withdrawals[t] + cons_net[t]
-- and must be bounded above by the consensus spec's maximum possible reward.
--
-- The consensus spec gives a closed-form maximum reward per epoch per validator:
--     base_reward_per_epoch = effective_balance_gwei × BASE_REWARD_FACTOR
--                             / √(total_active_effective_balance_gwei)
-- The total reward a validator can collect in one epoch is bounded by a small multiple
-- of base_reward (attestation + proposer + sync-committee duties summed). Multiplying
-- by EPOCHS_PER_DAY gives expected_max_reward_per_day.
--
-- Gnosis-specific constants (from github.com/gnosischain/configs consensus.yaml):
--     BASE_REWARD_FACTOR        = 25   -- (Ethereum = 64; Gnosis reduced it)
--     SLOTS_PER_EPOCH           = 16   -- (Ethereum = 32)
--     SECONDS_PER_SLOT          = 5    -- (Ethereum = 12)
--     EPOCHS_PER_DAY            = 86_400 / (5 × 16) = 1_080
-- Safety multiplier 3× covers attestation + proposer + sync + any future boosts.
--
-- Algorithm:
--     expected_reward_cap = 3 × effective_balance_gwei × 25 × 1080
--                              / √(total_active_effective_balance_gwei)     -- in gwei
--     effective_deposits_credited = LEAST(
--         reported_deposit,
--         GREATEST(0, balance_delta + withdrawals - cons_net - expected_reward_cap)
--     )
--     consensus_income_amount_gno = balance_delta
--                                 - effective_deposits_credited
--                                 + withdrawals - cons_net
--
-- Key properties:
--   (a) Per-day income is capped at the spec maximum × safety margin → no more -1767
--       GNO spikes on Pectra request days.
--   (b) Cumulative balance-flow over any window is exact (effective credits + income
--       + withdrawals + cons_net = balance change, guaranteed).
--   (c) Queued deposit amounts that haven't credited yet stay in reported_deposit but
--       out of effective_credit until the balance actually moves.
--   (d) Scales with validator size (32 GNO vs 2048 GNO) and with network stake
--       (more validators online → smaller per-validator reward).
--   (e) No magic per-validator constants (contrast the mod-32-rounding trick in
--       int_consensus_validators_per_index_apy_daily, which assumes 32-GNO deposit
--       granularity and fails on MaxEB sub-32 top-ups).
--
-- Prior art / reference: same formula is the core of every validator-yield UI —
-- beaconcha.in's calculator
-- (github.com/gobitfly/eth2-beaconchain-explorer/blob/master/templates/calculator.html)
-- projects future rewards with the same base_reward × √(total_stake)⁻¹ relation.
-- We apply it backwards, as a validity bound on observed income.
--
-- =============================================================================
-- INCREMENTAL STRATEGY
-- =============================================================================
-- Follows int_execution_tokens_balances_native_daily:
--   1. Daily deltas + point-in-time balance read only from the incremental window
--      (start_month/end_month var, or monthly lookback macro).
--   2. On incremental runs, prior-state (last known balance + cumulatives per validator
--      from the previous partition boundary) is pulled from {{ this }} and added to the
--      window-function cumulatives. Keeps lifetime cumulative_*_gno columns correct
--      without scanning full history.
--   3. balance_prev_gno uses lagInFrame within the window; when the row is the first in
--      the window (partition boundary), it falls back to the prior balance from {{ this }}.

{% set range_sql %}
  {% if validator_index_start is not none and validator_index_end is not none %}
    AND validator_index >= {{ validator_index_start }}
    AND validator_index < {{ validator_index_end }}
  {% endif %}
{% endset %}

WITH

-- Per-validator snapshot for the incremental window.
snapshots AS (
    SELECT
        s.date AS date
        ,s.validator_index AS validator_index
        ,s.balance_gwei / POWER(10, 9) AS balance_gno
        ,s.effective_balance_gwei / POWER(10, 9) AS effective_balance_gno
        ,s.effective_balance_gwei AS effective_balance_gwei   -- kept raw for spec-cap math
    FROM {{ ref('int_consensus_validators_snapshots_daily') }} s
    WHERE s.date < today()
    {% if start_month and end_month %}
      AND toStartOfMonth(s.date) >= toDate('{{ start_month }}')
      AND toStartOfMonth(s.date) <= toDate('{{ end_month }}')
    {% else %}
      {{ apply_monthly_incremental_filter('s.date', 'date', 'true', lookback_days=3, filters_sql=range_sql) }}
    {% endif %}
    {% if validator_index_start is not none and validator_index_end is not none %}
      AND s.validator_index >= {{ validator_index_start }}
      AND s.validator_index < {{ validator_index_end }}
    {% endif %}
),

deposits AS (
    SELECT date, validator_index, deposits_amount_gno, deposits_count
    FROM {{ ref('int_consensus_validators_deposits_daily') }}
    WHERE date < today()
    {% if start_month and end_month %}
      AND toStartOfMonth(date) >= toDate('{{ start_month }}')
      AND toStartOfMonth(date) <= toDate('{{ end_month }}')
    {% else %}
      {{ apply_monthly_incremental_filter('date', 'date', 'true', lookback_days=3, filters_sql=range_sql) }}
    {% endif %}
    {% if validator_index_start is not none and validator_index_end is not none %}
      AND validator_index >= {{ validator_index_start }}
      AND validator_index < {{ validator_index_end }}
    {% endif %}
),

withdrawals AS (
    SELECT date, validator_index, withdrawals_amount_gno, withdrawals_count
    FROM {{ ref('int_consensus_validators_withdrawals_daily') }}
    WHERE date < today()
    {% if start_month and end_month %}
      AND toStartOfMonth(date) >= toDate('{{ start_month }}')
      AND toStartOfMonth(date) <= toDate('{{ end_month }}')
    {% else %}
      {{ apply_monthly_incremental_filter('date', 'date', 'true', lookback_days=3, filters_sql=range_sql) }}
    {% endif %}
    {% if validator_index_start is not none and validator_index_end is not none %}
      AND validator_index >= {{ validator_index_start }}
      AND validator_index < {{ validator_index_end }}
    {% endif %}
),

consolidations AS (
    SELECT
        date
        ,validator_index
        ,SUMIf(transferred_amount_gno, role = 'target') AS consolidation_inflow_gno
        ,SUMIf(transferred_amount_gno, role = 'source') AS consolidation_outflow_gno
    FROM {{ ref('int_consensus_validators_consolidations_daily') }}
    WHERE 1=1
    {% if start_month and end_month %}
      AND toStartOfMonth(date) >= toDate('{{ start_month }}')
      AND toStartOfMonth(date) <= toDate('{{ end_month }}')
    {% endif %}
    {% if validator_index_start is not none and validator_index_end is not none %}
      AND validator_index >= {{ validator_index_start }}
      AND validator_index < {{ validator_index_end }}
    {% endif %}
    GROUP BY 1, 2
),

-- Daily network-wide active effective balance (the denominator of the spec reward
-- formula). int_consensus_validators_balances_daily stores effective_balance already
-- summed across all validators (in GNO). We convert back to gwei so the √ in the spec
-- formula sees the same units the consensus spec does.
network_state AS (
    SELECT
        toStartOfDay(date) AS date
        ,effective_balance AS network_effective_balance_gno
    FROM {{ ref('int_consensus_validators_balances_daily') }}
),

-- Join every per-validator row with the day's network state so each row carries
-- both its own effective_balance and the network total; the spec cap can then be
-- computed row-by-row without a separate join later.
--
-- DATA-QUALITY NOTES (2026-04 audit via cerebro-dev MCP):
--
-- 1. consensus.validators DOES NOT have day-level gaps. An earlier version of this
--    comment claimed a crawler outage 2025-12-10 → 2025-12-28; that was incorrect.
--    Day-gap audit over full history: the only gap is day 1 (genesis). What the
--    earlier debug actually observed was unmerged SharedReplacingMergeTree
--    DUPLICATE rows in the raw source (ratios up to 2.9× normal on
--    2025-12-{04,12,13,14,15,16}). Those duplicates are filtered at the staging
--    layer — stg_consensus__validators{,_all} both apply FINAL — so the dedup
--    happens before this model reads anything. See
--    tests/consensus_raw_validators_no_unmerged_dup.sql for the ops monitor.
--
-- 2. The real source of per-validator negative-income spikes observed on Validator
--    Explorer charts was a BUG in int_consensus_validators_consolidations_daily
--    (fixed 2026-04, v4). That model's unique_key=(date, validator_index, role) on
--    ReplacingMergeTree collapsed N target rows down to 1 when N legacy 0x01 sources
--    consolidated into the same 0x02 target on the same application day (common).
--    Result: target's consolidation_inflow_gno was under-credited by (N-1) × 32 GNO;
--    the spec-cap formula below then absorbed the missing credit into
--    effective_deposits_credited_gno, producing per-validator income that looked
--    hugely negative. Aggregate ledger still balanced (eff_deposits absorbed
--    exactly what was missing from cons_in) — so the bug was invisible at network
--    level, only broke per-credential and per-validator attribution. The v4 fix
--    aggregates at the unique-key grain so target rows survive dedup. Verified by
--    tests/consensus_consolidations_mass_balance.sql.
daily_raw AS (
    SELECT
        s.date AS date
        ,s.validator_index AS validator_index
        ,s.balance_gno AS balance_gno
        ,s.effective_balance_gno AS effective_balance_gno
        ,s.effective_balance_gwei AS effective_balance_gwei
        ,COALESCE(d.deposits_amount_gno, 0) AS deposits_amount_gno
        ,COALESCE(d.deposits_count, 0) AS deposits_count
        ,COALESCE(w.withdrawals_amount_gno, 0) AS withdrawals_amount_gno
        ,COALESCE(w.withdrawals_count, 0) AS withdrawals_count
        ,COALESCE(c.consolidation_inflow_gno, 0) AS consolidation_inflow_gno
        ,COALESCE(c.consolidation_outflow_gno, 0) AS consolidation_outflow_gno
        ,n.network_effective_balance_gno AS network_effective_balance_gno
    FROM snapshots s
    LEFT JOIN deposits d ON d.date = s.date AND d.validator_index = s.validator_index
    LEFT JOIN withdrawals w ON w.date = s.date AND w.validator_index = s.validator_index
    LEFT JOIN consolidations c ON c.date = s.date AND c.validator_index = s.validator_index
    INNER JOIN network_state n ON n.date = s.date
),

{% if is_incremental() %}
current_partition AS (
    SELECT max(toDate(date)) AS max_date
    FROM {{ this }}
    WHERE 1=1
    {{ range_sql }}
),

prev_state AS (
    -- Last known balance + cumulatives per validator at the prior partition boundary.
    -- Used to (a) seed balance_prev_gno for the first window-day of each validator and
    -- (b) add to the windowed cumulative sums so lifetime totals stay correct across
    -- incremental runs.
    SELECT
        t1.validator_index AS validator_index
        ,t1.balance_gno AS balance_prev_from_this
        ,t1.cumulative_deposits_gno AS prev_cumulative_deposits_gno
        ,t1.cumulative_reported_deposits_gno AS prev_cumulative_reported_deposits_gno
        ,t1.cumulative_withdrawals_gno AS prev_cumulative_withdrawals_gno
        ,t1.cumulative_consolidation_inflow_gno AS prev_cumulative_consolidation_inflow_gno
        ,t1.cumulative_consolidation_outflow_gno AS prev_cumulative_consolidation_outflow_gno
    FROM {{ this }} t1
    CROSS JOIN current_partition cp
    WHERE toDate(t1.date) = cp.max_date
    {{ range_sql }}
),
{% endif %}

-- Apply the beacon-spec reward cap and the mass-balance constraint per row.
--   balance_delta := balance_gno - balance_prev_gno
--   observed_net_inflow := balance_delta + withdrawals - cons_inflow + cons_outflow
--       (this is the total mass change attributable to deposits+rewards on this day)
--   expected_reward_cap := 3 × eb_gwei × 25 × 1080 / √(net_eb_gwei)  ÷ 1e9  (→ GNO)
--   effective_credit := LEAST(reported_deposit, GREATEST(0, observed_net_inflow − expected_reward_cap))
--   income := balance_delta − effective_credit + withdrawals − cons_net
scored AS (
    SELECT
        r.date AS date
        ,r.validator_index AS validator_index
        ,r.balance_gno AS balance_gno
        ,r.effective_balance_gno AS effective_balance_gno
        ,r.deposits_amount_gno AS deposits_amount_gno
        ,r.deposits_count AS deposits_count
        ,r.withdrawals_amount_gno AS withdrawals_amount_gno
        ,r.withdrawals_count AS withdrawals_count
        ,r.consolidation_inflow_gno AS consolidation_inflow_gno
        ,r.consolidation_outflow_gno AS consolidation_outflow_gno
        ,COALESCE(
            lagInFrame(toNullable(r.balance_gno), 1, NULL) OVER (
                PARTITION BY r.validator_index ORDER BY r.date
                ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
            )
            {% if is_incremental() %}
            , p.balance_prev_from_this
            {% endif %}
            , 0
        ) AS balance_prev_gno
        -- Consensus-spec reward cap, in GNO. 3× covers attestation + proposer + sync duties.
        -- Guarded SQRT: NULLIF 0 prevents div-by-zero on genesis / empty-network days.
        ,3.0
         * toFloat64(r.effective_balance_gwei) * 25 * 1080
         / NULLIF(SQRT(toFloat64(r.network_effective_balance_gno) * POWER(10, 9)), 0)
         / POWER(10, 9)
         AS expected_reward_cap_gno
    FROM daily_raw r
    {% if is_incremental() %}
    LEFT JOIN prev_state p ON p.validator_index = r.validator_index
    {% endif %}
),

-- Compute effective deposit credit from the spec bound only, then income.
-- Doing this in a separate CTE so we can reference `balance_prev_gno` in the formula
-- (window-function output can't be reused mid-SELECT in ClickHouse).
--
-- Important design choice — we DO NOT additionally cap effective_credit at
-- reported_deposit. Rationale: post-Pectra the reported-request amount is reported
-- once (on the request-submission day) but the actual balance credit lands N days
-- later when the beacon-chain churn queue processes it. On those drain days,
-- reported_deposit is 0 but balance jumps by hundreds of GNO, and capping at
-- reported would falsely attribute the jump to income. The spec cap alone is
-- enough: any day's balance movement exceeding expected_reward_cap CANNOT be
-- consensus reward (the spec doesn't permit it), so it must be deposit /
-- consolidation activity — queue-drain credits included.
-- Edge cases handled correctly:
--   * pure reward day            : balance_delta ≤ cap → effective_credit = 0 → income ≈ balance_delta ✓
--   * reported-but-not-credited   : balance_delta ≈ 0, reported ≫ 0 → effective_credit = 0 → income ≈ 0 ✓
--   * queue-drain day             : balance_delta ≫ 0, reported = 0 → effective_credit ≈ balance_delta → income = cap ✓
--   * slashing                    : balance_delta < 0 → effective_credit = 0 → income = balance_delta ✓ (negative, real)
--   * consolidation source        : balance_delta = −X, cons_outflow = +X → net = 0 → both income and credit = 0 ✓
--   * consolidation target        : balance_delta = +X, cons_inflow = +X → net = 0 → both income and credit = 0 ✓
with_credit AS (
    SELECT
        *
        ,GREATEST(
            0,
            (balance_gno - balance_prev_gno)
            + withdrawals_amount_gno
            - consolidation_inflow_gno + consolidation_outflow_gno
            - expected_reward_cap_gno
        ) AS effective_deposits_credited_gno
    FROM scored
),

-- Window-accumulate running cumulatives, continuing from prev_state when incremental.
windowed AS (
    SELECT
        *
        ,SUM(effective_deposits_credited_gno) OVER w_cum
            {% if is_incremental() %} + COALESCE(p.prev_cumulative_deposits_gno, 0) {% endif %}
            AS cumulative_deposits_gno
        ,SUM(deposits_amount_gno) OVER w_cum
            {% if is_incremental() %} + COALESCE(p.prev_cumulative_reported_deposits_gno, 0) {% endif %}
            AS cumulative_reported_deposits_gno
        ,SUM(withdrawals_amount_gno) OVER w_cum
            {% if is_incremental() %} + COALESCE(p.prev_cumulative_withdrawals_gno, 0) {% endif %}
            AS cumulative_withdrawals_gno
        ,SUM(consolidation_inflow_gno) OVER w_cum
            {% if is_incremental() %} + COALESCE(p.prev_cumulative_consolidation_inflow_gno, 0) {% endif %}
            AS cumulative_consolidation_inflow_gno
        ,SUM(consolidation_outflow_gno) OVER w_cum
            {% if is_incremental() %} + COALESCE(p.prev_cumulative_consolidation_outflow_gno, 0) {% endif %}
            AS cumulative_consolidation_outflow_gno
    FROM with_credit wc
    {% if is_incremental() %}
    LEFT JOIN prev_state p ON p.validator_index = wc.validator_index
    {% endif %}
    WINDOW
        w_cum AS (PARTITION BY validator_index ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)

SELECT
    date
    ,validator_index
    ,balance_gno
    ,balance_prev_gno
    ,effective_balance_gno
    ,deposits_amount_gno                              -- RAW reported request amount
    ,effective_deposits_credited_gno                   -- spec-bounded actual credit
    ,deposits_count
    ,withdrawals_amount_gno
    ,withdrawals_count
    ,consolidation_inflow_gno
    ,consolidation_outflow_gno
    ,expected_reward_cap_gno                           -- useful for debugging / APY checks
    -- Income uses effective credit, not reported. Guaranteed ≤ expected_reward_cap_gno + slashing_losses.
    ,balance_gno - balance_prev_gno
        - effective_deposits_credited_gno
        + withdrawals_amount_gno
        - consolidation_inflow_gno + consolidation_outflow_gno AS consensus_income_amount_gno
    ,(balance_gno - balance_prev_gno
        - effective_deposits_credited_gno
        + withdrawals_amount_gno
        - consolidation_inflow_gno + consolidation_outflow_gno)
        / NULLIF(balance_prev_gno + effective_deposits_credited_gno + consolidation_inflow_gno, 0) AS daily_rate
    ,ROUND((POWER(1 + COALESCE(
        (balance_gno - balance_prev_gno
            - effective_deposits_credited_gno
            + withdrawals_amount_gno
            - consolidation_inflow_gno + consolidation_outflow_gno)
            / NULLIF(balance_prev_gno + effective_deposits_credited_gno + consolidation_inflow_gno, 0),
        0), 365) - 1) * 100, 2) AS apy
    ,cumulative_deposits_gno                           -- cumulative EFFECTIVE credits
    ,cumulative_reported_deposits_gno                  -- cumulative reported (for audit)
    ,cumulative_withdrawals_gno
    ,cumulative_consolidation_inflow_gno
    ,cumulative_consolidation_outflow_gno
    -- total_income = balance + withdrawn − credited − cons_net. Uses effective.
    ,balance_gno + cumulative_withdrawals_gno - cumulative_deposits_gno
        - cumulative_consolidation_inflow_gno + cumulative_consolidation_outflow_gno AS total_income_estimated_gno
FROM windowed
