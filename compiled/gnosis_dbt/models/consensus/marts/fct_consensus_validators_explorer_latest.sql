

-- Per-operator (withdrawal_credentials) latest-snapshot roll-up. Feeds the
-- KPI cards on the Validator Explorer tab. Reads the int tables directly
-- (not the composite api_consensus_validators_performance_latest view) so
-- that the outer GROUP BY across 558k validators doesn't OOM the cluster.
--
-- v3 additions:
--   * lifecycle counts: active_count / exited_count / pending_count / slashed_count
--   * balance-weighted 30-day APY per credential (apy_30d, in %)
--   * driver switched to status_latest via LEFT JOIN so validators that have
--     no income rows yet (freshly activated, or never activated) still count
--     toward validator_count and populate status fields.

WITH

-- Per-validator identity / lifecycle / slashing source of truth.
wl AS (
    SELECT
        validator_index
        ,withdrawal_credentials
        ,status
        ,slashed
    FROM `dbt`.`fct_consensus_validators_status_latest`
),

-- Latest-day income per validator (end-of-period balances + cumulatives).
latest_income AS (
    SELECT
        i.validator_index
        ,i.balance_gno
        ,i.effective_balance_gno
        ,i.cumulative_deposits_gno
        ,i.cumulative_withdrawals_gno
        ,i.total_income_estimated_gno
    FROM `dbt`.`int_consensus_validators_income_daily` i
    WHERE i.date = (SELECT MAX(date) FROM `dbt`.`int_consensus_validators_income_daily`)
),

-- Last 30 days of income per validator.
income_30d AS (
    SELECT
        validator_index
        ,SUM(consensus_income_amount_gno) AS consensus_income_amount_30d_gno
    FROM `dbt`.`int_consensus_validators_income_daily`
    WHERE date >= (SELECT MAX(date) FROM `dbt`.`int_consensus_validators_income_daily`) - INTERVAL 30 DAY
    GROUP BY validator_index
),

-- Last 30 days of proposer rewards per validator.
proposer_30d AS (
    SELECT
        validator_index
        ,SUM(proposer_reward_total_gno) AS proposer_reward_total_30d_gno
    FROM `dbt`.`int_consensus_validators_proposer_rewards_daily`
    WHERE date >= (SELECT MAX(date) FROM `dbt`.`int_consensus_validators_income_daily`) - INTERVAL 30 DAY
    GROUP BY validator_index
),

-- Lifetime proposer counts per validator.
proposer_lifetime AS (
    SELECT
        validator_index
        ,SUM(proposed_blocks_count) AS proposed_blocks_count_lifetime
        ,SUM(proposer_reward_total_gno) AS proposer_reward_total_lifetime_gno
    FROM `dbt`.`int_consensus_validators_proposer_rewards_daily`
    GROUP BY validator_index
),

-- Credential-level balance-weighted 30-day APY. Computed once per credential
-- so the KPI card is a single cheap lookup.
--   apy_30d = SUM(income_30d) / AVG(effective_balance_30d) * 365 * 100
-- Only validators with effective_balance > 0 on some day in the window
-- contribute — idle/empty slots do not drag the mean down.
credential_apy_30d AS (
    SELECT
        w.withdrawal_credentials
        ,SUM(i.consensus_income_amount_gno)
            / NULLIF(AVG(i.effective_balance_gno), 0)
            * 365 * 100 AS apy_30d
    FROM `dbt`.`int_consensus_validators_income_daily` i
    INNER JOIN wl w ON w.validator_index = i.validator_index
    WHERE i.date >= (SELECT MAX(date) FROM `dbt`.`int_consensus_validators_income_daily`) - INTERVAL 30 DAY
      AND i.effective_balance_gno > 0
    GROUP BY w.withdrawal_credentials
)

-- Join everything first in an inner subquery so the outer GROUP BY sees a
-- flat column set. ClickHouse doesn't like aggregating on a qualified column
-- (`wl.withdrawal_credentials`) when Nested-style identifiers are in play.
SELECT
    withdrawal_credentials
    ,COUNT(*) AS validator_count
    -- Lifecycle counts (v3). Gnosis Chain currently emits:
    --   active_ongoing          — validator online, attesting
    --   pending_initialized     — deposit submitted, awaiting activation
    --   withdrawal_possible     — exited, balance not yet swept
    --   withdrawal_done         — fully exited, balance withdrawn (includes slashed)
    -- We also accept the Ethereum-mainnet-style values (active_exiting,
    -- active_slashed, exited_*) defensively in case the upstream crawler
    -- starts emitting them.
    ,SUM(IF(status IN ('active_ongoing','active_exiting','active_slashed'), 1, 0)) AS active_count
    ,SUM(IF(status IN ('withdrawal_done','withdrawal_possible') OR status LIKE 'exited_%', 1, 0)) AS exited_count
    ,SUM(IF(status LIKE 'pending_%', 1, 0)) AS pending_count
    ,SUM(IF(slashed,                 1, 0)) AS slashed_count
    -- balance roll-ups
    ,SUM(balance_gno)            AS balance_gno
    ,SUM(effective_balance_gno)  AS effective_balance_gno
    ,SUM(cumulative_deposits_gno)    AS cumulative_deposits_gno
    ,SUM(cumulative_withdrawals_gno) AS cumulative_withdrawals_gno
    ,SUM(total_income_estimated_gno) AS total_income_estimated_gno
    ,SUM(consensus_income_amount_30d_gno) AS consensus_income_amount_30d_gno
    ,SUM(proposer_reward_total_30d_gno)   AS proposer_reward_total_30d_gno
    ,SUM(proposed_blocks_count_lifetime)  AS proposed_blocks_count_lifetime
    ,SUM(proposer_reward_total_lifetime_gno) AS proposer_reward_total_lifetime_gno
    -- APY is pre-grouped per credential; every row in this group has the
    -- same value, so MAX() is just a collapse.
    ,MAX(apy_30d) AS apy_30d
FROM (
    SELECT
        wl.withdrawal_credentials AS withdrawal_credentials
        ,wl.status  AS status
        ,wl.slashed AS slashed
        ,COALESCE(li.balance_gno, 0)            AS balance_gno
        ,COALESCE(li.effective_balance_gno, 0)  AS effective_balance_gno
        ,COALESCE(li.cumulative_deposits_gno, 0)    AS cumulative_deposits_gno
        ,COALESCE(li.cumulative_withdrawals_gno, 0) AS cumulative_withdrawals_gno
        ,COALESCE(li.total_income_estimated_gno, 0) AS total_income_estimated_gno
        ,COALESCE(i30.consensus_income_amount_30d_gno, 0) AS consensus_income_amount_30d_gno
        ,COALESCE(p30.proposer_reward_total_30d_gno, 0)   AS proposer_reward_total_30d_gno
        ,COALESCE(pl.proposed_blocks_count_lifetime, 0)   AS proposed_blocks_count_lifetime
        ,COALESCE(pl.proposer_reward_total_lifetime_gno, 0) AS proposer_reward_total_lifetime_gno
        ,COALESCE(ap.apy_30d, 0) AS apy_30d
    FROM wl
    LEFT JOIN latest_income li       ON li.validator_index = wl.validator_index
    LEFT JOIN income_30d i30          ON i30.validator_index = wl.validator_index
    LEFT JOIN proposer_30d p30        ON p30.validator_index = wl.validator_index
    LEFT JOIN proposer_lifetime pl    ON pl.validator_index = wl.validator_index
    LEFT JOIN credential_apy_30d ap   ON ap.withdrawal_credentials = wl.withdrawal_credentials
) joined
GROUP BY withdrawal_credentials