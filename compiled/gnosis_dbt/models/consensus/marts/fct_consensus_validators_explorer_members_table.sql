

-- Members table for the Validator Explorer tab: one row per validator under
-- the selected withdrawal credential. Reads int tables directly (cheap: 558k
-- status rows, 558k latest-day income rows) rather than routing through
-- api_consensus_validators_performance_latest.
--
-- v3 additions:
--   * Lifecycle columns: activation_epoch, exit_epoch, withdrawable_epoch,
--     slashed, plus derived activation_date / exit_date / withdrawable_date.
--   * Per-validator balance-weighted 30-day APY (apy_30d).
--   * Driver is status_latest with LEFT JOIN into income/rewards so a validator
--     that is still pending activation (no income rows yet) still appears as
--     a row with status='pending_*' and zeros everywhere else — no silent
--     row-drops on the join.
--
-- FAR_FUTURE_EPOCH sentinel (2^64 - 1 = 18446744073709551615) is emitted by the
-- beacon state for epochs that have not happened yet (e.g. a validator that
-- has not exited). We NULL these out before converting to timestamps so the
-- UI renders a blank instead of a year in 292 billion AD.

WITH

-- Genesis anchor + slot timing for epoch→timestamp conversion. Single-row view.
time_helpers AS (
    SELECT
        genesis_time_unix
        ,seconds_per_slot
        ,slots_per_epoch
    FROM `dbt`.`stg_consensus__time_helpers`
    LIMIT 1
),

latest_income AS (
    SELECT
        i.validator_index
        ,i.date AS latest_date
        ,i.balance_gno
        ,i.effective_balance_gno
        ,i.total_income_estimated_gno
    FROM `dbt`.`int_consensus_validators_income_daily` i
    WHERE i.date = (SELECT MAX(date) FROM `dbt`.`int_consensus_validators_income_daily`)
),

income_30d AS (
    SELECT
        validator_index
        ,SUM(consensus_income_amount_gno) AS consensus_income_amount_30d_gno
        -- Per-validator balance-weighted 30d APY: SUM(income)/AVG(eff_balance)*365*100.
        -- Rows with zero effective balance are excluded so idle slots don't pull the mean.
        ,SUMIf(consensus_income_amount_gno, effective_balance_gno > 0)
            / NULLIF(AVGIf(effective_balance_gno, effective_balance_gno > 0), 0)
            * 365 * 100 AS apy_30d
    FROM `dbt`.`int_consensus_validators_income_daily`
    WHERE date >= (SELECT MAX(date) FROM `dbt`.`int_consensus_validators_income_daily`) - INTERVAL 30 DAY
    GROUP BY validator_index
),

proposer_30d AS (
    SELECT
        validator_index
        ,SUM(proposer_reward_total_gno) AS proposer_reward_total_30d_gno
    FROM `dbt`.`int_consensus_validators_proposer_rewards_daily`
    WHERE date >= (SELECT MAX(date) FROM `dbt`.`int_consensus_validators_income_daily`) - INTERVAL 30 DAY
    GROUP BY validator_index
),

proposer_lifetime AS (
    SELECT
        validator_index
        ,SUM(proposed_blocks_count) AS proposed_blocks_count_lifetime
        ,SUM(proposer_reward_total_gno) AS proposer_reward_total_lifetime_gno
    FROM `dbt`.`int_consensus_validators_proposer_rewards_daily`
    GROUP BY validator_index
)

SELECT
    s.withdrawal_credentials AS withdrawal_credentials
    ,s.validator_index AS validator_index
    ,s.pubkey AS pubkey
    ,s.withdrawal_address AS withdrawal_address
    ,s.status AS status
    ,s.slashed AS slashed
    -- Raw lifecycle epochs (UInt64 from the beacon state). FAR_FUTURE_EPOCH
    -- preserved here for debugging; masked out in the date columns below.
    ,s.activation_eligibility_epoch AS activation_eligibility_epoch
    ,s.activation_epoch AS activation_epoch
    ,s.exit_epoch AS exit_epoch
    ,s.withdrawable_epoch AS withdrawable_epoch
    -- Derived date columns: NULL for FAR_FUTURE_EPOCH, else genesis + epoch * slots * seconds.
    ,IF(
        s.activation_epoch >= toUInt64(18446744073709551615),
        NULL,
        toDate(toDateTime(th.genesis_time_unix + s.activation_epoch * th.slots_per_epoch * th.seconds_per_slot))
     ) AS activation_date
    ,IF(
        s.exit_epoch >= toUInt64(18446744073709551615),
        NULL,
        toDate(toDateTime(th.genesis_time_unix + s.exit_epoch * th.slots_per_epoch * th.seconds_per_slot))
     ) AS exit_date
    ,IF(
        s.withdrawable_epoch >= toUInt64(18446744073709551615),
        NULL,
        toDate(toDateTime(th.genesis_time_unix + s.withdrawable_epoch * th.slots_per_epoch * th.seconds_per_slot))
     ) AS withdrawable_date
    -- Balances / income
    ,COALESCE(li.balance_gno, 0) AS balance_gno
    ,COALESCE(li.effective_balance_gno, 0) AS effective_balance_gno
    ,COALESCE(i30.apy_30d, 0) AS apy_30d
    ,COALESCE(i30.consensus_income_amount_30d_gno, 0) AS consensus_income_amount_30d_gno
    ,COALESCE(p30.proposer_reward_total_30d_gno, 0) AS proposer_reward_total_30d_gno
    ,COALESCE(pl.proposed_blocks_count_lifetime, 0) AS proposed_blocks_count_lifetime
    ,COALESCE(pl.proposer_reward_total_lifetime_gno, 0) AS proposer_reward_total_lifetime_gno
    ,COALESCE(li.total_income_estimated_gno, 0) AS total_income_estimated_gno
    ,li.latest_date AS latest_date
FROM `dbt`.`fct_consensus_validators_status_latest` s
CROSS JOIN time_helpers th
LEFT JOIN latest_income li    ON li.validator_index = s.validator_index
LEFT JOIN income_30d i30      ON i30.validator_index = s.validator_index
LEFT JOIN proposer_30d p30    ON p30.validator_index = s.validator_index
LEFT JOIN proposer_lifetime pl ON pl.validator_index = s.validator_index
ORDER BY s.withdrawal_credentials, s.validator_index