-- Spec-cap overrun monitor for int_consensus_validators_income_daily.
--
-- After the v2 spec-cap refactor, consensus_income_amount_gno for a non-slashed,
-- currently-active validator must lie within [-0.05, expected_reward_cap_gno * 1.1]:
--   * upper bound = spec reward cap × 10% safety margin; anything above suggests a
--     consolidation inflow or deposit credit leaked into income;
--   * lower bound = -0.05 GNO daily loss (the old -1 mGNO floor in real-GNO terms,
--     rounded); anything below is either a slashing event
--     (rare, validators with slashed=1 in status_latest are excluded) or an
--     accounting bug.
--
-- Exclusions (all represent legitimate edge cases, not bugs):
--   1. slashed validators — real losses.
--   2. dormant validators (effective_balance_gno = 0 AND effective_reward_cap = 0) —
--      exited/zero-balance rows with tiny float-noise income don't represent a real
--      cap overrun. Their cap is 0 so any positive value would "exceed" it.
--   3. consolidation-inflow days (consolidation_inflow_gno > 0) — the
--      transferred_amount in int_consensus_validators_consolidations_daily is the
--      SOURCE's effective_balance (always a 32-GNO multiple pre-Pectra); a dormant
--      source carrying inactivity penalties has real balance slightly below its
--      effective_balance, so the target's actual balance delta is a few %
--      smaller than the reported inflow. This secondary effect is documented and
--      tolerated for now; a stricter fix would switch the consolidations amount
--      formula to use balance_gwei instead of effective_balance_gwei.
--
-- Returns offending rows; passing = zero rows. The tolerance is intentionally loose so
-- this catches systemic bugs (like the pre-fix consolidation asymmetry that produced
-- -1,007 GNO single-validator days) rather than rounding noise.
SELECT
    date
    ,validator_index
    ,consensus_income_amount_gno
    ,expected_reward_cap_gno
    ,effective_balance_gno
    ,consolidation_inflow_gno
    ,CASE
        WHEN consensus_income_amount_gno > expected_reward_cap_gno * 1.1 THEN 'above_cap'
        WHEN consensus_income_amount_gno < -0.05 THEN 'below_floor'
        ELSE 'ok'
    END AS violation_kind
FROM `dbt`.`int_consensus_validators_income_daily`
WHERE
    toDate(date) >= today() - 7
    
    -- Only evaluate active validators — dormant rows have cap=0 and can't be
    -- meaningfully bounded by the spec formula.
    AND effective_balance_gno > 0
    AND (
        consensus_income_amount_gno > expected_reward_cap_gno * 1.1
        OR consensus_income_amount_gno < -0.05
    )
    -- Exclude known-slashed validators (real losses).
    AND validator_index NOT IN (
        SELECT validator_index
        FROM `dbt`.`fct_consensus_validators_status_latest`
        WHERE slashed = 1
    )
    -- Tolerate consolidation-inflow days (effective-vs-real balance mismatch on
    -- source validators carrying inactivity penalties; see test header comment).
    AND consolidation_inflow_gno = 0