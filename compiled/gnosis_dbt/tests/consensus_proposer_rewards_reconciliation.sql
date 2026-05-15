-- Reconciles the daily proposer-rewards fact against the slot-level staging source.
-- For each (date, validator_index) in the fact's lookback window, SUM(total)
-- from stg_consensus__rewards grouped by proposer_index must match the fact's
-- proposer_reward_total_gno (after Gwei -> GNO conversion).

WITH

source_totals AS (
    SELECT
        toStartOfDay(slot_timestamp) AS date
        ,proposer_index AS validator_index
        ,SUM(total) / POWER(10, 9) AS proposer_reward_total_from_source_gno
        ,COUNT(*) AS blocks_from_source
    FROM `dbt`.`stg_consensus__rewards`
    WHERE slot_timestamp < today()
    GROUP BY 1, 2
),

fact AS (
    SELECT
        date
        ,validator_index
        ,proposer_reward_total_gno
        ,proposed_blocks_count
    FROM `dbt`.`int_consensus_validators_proposer_rewards_daily`
)

SELECT
    f.date
    ,f.validator_index
    ,f.proposer_reward_total_gno
    ,s.proposer_reward_total_from_source_gno
    ,f.proposed_blocks_count
    ,s.blocks_from_source
FROM fact f
INNER JOIN source_totals s
    ON s.date = f.date AND s.validator_index = f.validator_index
WHERE
    toDate(f.date) >= today() - 7
    
    AND (
        ABS(f.proposer_reward_total_gno - s.proposer_reward_total_from_source_gno) > 1e-6
        OR f.proposed_blocks_count != s.blocks_from_source
    )