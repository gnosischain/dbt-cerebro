






-- Every "_gno" column below is REAL GNO: source reward amounts are gwei-of-mGNO
-- (32 mGNO = 1 GNO), converted here at the origin via /1e9/32.
-- Consumers must NOT divide by 32 again.
--
-- incremental_strategy resolves to `append` when start_month is set: refresh.py
-- runs validator-index STAGES within each month, and insert_overwrite would make
-- every stage's REPLACE PARTITION wipe the previous stages' rows (verified
-- 2026-07-09: a staged insert_overwrite rebuild left only the 500k-600k stage).
-- Same design as int_consensus_validators_income_daily.



SELECT
    toStartOfDay(slot_timestamp) AS date
    ,proposer_index AS validator_index
    ,COUNT(*) AS proposed_blocks_count
    ,SUM(total) / POWER(10, 9) / 32 AS proposer_reward_total_gno
    ,SUM(attestations) / POWER(10, 9) / 32 AS proposer_reward_attestations_gno
    ,SUM(sync_aggregate) / POWER(10, 9) / 32 AS proposer_reward_sync_aggregate_gno
    ,SUM(proposer_slashings) / POWER(10, 9) / 32 AS proposer_reward_proposer_slashings_gno
    ,SUM(attester_slashings) / POWER(10, 9) / 32 AS proposer_reward_attester_slashings_gno
FROM `dbt`.`stg_consensus__rewards`
WHERE
    slot_timestamp < today()
    
    
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(slot_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -1))
        FROM `dbt`.`int_consensus_validators_proposer_rewards_daily` AS x1
        WHERE 1=1 
  

      )
      
    
  

    
    
GROUP BY 1, 2