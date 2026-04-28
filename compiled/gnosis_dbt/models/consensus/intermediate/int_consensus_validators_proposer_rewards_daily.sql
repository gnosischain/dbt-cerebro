








SELECT
    toStartOfDay(slot_timestamp) AS date
    ,proposer_index AS validator_index
    ,COUNT(*) AS proposed_blocks_count
    ,SUM(total) / POWER(10, 9) AS proposer_reward_total_gno
    ,SUM(attestations) / POWER(10, 9) AS proposer_reward_attestations_gno
    ,SUM(sync_aggregate) / POWER(10, 9) AS proposer_reward_sync_aggregate_gno
    ,SUM(proposer_slashings) / POWER(10, 9) AS proposer_reward_proposer_slashings_gno
    ,SUM(attester_slashings) / POWER(10, 9) AS proposer_reward_attester_slashings_gno
FROM `dbt`.`stg_consensus__rewards`
WHERE
    slot_timestamp < today()
    
    
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(slot_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -1))
        FROM `dbt`.`int_consensus_validators_proposer_rewards_daily` AS x1
        WHERE 1=1 
  

      )
      AND toDate(slot_timestamp) >= (
        SELECT
          
            addDays(max(toDate(x2.date)), -1)
          

        FROM `dbt`.`int_consensus_validators_proposer_rewards_daily` AS x2
        WHERE 1=1 
  

      )
    
  

    
    
GROUP BY 1, 2