










-- Read through the staging view (FINAL handles source-side dedup). Target's
-- ReplacingMergeTree is the write-side safety net; no in-model aggregation.
SELECT
    toStartOfDay(slot_timestamp) AS date
    ,validator_index
    ,status
    ,lower(pubkey) AS pubkey
    ,lower(withdrawal_credentials) AS withdrawal_credentials
    ,if(
        startsWith(lower(withdrawal_credentials), '0x01')
        OR startsWith(lower(withdrawal_credentials), '0x02'),
        concat('0x', substring(lower(withdrawal_credentials), 27, 40)),
        NULL
    ) AS withdrawal_address
    ,balance AS balance_gwei
    ,effective_balance AS effective_balance_gwei
    ,slashed
    ,activation_epoch
    ,exit_epoch
    ,withdrawable_epoch
    ,slot AS last_slot
FROM `dbt`.`stg_consensus__validators_all`
WHERE
    slot_timestamp < today()
    
    
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(slot_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`int_consensus_validators_snapshots_daily` AS x1
        WHERE 1=1 
  

      )
      AND toDate(slot_timestamp) >= (
        SELECT
          
            addDays(max(toDate(x2.date)), -0)
          

        FROM `dbt`.`int_consensus_validators_snapshots_daily` AS x2
        WHERE 1=1 
  

      )
    
  

    
    