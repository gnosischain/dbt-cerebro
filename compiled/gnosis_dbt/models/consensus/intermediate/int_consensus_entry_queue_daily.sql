

WITH

queue_activation AS (
    SELECT
        validator_index
        ,date
        ,epoch_eligibility
        ,epoch_activation
        ,(epoch_activation - epoch_eligibility) * 16 * 5 /(60 * 60 * 24) AS activation_days
    FROM (
        SELECT 
            validator_index
            ,toStartOfDay(argMin(slot_timestamp,slot)) AS date
            ,argMin(activation_eligibility_epoch,slot) AS epoch_eligibility
            ,argMin(activation_epoch,slot) AS epoch_activation
        FROM `dbt`.`stg_consensus__validators`
        WHERE 
            activation_epoch < 18446744073709551615
            
  
    
    

   AND 
    toStartOfMonth(toDate(slot_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
      FROM `dbt`.`int_consensus_entry_queue_daily` AS x1
      WHERE 1=1 
    )
    AND toDate(slot_timestamp) >= (
      SELECT addDays(max(toDate(x2.date)), -0)
      FROM `dbt`.`int_consensus_entry_queue_daily` AS x2
      WHERE 1=1 
    )
  

        GROUP BY 1
    )
)

SELECT
    date
    ,validator_count
    ,q_activation[1] AS q05
    ,q_activation[2] AS q10
    ,q_activation[3] AS q25
    ,q_activation[4] AS q50
    ,q_activation[5] AS q75
    ,q_activation[6] AS q90
    ,q_activation[7] AS q95
    ,mean
FROM (
    SELECT
        date,
        count() AS validator_count
        ,quantilesTDigest(
            0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95
        )(activation_days) AS q_activation
        ,avg(activation_days) AS  mean
    FROM queue_activation
    GROUP BY date
)