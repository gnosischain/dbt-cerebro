

WITH

time_helpers AS (
    SELECT
        genesis_time_unix,
        seconds_per_slot
    FROM 
        `dbt`.`stg_consensus__time_helpers`
)

SELECT
    date
    ,cnt AS blocks_produced
    ,CASE
        WHEN toStartOfDay(toDateTime(genesis_time_unix)) = date 
            THEN CAST((86400 - toUnixTimestamp(toDateTime(genesis_time_unix)) % 86400) / seconds_per_slot - cnt AS UInt64)
        ELSE CAST(86400 / seconds_per_slot - cnt AS UInt64)
    END AS blocks_missed
FROM (
    SELECT
        toStartOfDay(slot_timestamp) AS date
        ,COUNT(*) AS cnt
    FROM `dbt`.`stg_consensus__blocks`
    WHERE
        slot_timestamp < today()
        
  
    
      
    

    AND 
    toStartOfMonth(toStartOfDay(slot_timestamp)) >= (
      SELECT
        max(toStartOfMonth(date))
      FROM `dbt`.`int_consensus_blocks_daily`
    )
  

    GROUP BY 1
) t1
CROSS JOIN time_helpers t2