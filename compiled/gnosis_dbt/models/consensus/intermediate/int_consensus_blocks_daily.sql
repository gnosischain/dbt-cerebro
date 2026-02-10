

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
    ,total_blob_commitments
    ,blocks_with_zero_blob_commitments
    ,CASE
        WHEN toStartOfDay(toDateTime(genesis_time_unix)) = date 
            THEN CAST((86400 - toUnixTimestamp(toDateTime(genesis_time_unix)) % 86400) / seconds_per_slot - cnt AS UInt64)
        ELSE CAST(86400 / seconds_per_slot - cnt AS UInt64)
    END AS blocks_missed
FROM (
    SELECT
        toStartOfDay(slot_timestamp) AS date
        ,COUNT(*) AS cnt
        ,SUM(blob_kzg_commitments_count) AS total_blob_commitments
        ,SUM(IF(blob_kzg_commitments_count = 0, 1, 0)) AS blocks_with_zero_blob_commitments
    FROM `dbt`.`stg_consensus__blocks`
    WHERE
        slot_timestamp < today()
        
  
    
    

   AND 
    toStartOfMonth(toDate(slot_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
      FROM `dbt`.`int_consensus_blocks_daily` AS x1
      WHERE 1=1 
    )
    AND toDate(slot_timestamp) >= (
      SELECT addDays(max(toDate(x2.date)), -0)
      FROM `dbt`.`int_consensus_blocks_daily` AS x2
      WHERE 1=1 
    )
  

    GROUP BY 1
) t1
CROSS JOIN time_helpers t2