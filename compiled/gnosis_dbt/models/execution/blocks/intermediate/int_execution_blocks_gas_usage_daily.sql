



WITH deduped_blocks AS (
    

SELECT block_timestamp, gas_used, gas_limit
FROM (
    SELECT
        block_timestamp, gas_used, gas_limit,
        ROW_NUMBER() OVER (
            PARTITION BY block_number
            ORDER BY insert_version DESC
        ) AS _dedup_rn
    FROM `execution`.`blocks`
    
    WHERE 
    block_timestamp > '1970-01-01'
    
  
    
    

   AND 
    toStartOfMonth(toDate(block_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
      FROM `dbt`.`int_execution_blocks_gas_usage_daily` AS x1
      WHERE 1=1 
    )
    AND toDate(block_timestamp) >= (
      SELECT addDays(max(toDate(x2.date)), -0)
      FROM `dbt`.`int_execution_blocks_gas_usage_daily` AS x2
      WHERE 1=1 
    )
  


    
)
WHERE _dedup_rn = 1

)

SELECT
  toDate(block_timestamp)         AS date,
  SUM(gas_used)                   AS gas_used_sum,
  SUM(gas_limit)                  AS gas_limit_sum,
  gas_used_sum / NULLIF(gas_limit_sum, 0) AS gas_used_fraq
FROM deduped_blocks
GROUP BY date