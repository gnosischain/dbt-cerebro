



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
    toStartOfMonth(toStartOfDay(block_timestamp)) >= (
      SELECT max(toStartOfMonth(x1.date))
      FROM `dbt`.`int_execution_blocks_gas_usage_daily` AS x1
    )
    AND toStartOfDay(block_timestamp) >= (
      SELECT max(toStartOfDay(x2.date, 'UTC'))
      FROM `dbt`.`int_execution_blocks_gas_usage_daily` AS x2
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