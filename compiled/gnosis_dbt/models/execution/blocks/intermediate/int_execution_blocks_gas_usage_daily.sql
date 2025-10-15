

SELECT
  toDate(block_timestamp)         AS date,
  SUM(gas_used)                   AS gas_used_sum,
  SUM(gas_limit)                  AS gas_limit_sum,
  gas_used_sum / NULLIF(gas_limit_sum, 0) AS gas_used_fraq
FROM `dbt`.`stg_execution__blocks`

  
    
      
    

    WHERE 
    toStartOfMonth(toStartOfDay(block_timestamp)) >= (
      SELECT
        max(toStartOfMonth(date))
      FROM `dbt`.`int_execution_blocks_gas_usage_daily`
    )
  

GROUP BY date