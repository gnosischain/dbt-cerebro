




WITH base AS (
  SELECT *
  FROM `dbt`.`int_execution_transactions_by_project_daily`
  WHERE 1=1
  
    
  
    
    

   AND 
    toStartOfMonth(toDate(date)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -1))
      FROM `dbt`.`fct_execution_transactions_by_sector_daily` AS x1
      WHERE 1=1 
    )
    AND toDate(date) >= (
      SELECT addDays(max(toDate(x2.date)), -1)
      FROM `dbt`.`fct_execution_transactions_by_sector_daily` AS x2
      WHERE 1=1 
    )
  

  
)

SELECT
    date,
    sector,
    groupBitmapMerge(ua_bitmap_state)        AS active_accounts,
    sum(tx_count)                            AS txs,
    sum(gas_used_sum)                        AS gas_used_sum,
    round(toFloat64(sum(fee_native_sum)), 6) AS fee_native_sum
FROM base
GROUP BY date, sector