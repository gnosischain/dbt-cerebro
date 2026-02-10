




WITH base AS (
  SELECT *
  FROM `dbt`.`int_execution_transactions_by_project_daily`
  WHERE date < toStartOfWeek(today())
  
    
  
    
    

   AND 
    toStartOfMonth(toDate(date)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.week)), -1))
      FROM `dbt`.`fct_execution_transactions_by_sector_weekly` AS x1
      WHERE 1=1 
    )
    AND toDate(date) >= (
      SELECT addDays(max(toDate(x2.week)), -1)
      FROM `dbt`.`fct_execution_transactions_by_sector_weekly` AS x2
      WHERE 1=1 
    )
  

  
)

SELECT
  toStartOfWeek(date)                          AS week,
  sector,
  toUInt64(groupBitmapMerge(ua_bitmap_state))  AS active_accounts,  
  sum(tx_count)                                AS txs,
  sum(gas_used_sum)                            AS gas_used_sum,
  round(toFloat64(sum(fee_native_sum)), 2)     AS fee_native_sum
FROM base
GROUP BY week, sector