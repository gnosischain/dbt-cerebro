




WITH src AS (
  SELECT
    toStartOfMonth(date)                   AS month,
    project,
    sumState(tx_count)                     AS txs_state,
    sumState(fee_native_sum)               AS fee_state,
    groupBitmapMergeState(ua_bitmap_state) AS aa_state
  FROM `dbt`.`int_execution_transactions_by_project_daily`
  WHERE 1=1
    
      
  
    
    

   AND 
    toStartOfMonth(toDate(date)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.month)), -0))
      FROM `dbt`.`int_execution_transactions_by_project_alltime_state` AS x1
      WHERE 1=1 
    )
    AND toDate(date) >= (
      SELECT addDays(max(toDate(x2.month)), -0)
      FROM `dbt`.`int_execution_transactions_by_project_alltime_state` AS x2
      WHERE 1=1 
    )
  

    
  GROUP BY month, project
)

SELECT project, month, txs_state, fee_state, aa_state
FROM src