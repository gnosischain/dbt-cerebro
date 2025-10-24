




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
    toStartOfMonth(toStartOfDay(date)) >= (
      SELECT max(toStartOfMonth(t.month))
      FROM `dbt`.`int_execution_transactions_by_project_alltime_state` AS t
    )
    AND toStartOfDay(date) >= (
      SELECT max(toStartOfDay(t2.month, 'UTC'))
      FROM `dbt`.`int_execution_transactions_by_project_alltime_state` AS t2
    )
  

    
  GROUP BY month, project
)

SELECT project, month, txs_state, fee_state, aa_state
FROM src