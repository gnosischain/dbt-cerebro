




SELECT
    date,
    groupBitmapMerge(ua_bitmap_state) AS active_accounts
FROM `dbt`.`int_execution_transactions_by_project_daily`
WHERE 1=1

  
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -1))
        FROM `dbt`.`fct_execution_transactions_active_accounts_daily` AS x1
        WHERE 1=1 
      )
      
    
  


GROUP BY date