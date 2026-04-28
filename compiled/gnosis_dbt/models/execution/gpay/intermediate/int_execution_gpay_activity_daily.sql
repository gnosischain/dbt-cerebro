

SELECT
    date
    ,wallet_address
    ,action
    ,direction
    ,symbol
    ,SUM(value_raw) AS amount_raw
    ,SUM(amount) AS amount
    ,SUM(amount_usd) AS amount_usd
    ,COUNT() AS activity_count
FROM `dbt`.`int_execution_gpay_activity`

  
    
    
    
    
    

    WHERE 
    
      
      toStartOfMonth(toDate(date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`int_execution_gpay_activity_daily` AS x1
        WHERE 1=1 
      )
      AND toDate(date) >= (
        SELECT
          
            addDays(max(toDate(x2.date)), -0)
          

        FROM `dbt`.`int_execution_gpay_activity_daily` AS x2
        WHERE 1=1 
      )
    
  

GROUP BY date, wallet_address, action, direction, symbol