

WITH gp_delay_modules AS (
    SELECT gp_safe, module_proxy_address AS delay_module_address
    FROM `dbt`.`int_execution_gpay_safe_modules`
    WHERE contract_type = 'DelayModule'
),

events_filtered AS (
    SELECT
        toDate(d.block_timestamp)  AS date,
        d.delay_module_address     AS delay_module_address
    FROM `dbt`.`int_execution_gpay_delay_events` d
    WHERE d.event_name = 'TransactionAdded'
      AND toDate(d.block_timestamp) < today()
      
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(d.block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`int_execution_gpay_delay_activity_daily` AS x1
        WHERE 1=1 
      )
      AND toDate(d.block_timestamp) >= (
        SELECT
          
            addDays(max(toDate(x2.date)), -0)
          

        FROM `dbt`.`int_execution_gpay_delay_activity_daily` AS x2
        WHERE 1=1 
      )
    
  

)

SELECT
    e.date,
    m.gp_safe,
    count() AS tx_added_count
FROM events_filtered e
INNER JOIN gp_delay_modules m
    ON m.delay_module_address = e.delay_module_address
GROUP BY e.date, m.gp_safe