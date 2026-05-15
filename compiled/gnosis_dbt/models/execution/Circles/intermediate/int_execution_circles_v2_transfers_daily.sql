

-- Daily transfer volume + velocity by category. One row per
-- (date, transfer_category). Built directly off int_execution_circles_v2_transfers_categorised.




SELECT
    toDate(block_timestamp)                                  AS date,
    transfer_category                                        AS transfer_category,
    count()                                                  AS n_transfers,
    uniqExactIf(from_address,
        from_address != '0x0000000000000000000000000000000000000000') AS n_senders,
    uniqExactIf(to_address,
        to_address   != '0x0000000000000000000000000000000000000000') AS n_receivers,
    sum(toFloat64(amount_raw))            / pow(10, 18)      AS amount,
    sum(toFloat64(amount_demurraged_raw)) / pow(10, 18)      AS amount_demurraged
FROM `dbt`.`int_execution_circles_v2_transfers_categorised`
WHERE block_timestamp < today()
  
    
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`int_execution_circles_v2_transfers_daily` AS x1
        WHERE 1=1 
      )
      AND toDate(block_timestamp) >= (
        SELECT
          
            addDays(max(toDate(x2.date)), -0)
          

        FROM `dbt`.`int_execution_circles_v2_transfers_daily` AS x2
        WHERE 1=1 
      )
    
  

  
GROUP BY date, transfer_category