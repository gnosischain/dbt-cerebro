







    
    
    

    
    
    

    
    
    

    
    
    

    
    
    

    
    
    

    
    
    

    
    
    

    
    
    



        SELECT
            'bC3M' as bticker
            ,toStartOfDay(block_timestamp) AS date
            ,argMax(toUInt256OrNull(decoded_params['current'])/POWER(10,8),block_timestamp) AS price
        FROM `dbt`.`contracts_backedfi_bC3M_Oracle_events`
        WHERE
            event_name = 'AnswerUpdated'
            AND block_timestamp < today()
            
  
    
      
    

   AND 
    toStartOfMonth(toStartOfDay(block_timestamp)) >= (
      SELECT max(toStartOfMonth(t.date))
      FROM `dbt`.`int_execution_rwa_backedfi_prices` AS t
    )
    AND toStartOfDay(block_timestamp) >= (
      SELECT max(toStartOfDay(t2.date, 'UTC'))
      FROM `dbt`.`int_execution_rwa_backedfi_prices` AS t2
    )
  

        GROUP BY 1, 2
    
union all

        SELECT
            'bCOIN' as bticker
            ,toStartOfDay(block_timestamp) AS date
            ,argMax(toUInt256OrNull(decoded_params['current'])/POWER(10,8),block_timestamp) AS price
        FROM `dbt`.`contracts_backedfi_bCOIN_Oracle_events`
        WHERE
            event_name = 'AnswerUpdated'
            AND block_timestamp < today()
            
  
    
      
    

   AND 
    toStartOfMonth(toStartOfDay(block_timestamp)) >= (
      SELECT max(toStartOfMonth(t.date))
      FROM `dbt`.`int_execution_rwa_backedfi_prices` AS t
    )
    AND toStartOfDay(block_timestamp) >= (
      SELECT max(toStartOfDay(t2.date, 'UTC'))
      FROM `dbt`.`int_execution_rwa_backedfi_prices` AS t2
    )
  

        GROUP BY 1, 2
    
union all

        SELECT
            'bCSPX' as bticker
            ,toStartOfDay(block_timestamp) AS date
            ,argMax(toUInt256OrNull(decoded_params['current'])/POWER(10,8),block_timestamp) AS price
        FROM `dbt`.`contracts_backedfi_bCSPX_Oracle_events`
        WHERE
            event_name = 'AnswerUpdated'
            AND block_timestamp < today()
            
  
    
      
    

   AND 
    toStartOfMonth(toStartOfDay(block_timestamp)) >= (
      SELECT max(toStartOfMonth(t.date))
      FROM `dbt`.`int_execution_rwa_backedfi_prices` AS t
    )
    AND toStartOfDay(block_timestamp) >= (
      SELECT max(toStartOfDay(t2.date, 'UTC'))
      FROM `dbt`.`int_execution_rwa_backedfi_prices` AS t2
    )
  

        GROUP BY 1, 2
    
union all

        SELECT
            'bHIGH' as bticker
            ,toStartOfDay(block_timestamp) AS date
            ,argMax(toUInt256OrNull(decoded_params['current'])/POWER(10,8),block_timestamp) AS price
        FROM `dbt`.`contracts_backedfi_bHIGH_Oracle_events`
        WHERE
            event_name = 'AnswerUpdated'
            AND block_timestamp < today()
            
  
    
      
    

   AND 
    toStartOfMonth(toStartOfDay(block_timestamp)) >= (
      SELECT max(toStartOfMonth(t.date))
      FROM `dbt`.`int_execution_rwa_backedfi_prices` AS t
    )
    AND toStartOfDay(block_timestamp) >= (
      SELECT max(toStartOfDay(t2.date, 'UTC'))
      FROM `dbt`.`int_execution_rwa_backedfi_prices` AS t2
    )
  

        GROUP BY 1, 2
    
union all

        SELECT
            'bIB01' as bticker
            ,toStartOfDay(block_timestamp) AS date
            ,argMax(toUInt256OrNull(decoded_params['current'])/POWER(10,8),block_timestamp) AS price
        FROM `dbt`.`contracts_backedfi_bIB01_Oracle_events`
        WHERE
            event_name = 'AnswerUpdated'
            AND block_timestamp < today()
            
  
    
      
    

   AND 
    toStartOfMonth(toStartOfDay(block_timestamp)) >= (
      SELECT max(toStartOfMonth(t.date))
      FROM `dbt`.`int_execution_rwa_backedfi_prices` AS t
    )
    AND toStartOfDay(block_timestamp) >= (
      SELECT max(toStartOfDay(t2.date, 'UTC'))
      FROM `dbt`.`int_execution_rwa_backedfi_prices` AS t2
    )
  

        GROUP BY 1, 2
    
union all

        SELECT
            'bIBTA' as bticker
            ,toStartOfDay(block_timestamp) AS date
            ,argMax(toUInt256OrNull(decoded_params['current'])/POWER(10,8),block_timestamp) AS price
        FROM `dbt`.`contracts_backedfi_bIBTA_Oracle_events`
        WHERE
            event_name = 'AnswerUpdated'
            AND block_timestamp < today()
            
  
    
      
    

   AND 
    toStartOfMonth(toStartOfDay(block_timestamp)) >= (
      SELECT max(toStartOfMonth(t.date))
      FROM `dbt`.`int_execution_rwa_backedfi_prices` AS t
    )
    AND toStartOfDay(block_timestamp) >= (
      SELECT max(toStartOfDay(t2.date, 'UTC'))
      FROM `dbt`.`int_execution_rwa_backedfi_prices` AS t2
    )
  

        GROUP BY 1, 2
    
union all

        SELECT
            'bMSTR' as bticker
            ,toStartOfDay(block_timestamp) AS date
            ,argMax(toUInt256OrNull(decoded_params['current'])/POWER(10,8),block_timestamp) AS price
        FROM `dbt`.`contracts_backedfi_bMSTR_Oracle_events`
        WHERE
            event_name = 'AnswerUpdated'
            AND block_timestamp < today()
            
  
    
      
    

   AND 
    toStartOfMonth(toStartOfDay(block_timestamp)) >= (
      SELECT max(toStartOfMonth(t.date))
      FROM `dbt`.`int_execution_rwa_backedfi_prices` AS t
    )
    AND toStartOfDay(block_timestamp) >= (
      SELECT max(toStartOfDay(t2.date, 'UTC'))
      FROM `dbt`.`int_execution_rwa_backedfi_prices` AS t2
    )
  

        GROUP BY 1, 2
    
union all

        SELECT
            'bNVDA' as bticker
            ,toStartOfDay(block_timestamp) AS date
            ,argMax(toUInt256OrNull(decoded_params['current'])/POWER(10,8),block_timestamp) AS price
        FROM `dbt`.`contracts_backedfi_bNVDA_Oracle_events`
        WHERE
            event_name = 'AnswerUpdated'
            AND block_timestamp < today()
            
  
    
      
    

   AND 
    toStartOfMonth(toStartOfDay(block_timestamp)) >= (
      SELECT max(toStartOfMonth(t.date))
      FROM `dbt`.`int_execution_rwa_backedfi_prices` AS t
    )
    AND toStartOfDay(block_timestamp) >= (
      SELECT max(toStartOfDay(t2.date, 'UTC'))
      FROM `dbt`.`int_execution_rwa_backedfi_prices` AS t2
    )
  

        GROUP BY 1, 2
    
union all

        SELECT
            'TSLAx' as bticker
            ,toStartOfDay(block_timestamp) AS date
            ,argMax(toUInt256OrNull(decoded_params['current'])/POWER(10,8),block_timestamp) AS price
        FROM `dbt`.`contracts_backedfi_TSLAx_Oracle_events`
        WHERE
            event_name = 'AnswerUpdated'
            AND block_timestamp < today()
            
  
    
      
    

   AND 
    toStartOfMonth(toStartOfDay(block_timestamp)) >= (
      SELECT max(toStartOfMonth(t.date))
      FROM `dbt`.`int_execution_rwa_backedfi_prices` AS t
    )
    AND toStartOfDay(block_timestamp) >= (
      SELECT max(toStartOfDay(t2.date, 'UTC'))
      FROM `dbt`.`int_execution_rwa_backedfi_prices` AS t2
    )
  

        GROUP BY 1, 2
    