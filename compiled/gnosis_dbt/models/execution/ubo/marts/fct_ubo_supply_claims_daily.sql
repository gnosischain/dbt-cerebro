




SELECT date, protocol, container_address, token_address, symbol, token_class,
       ubo_address, balance_raw, balance, balance_usd
FROM `dbt`.`int_ubo_claims_aave_daily`
WHERE date < today()
  
    
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`fct_ubo_supply_claims_daily` AS x1
        WHERE 1=1 
      )
    
  

  

UNION ALL

SELECT date, protocol, container_address, token_address, symbol, token_class,
       ubo_address, balance_raw, balance, balance_usd
FROM `dbt`.`int_ubo_claims_balancer_v2_daily`
WHERE date < today()
  
    
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`fct_ubo_supply_claims_daily` AS x1
        WHERE 1=1 
      )
    
  

  

UNION ALL

SELECT date, protocol, container_address, token_address, symbol, token_class,
       ubo_address, balance_raw, balance, balance_usd
FROM `dbt`.`int_ubo_claims_uniswap_v3_daily`
WHERE date < today()
  
    
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`fct_ubo_supply_claims_daily` AS x1
        WHERE 1=1 
      )
    
  

  

UNION ALL

SELECT date, protocol, container_address, token_address, symbol, token_class,
       ubo_address, balance_raw, balance, balance_usd
FROM `dbt`.`int_ubo_claims_swapr_v3_daily`
WHERE date < today()
  
    
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`fct_ubo_supply_claims_daily` AS x1
        WHERE 1=1 
      )
    
  

  

UNION ALL

SELECT date, protocol, container_address, token_address, symbol, token_class,
       ubo_address, balance_raw, balance, balance_usd
FROM `dbt`.`int_ubo_claims_curve_daily`
WHERE date < today()
  
    
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`fct_ubo_supply_claims_daily` AS x1
        WHERE 1=1 
      )
    
  

  

UNION ALL

SELECT date, protocol, container_address, token_address, symbol, token_class,
       ubo_address, balance_raw, balance, balance_usd
FROM `dbt`.`int_ubo_claims_sdai_daily`
WHERE date < today()
  
    
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`fct_ubo_supply_claims_daily` AS x1
        WHERE 1=1 
      )
    
  

  