




-- Rows from fct_ubo_supply_claims_daily where ubo_address is itself a known
-- container for the bridge token (container_address). Materialized so that
-- fct_ubo_supply_claims_resolved_daily can place this tiny table on the right
-- (hash-table) side of the redistribution join, streaming the full claims
-- dataset through on the left (probe) side without loading it into memory.
SELECT
    f.date, f.protocol, f.container_address, f.ubo_address, f.token_address,
    f.symbol, f.token_class, f.balance_raw, f.balance, f.balance_usd
FROM `dbt`.`fct_ubo_supply_claims_daily` f
INNER JOIN `dbt`.`fct_ubo_known_containers_daily` kc
    ON  f.date              = kc.date
    AND f.ubo_address       = kc.container_address
    AND f.container_address = kc.token_address
WHERE f.date < today()
  
    
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(f.date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`int_ubo_second_level_daily` AS x1
        WHERE 1=1 
      )
      
    
  

  