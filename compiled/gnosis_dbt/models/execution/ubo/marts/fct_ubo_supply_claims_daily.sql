

-- THE reusable UBO supply-claims surface.
--
-- One row per (date, protocol, container_address, ubo_address). A "supply
-- claim" is a withdrawable position an end-holder has on a token that is
-- held by a container contract (aToken, vault, pool LP). Joining this
-- model lets downstream consumers see real beneficial owners in place of
-- pool contracts.
--
-- Phase 1: Aave V3 + SparkLend.
-- Phase 2: Balancer V2.
-- Phase 3+: add UNION ALL branches for Balancer V3, Curve, V2-style AMMs.
-- Consumers downstream do not change — they pick up new protocols automatically.




SELECT
    date,
    protocol,
    container_address,
    token_address,
    symbol,
    token_class,
    ubo_address,
    balance_raw,
    balance,
    balance_usd
FROM `dbt`.`int_ubo_claims_aave_daily`
WHERE date < today()
  
    
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`fct_ubo_supply_claims_daily` AS x1
        WHERE 1=1 
      )
      AND toDate(date) >= (
        SELECT
          
            addDays(max(toDate(x2.date)), -0)
          

        FROM `dbt`.`fct_ubo_supply_claims_daily` AS x2
        WHERE 1=1 
      )
    
  

  

UNION ALL

SELECT
    date,
    protocol,
    container_address,
    token_address,
    symbol,
    token_class,
    ubo_address,
    balance_raw,
    balance,
    balance_usd
FROM `dbt`.`int_ubo_claims_balancer_v2_daily`
WHERE date < today()
  
    
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`fct_ubo_supply_claims_daily` AS x1
        WHERE 1=1 
      )
      AND toDate(date) >= (
        SELECT
          
            addDays(max(toDate(x2.date)), -0)
          

        FROM `dbt`.`fct_ubo_supply_claims_daily` AS x2
        WHERE 1=1 
      )
    
  

  