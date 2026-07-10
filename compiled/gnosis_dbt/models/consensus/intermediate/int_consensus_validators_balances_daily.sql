





-- Values in real GNO. Source columns are gwei-of-mGNO (32 mGNO = 1 GNO), so the
-- full conversion is /1e9 (gwei) /32 (mGNO -> GNO), applied HERE at the origin —
-- downstream marts must NOT divide again.
-- Full-history rebuilds must go through scripts/full_refresh/refresh.py (see
-- meta.full_refresh in schema.yml): a single-pass FINAL scan over the whole
-- stg_consensus__validators history exceeds the 10.8 GiB memory cap (CH 241).
SELECT
    toStartOfDay(slot_timestamp) AS date
    ,SUM(balance/POWER(10,9)/32) AS balance
    ,SUM(effective_balance/POWER(10,9)/32) AS effective_balance
FROM `dbt`.`stg_consensus__validators`
WHERE
    slot_timestamp < today()
    
    
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(slot_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`int_consensus_validators_balances_daily` AS x1
        WHERE 1=1 
      )
      
    
  

    
GROUP BY date