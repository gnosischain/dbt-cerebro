




-- Outbound leg of token movements (from_address = address, to_address = counterparty).
-- Split out from the daily fct so the aggregating transform doesn't have both
-- legs of a UNION-then-GROUP plan resident at the same time.

SELECT
  date,
  lower(token_address) AS token_address,
  symbol,
  lower("from") AS address,
  lower("to") AS counterparty,
  'out' AS direction,
  -sum(amount_raw) AS net_amount_raw,
  sum(abs(amount_raw)) AS gross_amount_raw,
  sum(transfer_count) AS transfer_count
FROM `dbt`.`int_execution_transfers_whitelisted_daily`
WHERE date < today()
  AND "from" IS NOT NULL
  AND "from" != ''
  
    
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -1))
        FROM `dbt`.`int_execution_account_token_movements_out_daily` AS x1
        WHERE 1=1 
      )
      AND toDate(date) >= (
        SELECT
          
            addDays(max(toDate(x2.date)), -1)
          

        FROM `dbt`.`int_execution_account_token_movements_out_daily` AS x2
        WHERE 1=1 
      )
    
  

  
GROUP BY date, token_address, symbol, address, counterparty