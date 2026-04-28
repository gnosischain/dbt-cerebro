






-- Inbound leg of token movements (to_address = address, from_address = counterparty).

SELECT
  date,
  lower(token_address) AS token_address,
  symbol,
  lower("to") AS address,
  lower("from") AS counterparty,
  'in' AS direction,
  sum(amount_raw) AS net_amount_raw,
  sum(abs(amount_raw)) AS gross_amount_raw,
  sum(transfer_count) AS transfer_count
FROM `dbt`.`int_execution_transfers_whitelisted_daily`
WHERE date < today()
  AND "to" IS NOT NULL
  AND "to" != ''
  
    
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -1))
        FROM `dbt`.`int_execution_account_token_movements_in_daily` AS x1
        WHERE 1=1 
      )
      AND toDate(date) >= (
        SELECT
          
            addDays(max(toDate(x2.date)), -1)
          

        FROM `dbt`.`int_execution_account_token_movements_in_daily` AS x2
        WHERE 1=1 
      )
    
  

  
GROUP BY date, token_address, symbol, address, counterparty