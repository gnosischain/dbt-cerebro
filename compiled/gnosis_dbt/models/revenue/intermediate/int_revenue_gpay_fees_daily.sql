

  
  
  






WITH transfers AS (
    SELECT
        t.date,
        lower(t."from") AS user,
        t.symbol,
        multiIf(
            t.symbol = 'EURe',   toFloat64(20) / 10000.0,
            t.symbol = 'GBPe',   toFloat64(20) / 10000.0,
            t.symbol = 'USDC.e', toFloat64(100) / 10000.0,
            toFloat64(0)
        ) AS fee_rate,
        sum(toFloat64(t.amount_raw) / pow(10, w.decimals)) AS amount_native
    FROM `dbt`.`int_execution_transfers_whitelisted_daily` t
    INNER JOIN `dbt`.`tokens_whitelist` w
        ON lower(w.address) = t.token_address
       AND t.date >= w.date_start
       AND (w.date_end IS NULL OR t.date < w.date_end)
    WHERE t.date < today()
      AND lower(t."to") = '0x4822521e6135cd2599199c83ea35179229a172ee'
      AND t.symbol IN ('EURe','GBPe','USDC.e')
      AND t.amount_raw IS NOT NULL
      AND t."from" IS NOT NULL
      
        
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(t.date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -1))
        FROM `dbt`.`int_revenue_gpay_fees_daily` AS x1
        WHERE 1=1 
      )
      AND toDate(t.date) >= (
        SELECT
          
            addDays(max(toDate(x2.date)), -1)
          

        FROM `dbt`.`int_revenue_gpay_fees_daily` AS x2
        WHERE 1=1 
      )
    
  

      
    GROUP BY t.date, lower(t."from"), t.symbol, fee_rate
),

prices AS (
    SELECT date, symbol, price
    FROM `dbt`.`int_execution_token_prices_daily`
    WHERE price IS NOT NULL
)

SELECT
    tr.date,
    tr.user,
    tr.symbol,
    round(sum(tr.amount_native * tr.fee_rate), 8)           AS fees_native,
    round(sum(tr.amount_native * tr.fee_rate * p.price), 8) AS fees,
    round(sum(tr.amount_native * p.price), 6)               AS volume_usd
FROM transfers tr
LEFT JOIN prices p
    ON p.date = tr.date AND p.symbol = tr.symbol
GROUP BY tr.date, tr.user, tr.symbol