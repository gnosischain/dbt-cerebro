




SELECT
    b.date,
    b.token_address,
    any(b.symbol) AS symbol,
    any(b.token_class) AS token_class,

    sumIf(
        b.balance,
        lower(b.address) != '0x0000000000000000000000000000000000000000'
    ) AS supply,

    toUInt64(
      countDistinctIf(
          b.address,
          b.balance > 0
          AND lower(b.address) != '0x0000000000000000000000000000000000000000'
      )
    ) AS holders
FROM `dbt`.`int_execution_tokens_balances_daily` b
WHERE b.date < today()
  
    
  
    
      
    

   AND 
    toStartOfMonth(toStartOfDay(b.date)) >= (
      SELECT max(toStartOfMonth(x1.date))
      FROM `dbt`.`int_execution_tokens_supply_holders_daily` AS x1
    )
    AND toStartOfDay(b.date) >= (
      SELECT max(toStartOfDay(x2.date, 'UTC'))
      FROM `dbt`.`int_execution_tokens_supply_holders_daily` AS x2
    )
  

  
GROUP BY b.date, b.token_address
ORDER BY b.date, b.token_address