




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
    toStartOfMonth(toDate(b.date)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
      FROM `dbt`.`int_execution_tokens_supply_holders_daily` AS x1
      WHERE 1=1 
    )
    AND toDate(b.date) >= (
      SELECT addDays(max(toDate(x2.date)), -0)
      FROM `dbt`.`int_execution_tokens_supply_holders_daily` AS x2
      WHERE 1=1 
    )
  

  
GROUP BY b.date, b.token_address
ORDER BY b.date, b.token_address