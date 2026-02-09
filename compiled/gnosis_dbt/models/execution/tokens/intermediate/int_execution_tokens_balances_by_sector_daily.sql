




WITH

balances_filtered AS (
    SELECT
        b.date,
        lower(b.token_address) AS token_address,
        b.symbol AS symbol,
        b.token_class,
        lower(b.address) AS address,
        b.balance,
        b.balance_usd
    FROM `dbt`.`int_execution_tokens_balances_daily` b
    WHERE b.date < today()
      AND b.balance > 0
      AND lower(b.address) != '0x0000000000000000000000000000000000000000'
      
        
  
    
      
    

   AND 
    toStartOfMonth(toStartOfDay(b.date)) >= (
      SELECT max(toStartOfMonth(x1.date))
      FROM `dbt`.`int_execution_tokens_balances_by_sector_daily` AS x1
    )
    AND toStartOfDay(b.date) >= (
      SELECT max(toStartOfDay(x2.date, 'UTC'))
      FROM `dbt`.`int_execution_tokens_balances_by_sector_daily` AS x2
    )
  

      
),

labels AS (
    SELECT
        lower(address) AS address,
        sector
    FROM `dbt`.`int_crawlers_data_labels`
),

joined AS (
    SELECT
        b.date,
        b.token_address,
        b.symbol,
        b.token_class,
        b.address,
        b.balance,
        b.balance_usd,
        COALESCE(nullIf(trim(l.sector), ''), 'Unknown') AS sector
    FROM balances_filtered b
    LEFT JOIN labels l ON b.address = l.address
),

agg AS (
    SELECT
        date,
        token_address,
        symbol,
        token_class,
        sector,
        SUM(balance) AS supply,
        SUM(balance_usd) AS supply_usd
    FROM joined
    GROUP BY
        date,
        token_address,
        symbol,
        token_class,
        sector
)

SELECT
    date,
    token_address,
    symbol,
    token_class,
    sector,
    supply,
    supply_usd
FROM agg
WHERE date < today()
ORDER BY date, token_address, sector, token_class