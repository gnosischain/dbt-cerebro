






WITH

balances_filtered AS (
    SELECT
        b.date,
        lower(b.token_address)                     AS token_address,
        upper(b.symbol)                            AS symbol,
        b.token_class,
        lower(b.address)                           AS address,
        cityHash64(lower(b.address)) % 1000        AS address_bucket,
        b.balance
    FROM `dbt`.`int_execution_tokens_balances_daily` b
    WHERE b.date < today()
      
        
  

      
      
),

bounds AS (
    
    SELECT
        min(date) AS min_date,
        max(date) AS max_date
    FROM balances_filtered
    
),

prev_state AS (
    
    SELECT
        cast('' AS String)  AS token_address,
        cast('' AS String)  AS symbol,
        cast('' AS String)  AS token_class,
        cast('' AS String)  AS address,
        toInt32(0)          AS address_bucket,
        cast(0  AS Float64) AS balance
    WHERE 0
    
),

seed_sparse AS (
    SELECT
        date,
        token_address,
        symbol,
        token_class,
        address,
        address_bucket,
        balance
    FROM balances_filtered

    UNION ALL

    SELECT
        addDays(b.min_date, -1) AS date,   
        p.token_address,
        p.symbol,
        p.token_class,
        p.address,
        p.address_bucket,
        p.balance
    FROM prev_state p
    CROSS JOIN bounds b
),

addr_pairs AS (
    SELECT
        token_address,
        symbol,
        token_class,
        address,
        address_bucket
    FROM seed_sparse
    GROUP BY
        token_address,
        symbol,
        token_class,
        address,
        address_bucket
),

calendar AS (
    SELECT
        toDate(
          arrayJoin(
            range(
              toUInt32(addDays(min_date, -1)),  
              toUInt32(max_date) + 1           
            )
          )
        ) AS date
    FROM bounds
),

addr_calendar AS (
    SELECT
        c.date,
        a.token_address,
        a.symbol,
        a.token_class,
        a.address,
        a.address_bucket
    FROM calendar c
    CROSS JOIN addr_pairs a
),

dense_balances AS (
    SELECT
        ac.date,
        ac.token_address,
        ac.symbol,
        ac.token_class,
        ac.address,
        ac.address_bucket,
        last_value(s.balance) IGNORE NULLS
          OVER (
            PARTITION BY ac.token_address, ac.address
            ORDER BY ac.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
          ) AS balance
    FROM addr_calendar ac
    LEFT JOIN seed_sparse s
      ON s.date          = ac.date
     AND s.token_address = ac.token_address
     AND s.address       = ac.address
),

priced AS (
    SELECT
        d.date,
        d.token_address,
        d.symbol,
        d.token_class,
        d.address,
        d.address_bucket,
        d.balance,
        p.price                          AS price_usd,
        d.balance * p.price              AS balance_usd
    FROM dense_balances d
    LEFT JOIN `dbt`.`int_execution_token_prices_daily` p
      ON p.date   = d.date
     AND p.symbol = d.symbol
    WHERE d.balance > 0
      AND d.date >= (SELECT min_date FROM bounds)  
      AND d.date <= (SELECT max_date FROM bounds)
      AND lower(d.address) != '0x0000000000000000000000000000000000000000'
),

bucketed AS (
    SELECT
        date,
        token_address,
        symbol,
        token_class,
        address,
        address_bucket,
        balance_usd,
        CASE
            WHEN balance_usd <       10       THEN '0-10'
            WHEN balance_usd <      100       THEN '10-100'
            WHEN balance_usd <     1000       THEN '100-1k'
            WHEN balance_usd <    10000       THEN '1k-10k'
            WHEN balance_usd <   100000       THEN '10k-100k'
            WHEN balance_usd <  1000000       THEN '100k-1M'
            ELSE                                  '1M+'
        END AS balance_bucket
    FROM priced
    WHERE balance_usd IS NOT NULL
),

agg AS (
    SELECT
        date,
        token_address,
        symbol,
        token_class,
        balance_bucket,
        address_bucket,
        countDistinct(address) AS holders_in_bucket,
        sum(balance_usd)       AS value_usd_in_bucket
    FROM bucketed
    GROUP BY
        date,
        token_address,
        symbol,
        token_class,
        balance_bucket,
        address_bucket
)

SELECT
    date,
    token_address,
    symbol,
    token_class,
    balance_bucket,
    address_bucket,
    holders_in_bucket,
    value_usd_in_bucket
FROM agg
WHERE date < today()