



WITH

latest_date AS (
    SELECT max(date) AS max_date
    FROM `dbt`.`int_execution_tokens_balances_daily`
    WHERE date < today()
      AND balance > 0
),

ranked AS (
    SELECT
        token_address,
        symbol,
        token_class,
        address,
        balance,
        balance_usd,
        balance_usd / nullIf(sum(balance_usd) OVER (PARTITION BY token_address), 0) * 100
            AS pct_of_total,
        row_number() OVER (PARTITION BY token_address ORDER BY balance_usd DESC) AS rank
    FROM `dbt`.`int_execution_tokens_balances_daily`
    CROSS JOIN latest_date
    WHERE date = latest_date.max_date
      AND balance > 0
      AND lower(address) != '0x0000000000000000000000000000000000000000'
)

SELECT *
FROM ranked
WHERE rank <= 500