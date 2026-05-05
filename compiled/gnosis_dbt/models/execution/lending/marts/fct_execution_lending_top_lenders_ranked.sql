



WITH

latest_date AS (
    SELECT max(date) AS max_date
    FROM `dbt`.`int_execution_lending_aave_user_balances_daily`
    WHERE date < today()
      AND balance > 0
),

ranked AS (
    SELECT
        protocol,
        reserve_address,
        symbol,
        user_address,
        balance,
        balance_usd,
        balance_usd / nullIf(sum(balance_usd) OVER (PARTITION BY protocol, symbol), 0) * 100
            AS pct_of_total,
        row_number() OVER (PARTITION BY protocol, symbol ORDER BY balance_usd DESC) AS rank
    FROM `dbt`.`int_execution_lending_aave_user_balances_daily`
    CROSS JOIN latest_date
    WHERE date = latest_date.max_date
      AND balance > 0
)

SELECT *
FROM ranked
WHERE rank <= 500