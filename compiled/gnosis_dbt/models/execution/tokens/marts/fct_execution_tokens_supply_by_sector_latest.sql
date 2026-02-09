

WITH

latest_date AS (
    SELECT MAX(date) AS max_date
    FROM `dbt`.`fct_execution_tokens_metrics_daily`
    WHERE date < today()
),

sector_supply AS (
    SELECT
        token_class,
        sector,
        SUM(supply) AS value,
        SUM(supply_usd) AS value_usd
    FROM `dbt`.`int_execution_tokens_balances_by_sector_daily`
    CROSS JOIN latest_date
    WHERE date = latest_date.max_date
    GROUP BY token_class, sector
),

total_supply AS (
    SELECT 
        token_class,
        SUM(value) AS total,
        SUM(value_usd) AS total_usd
    FROM sector_supply
    GROUP BY token_class
)

SELECT
    ss.token_class,
    ss.sector,
    ss.value,
    ss.value_usd,
    ROUND(ss.value_usd / NULLIF(ts.total_usd, 0) * 100, 2) AS percentage
FROM sector_supply ss
INNER JOIN total_supply ts
    ON ss.token_class = ts.token_class
ORDER BY ss.token_class, ss.value_usd DESC