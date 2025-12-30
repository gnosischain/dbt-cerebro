{{
  config(
    materialized='table',
    tags=['dev','execution','stablecoins','supply_by_sector']
  )
}}

WITH

latest_date AS (
    SELECT MAX(date) AS max_date
    FROM {{ ref('fct_execution_tokens_metrics_daily') }}
    WHERE date < today()
),

sector_supply AS (
    SELECT
        sector,
        SUM(supply) AS value,
        SUM(supply_usd) AS value_usd
    FROM {{ ref('int_execution_stablecoins_balances_by_sector_daily') }}
    CROSS JOIN latest_date
    WHERE date = latest_date.max_date
    GROUP BY sector
),

total_supply AS (
    SELECT SUM(value) AS total
    FROM sector_supply
)

SELECT
    sector,
    value,
    value_usd,
    ROUND(value / NULLIF(total, 0) * 100, 2) AS percentage
FROM sector_supply
CROSS JOIN total_supply
ORDER BY value DESC

