{{
  config(
    materialized='table',
    tags=['dev','execution','stablecoins','distribution']
  )
}}

WITH

latest_date AS (
    SELECT MAX(date) AS max_date
    FROM {{ ref('fct_execution_tokens_metrics_daily') }}
    WHERE date < today()
),

stablecoins_supply AS (
    SELECT
        symbol AS token,
        supply AS value
    FROM {{ ref('fct_execution_tokens_metrics_daily') }}
    CROSS JOIN latest_date
    WHERE date = latest_date.max_date
      AND token_class = 'STABLECOIN'
),

total_supply AS (
    SELECT SUM(value) AS total
    FROM stablecoins_supply
)

SELECT
    token,
    value,
    ROUND(value / NULLIF(total, 0) * 100, 2) AS percentage
FROM stablecoins_supply
CROSS JOIN total_supply
ORDER BY value DESC

