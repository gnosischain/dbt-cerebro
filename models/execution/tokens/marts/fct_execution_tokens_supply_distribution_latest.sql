{{
  config(
    materialized='table',
    tags=['dev','execution','tokens','distribution']
  )
}}

WITH

latest_date AS (
    SELECT MAX(date) AS max_date
    FROM {{ ref('fct_execution_tokens_metrics_daily') }}
    WHERE date < today()
),

tokens_supply AS (
    SELECT
        token_class,
        symbol AS token,
        supply AS value
    FROM {{ ref('fct_execution_tokens_metrics_daily') }}
    CROSS JOIN latest_date
    WHERE date = latest_date.max_date
),

total_supply AS (
    SELECT 
        token_class,
        SUM(value) AS total
    FROM tokens_supply
    GROUP BY token_class
)

SELECT
    ts.token_class,
    ts.token,
    ts.value,
    ROUND(ts.value / NULLIF(tot.total, 0) * 100, 2) AS percentage
FROM tokens_supply ts
INNER JOIN total_supply tot
    ON ts.token_class = tot.token_class
ORDER BY ts.token_class, ts.value DESC
