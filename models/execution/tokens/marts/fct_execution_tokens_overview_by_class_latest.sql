{{
  config(
    materialized='table',
    tags=['production','execution','tokens','overview']
  )
}}

WITH

latest_date AS (
    SELECT MAX(date) AS max_date
    FROM {{ ref('fct_execution_tokens_metrics_daily') }}
    WHERE date < today()
),

supply_latest AS (
    SELECT
        token_class,
        SUM(supply) AS value
    FROM {{ ref('fct_execution_tokens_metrics_daily') }}
    CROSS JOIN latest_date
    WHERE date = latest_date.max_date
    GROUP BY token_class
),

supply_7d AS (
    SELECT
        token_class,
        SUM(supply) AS value
    FROM {{ ref('fct_execution_tokens_metrics_daily') }}
    CROSS JOIN latest_date
    WHERE date = subtractDays(latest_date.max_date, 7)
    GROUP BY token_class
),

holders_latest AS (
    SELECT
        token_class,
        CAST(COUNT(DISTINCT address) AS Float64) AS value
    FROM {{ ref('int_execution_tokens_balances_daily') }}
    CROSS JOIN latest_date
    WHERE date = latest_date.max_date
      AND balance_raw > 0
    GROUP BY token_class
),

holders_7d AS (
    SELECT
        token_class,
        CAST(COUNT(DISTINCT address) AS Float64) AS value
    FROM {{ ref('int_execution_tokens_balances_daily') }}
    CROSS JOIN latest_date
    WHERE date = subtractDays(latest_date.max_date, 7)
      AND balance_raw > 0
    GROUP BY token_class
),

info_latest AS (
    SELECT 
        token_class,
        'supply_total' AS label, 
        value
    FROM supply_latest
    UNION ALL
    SELECT 
        token_class,
        'holders_total' AS label, 
        value
    FROM holders_latest
),

info_7d AS (
    SELECT 
        token_class,
        'supply_total' AS label, 
        value
    FROM supply_7d
    UNION ALL
    SELECT 
        token_class,
        'holders_total' AS label, 
        value
    FROM holders_7d
)

SELECT
    t1.token_class,
    t1.label,
    t1.value AS value,
    IF(t1.value = 0 AND t2.value = 0, 0, 
       ROUND((COALESCE(t1.value / NULLIF(t2.value, 0), 0) - 1) * 100, 1)
    ) AS change_pct
FROM info_latest t1
INNER JOIN info_7d t2
    ON t1.token_class = t2.token_class
    AND t1.label = t2.label
ORDER BY t1.token_class, t1.label
