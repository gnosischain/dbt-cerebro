{{
    config(
        materialized='view',
        tags=['dev', 'execution', 'yields', 'staging']
    )
}}

{#-
  Normalized daily token prices keyed by uppercase symbol.
  Wraps int_execution_token_prices_daily with consistent types and naming.
  Referenced by enriched, fees, and TVL models.
-#}

SELECT
    toDate(date) AS date,
    nullIf(upper(trimBoth(symbol)), '') AS token,
    toFloat64(price) AS price_usd
FROM {{ ref('int_execution_token_prices_daily') }}
WHERE date < today()
