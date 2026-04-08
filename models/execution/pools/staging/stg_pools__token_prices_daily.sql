{{
    config(
        materialized='view',
        tags=['production', 'execution', 'pools', 'staging']
    )
}}

{#- Model documentation in schema.yml -#}

SELECT
    toDate(date) AS date,
    nullIf(upper(trimBoth(symbol)), '') AS token,
    toFloat64(price) AS price_usd
FROM {{ ref('int_execution_token_prices_daily') }}
WHERE date < today()
