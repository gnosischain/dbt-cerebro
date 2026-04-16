{{
    config(
        materialized='view',
        tags=['production', 'execution', 'pools', 'staging']
    )
}}

SELECT
    lower(address) AS token_address,
    nullIf(trimBoth(symbol), '') AS token,
    decimals,
    date_start,
    date_end
FROM {{ ref('tokens_whitelist') }}
