{{
    config(
        materialized='view',
        tags=['production', 'execution', 'pools', 'staging']
    )
}}

{#-
  Normalized token metadata from the whitelist seed.
  Single source of truth for token_address → symbol mapping, decimals,
  and validity window. Referenced by all yields models that need token info.
-#}

SELECT
    lower(address) AS token_address,
    nullIf(upper(trimBoth(symbol)), '') AS token,
    decimals,
    date_start,
    date_end
FROM {{ ref('tokens_whitelist') }}
