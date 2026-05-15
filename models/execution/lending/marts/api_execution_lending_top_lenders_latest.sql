{{
  config(
    materialized='view',
    tags=['dev','execution','tier1','api:lending_top_lenders', 'granularity:latest']
  )
}}

SELECT
    rank,
    protocol,
    symbol,
    user_address,
    label,
    balance,
    balance_usd,
    pct_of_total,
    cumulative_pct,
    change_usd_7d
FROM {{ ref('fct_execution_lending_top_lenders_latest') }}
ORDER BY protocol, symbol, rank
