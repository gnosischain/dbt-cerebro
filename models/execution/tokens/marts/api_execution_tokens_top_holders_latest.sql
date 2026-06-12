{{
  config(
    materialized='view',
    tags=['production','execution','tier1','api:tokens_top_holders', 'granularity:latest']
  )
}}

SELECT sub.*, (SELECT toDate(max(date)) FROM {{ ref('int_execution_tokens_balances_daily') }}) AS as_of_date
FROM (
SELECT
    rank,
    token_address,
    symbol,
    token_class,
    address,
    label,
    label_sector,
    balance,
    balance_usd,
    pct_of_total,
    cumulative_pct,
    change_usd_7d,
    unwound_from,
    protocols,
    is_terminal_ubo
FROM {{ ref('fct_execution_tokens_top_holders_latest') }}
ORDER BY token_address, rank
) AS sub
