{{
  config(
    materialized='view',
    tags=['production','execution','tier1','api:tokens_ubo_venue_breakdown','granularity:latest']
  )
}}

SELECT sub.*, (SELECT toDate(max(date)) FROM {{ ref('int_execution_tokens_balances_daily') }}) AS as_of_date
FROM (
SELECT token_address, symbol, token_class, venue, balance, balance_usd, percentage
FROM {{ ref('fct_execution_tokens_ubo_venue_breakdown_latest') }}
ORDER BY token_address, balance_usd DESC
) AS sub
