{{
  config(
    materialized='view',
    tags=['dev','execution','tier1','api:tokens_ubo_coverage','granularity:latest']
  )
}}

SELECT
    token_address,
    symbol,
    token_class,
    total_usd,
    pct_direct_terminal,
    pct_unwound_terminal,
    pct_unwound_other,
    pct_known_container,
    pct_unclassified,
    pct_unwound_total
FROM {{ ref('fct_execution_tokens_ubo_coverage_latest') }}
ORDER BY total_usd DESC
