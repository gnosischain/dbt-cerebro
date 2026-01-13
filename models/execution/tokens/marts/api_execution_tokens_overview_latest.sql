{{
  config(
    materialized='view',
    tags=['dev','execution','tier1','api:tokens_overview', 'granularity:latest']
  )
}}

SELECT
    token_class,
    label,
    value,
    change_pct
FROM {{ ref('fct_execution_tokens_overview_by_class_latest') }}
ORDER BY token_class, label
