{{
  config(
    materialized='view',
    tags=['production','execution','tier1','api:circles_v2_mints', 'granularity:daily']
  )
}}

SELECT
    date,
    n_mint_events,
    n_minters,
    amount_minted
FROM {{ ref('fct_execution_circles_v2_mints_daily') }}
WHERE date < today()
ORDER BY date DESC
