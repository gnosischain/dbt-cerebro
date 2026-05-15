{{
  config(
    materialized='view',
    tags=['production','execution','tier1','api:circles_v2_mints', 'granularity:daily']
  )
}}

SELECT
    date,
    mint_kind,
    n_mint_events,
    n_minters,
    amount_minted
FROM {{ ref('int_execution_circles_v2_mints_daily') }}
WHERE date < today()
ORDER BY date DESC, mint_kind
