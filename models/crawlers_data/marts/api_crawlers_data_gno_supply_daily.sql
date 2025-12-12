{{ 
  config(
    materialized='view', 
    tags=['production','crawlers_data', 'tier1', 'api:gno_supply', 'granularity:daily']) 
}}

SELECT
  label,
  block_date AS date,
  supply    
FROM {{ ref('stg_crawlers_data__dune_gno_supply') }}
ORDER BY date, label