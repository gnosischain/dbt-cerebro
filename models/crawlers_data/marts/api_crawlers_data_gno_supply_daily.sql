{{
  config(
    materialized='view',
    tags=['deprecated','crawlers_data'])
}}

SELECT
  label,
  block_date AS date,
  supply    
FROM {{ ref('stg_crawlers_data__dune_gno_supply') }}
ORDER BY date, label