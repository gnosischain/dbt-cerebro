{{
  config(
    materialized='view',
    tags=['deprecated','staging','crawlers_data']
  )
}}



SELECT
    label,
    block_date,
    supply
FROM {{ source('crawlers_data','dune_gno_supply') }}
