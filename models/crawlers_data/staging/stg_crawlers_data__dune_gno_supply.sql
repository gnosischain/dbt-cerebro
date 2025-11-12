{{
  config(
    materialized='view',
    tags=['staging','crawlers_data']
  )
}}



SELECT
    label,
    block_date,
    supply
FROM {{ source('crawlers_data','dune_gno_supply') }}
