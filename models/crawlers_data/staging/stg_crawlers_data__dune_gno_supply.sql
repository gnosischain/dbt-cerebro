{{
  config(
    materialized='view',
    tags=['production','staging','crawlers_data']
  )
}}



SELECT
    label,
    block_date,
    supply
FROM {{ source('crawlers_data','dune_gno_supply') }}
