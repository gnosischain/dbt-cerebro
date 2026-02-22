{{
  config(
    materialized='view',
    tags=['staging','execution','gpay']
  )
}}

SELECT DISTINCT
    address
FROM {{ ref('int_crawlers_data_labels') }}
WHERE project = 'gpay'
