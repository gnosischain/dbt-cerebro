{{
  config(
    materialized='view',
    tags=['staging','execution','gpay']
  )
}}

SELECT
    address
    ,MIN(introduced_at) AS introduced_at
FROM {{ ref('int_crawlers_data_labels') }}
WHERE project = 'gpay'
GROUP BY address