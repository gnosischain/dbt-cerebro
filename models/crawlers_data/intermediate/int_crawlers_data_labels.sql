{{
  config(
    materialized='table',
    tags=['production','crawlers_data','labels']
  )
}}

SELECT 
    address, 
    anyLast(project) AS project
FROM (
    SELECT 
        lower(address) AS address,
        project
    FROM {{ ref('stg_crawlers_data__dune_labels') }}
)
GROUP BY address