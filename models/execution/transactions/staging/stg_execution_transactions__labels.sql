{{
  config(
    materialized='view',
    tags=['production','execution','transactions']
  )
}}

WITH 
src AS (
  SELECT
    lower(address) AS addr_raw,
    IF(startsWith(lower(address), '0x'), lower(address), CONCAT('0x', lower(address))) AS address_norm,
    label,
    introduced_at
  FROM {{ source('playground_max','dune_labels') }}
)
SELECT
  anyHeavy(address_norm)               AS address,      
  argMax(label, introduced_at)         AS project,      
  MAX(introduced_at)                   AS introduced_at
FROM src
GROUP BY addr_raw
