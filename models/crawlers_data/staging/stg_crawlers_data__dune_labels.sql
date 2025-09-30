{{
  config(
    materialized='view',
    tags=['staging','crawlers_data']
  )
}}

WITH src AS (
  SELECT
    lower(address)  AS address,
    label,
    introduced_at
  FROM {{ source('playground_max','dune_labels') }}
),
ranked AS (
  SELECT
    address,
    label,
    introduced_at,
    row_number() OVER (
      PARTITION BY address
      ORDER BY introduced_at DESC
    ) AS rn
  FROM src
)
SELECT
  address,
  label         AS project,
  introduced_at
FROM ranked
WHERE rn = 1