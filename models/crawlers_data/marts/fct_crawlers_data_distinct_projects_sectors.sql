{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(project, sector)',
    tags=['production','execution','transactions']
  )
}}

SELECT DISTINCT
  project,
  sector
FROM {{ ref('int_crawlers_data_labels') }}
WHERE project IS NOT NULL
  AND sector  IS NOT NULL