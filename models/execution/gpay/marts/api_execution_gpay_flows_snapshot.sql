{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier1','api:gpay_flows_snapshots','granularity:in_ranges']
  )
}}

SELECT
  window
  ,symbol
  ,from_label
  ,to_label
  ,amount_usd
  ,tf_cnt
FROM {{ ref('fct_execution_gpay_flows_snapshot') }}
ORDER BY days ASC 