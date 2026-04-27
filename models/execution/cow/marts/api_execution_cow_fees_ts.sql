{{
  config(
    materialized='view',
    tags=['production','execution','cow','tier1',
          'api:cow_fees_ts','granularity:daily']
  )
}}

SELECT
    toDate(block_timestamp)                                                      AS date,
    coalesce(fee_source, 'unknown')                                              AS label,
    sum(fee_usd)                                                                 AS value
FROM {{ ref('fct_execution_cow_trades') }}
WHERE toDate(block_timestamp) < today()
  AND fee_usd > 0
GROUP BY date, label
ORDER BY date, label
