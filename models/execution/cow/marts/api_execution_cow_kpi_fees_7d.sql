{{
  config(
    materialized='view',
    tags=['production','execution','cow','kpi','tier0',
          'api:cow_kpi_fees_7d','granularity:last_7d']
  )
}}

-- 7-day CoW Protocol revenue, filtered to fee_source = 'api' (surplus-based
-- fees, Sep 2024+). On-chain feeAmount values pre-Sep 2024 reflect signed
-- maxima under the old fee-subsidy model and are excluded. See
-- api_execution_cow_fees_ts for the full rationale.

WITH
recent AS (
    SELECT sum(fee_usd) AS v
    FROM {{ ref('fct_execution_cow_trades') }}
    WHERE toDate(block_timestamp) >= today() - INTERVAL 7 DAY
      AND toDate(block_timestamp) < today()
      AND fee_source = 'api'
),
prior AS (
    SELECT sum(fee_usd) AS v
    FROM {{ ref('fct_execution_cow_trades') }}
    WHERE toDate(block_timestamp) >= today() - INTERVAL 14 DAY
      AND toDate(block_timestamp) < today() - INTERVAL 7 DAY
      AND fee_source = 'api'
)
SELECT
    round((SELECT v FROM recent), 2)                                             AS value,
    round(((SELECT v FROM recent) - (SELECT v FROM prior))
          / nullIf((SELECT v FROM prior), 0) * 100, 1)                           AS change_pct
