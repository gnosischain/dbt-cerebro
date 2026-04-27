{{
  config(
    materialized='view',
    tags=['production','execution','cow','tier1',
          'api:cow_fees_ts','granularity:daily']
  )
}}

-- Daily CoW Protocol revenue on Gnosis Chain.
-- Filtered to fee_source = 'api' (the surplus-based fee model introduced
-- Sep 2024). The on-chain feeAmount field on the GPv2 Trade event predates
-- this and represented the user's *signed-maximum* fee under CoW v1's
-- fee-subsidy model — not actual protocol revenue. Including those values
-- here would overstate revenue for 2021–mid-2024 by orders of magnitude
-- relative to what the protocol actually kept, so they are excluded.

SELECT
    toDate(block_timestamp)                                                      AS date,
    sum(fee_usd)                                                                 AS value
FROM {{ ref('fct_execution_cow_trades') }}
WHERE toDate(block_timestamp) < today()
  AND fee_source = 'api'
  AND fee_usd > 0
GROUP BY date
ORDER BY date
