{{
  config(
    materialized='view',
    tags=['production','staging','crawlers_data']
  )
}}

SELECT
    order_uid,
    lower(fee_token)                        AS fee_token,
    fee_amount,
    fee_policies,
    ingested_at,

    -- Identify the surplus-generating policy (always first entry in the array).
    -- 'priceImprovement' = improvement over the quoted market price (limit orders).
    -- 'surplus'          = improvement over the clearing price (market orders).
    -- 'volume'           = flat volume tax only, no surplus component.
    CASE
        WHEN JSONHas(fee_policies, 1, 'policy', 'priceImprovement') THEN 'priceImprovement'
        WHEN JSONHas(fee_policies, 1, 'policy', 'surplus')          THEN 'surplus'
        ELSE 'volume'
    END                                     AS surplus_policy_type,

    -- CoW Protocol's cut of the found value (raw token atoms, string-encoded).
    -- Gross value found by solver = surplus_component_raw / surplus_factor.
    JSONExtractString(fee_policies, 1, 'amount') AS surplus_component_raw,

    -- Fraction of found value CoW keeps (e.g. 0.5 = 50%).
    CASE
        WHEN JSONHas(fee_policies, 1, 'policy', 'priceImprovement')
        THEN JSONExtractFloat(fee_policies, 1, 'policy', 'priceImprovement', 'factor')
        WHEN JSONHas(fee_policies, 1, 'policy', 'surplus')
        THEN JSONExtractFloat(fee_policies, 1, 'policy', 'surplus', 'factor')
        ELSE NULL
    END                                     AS surplus_factor

FROM {{ source('crawlers_data', 'cow_api_trade_fees') }} FINAL
WHERE fee_amount != '0'
  AND fee_amount != ''
