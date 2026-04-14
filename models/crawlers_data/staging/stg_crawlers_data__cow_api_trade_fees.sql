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
    ingested_at
FROM {{ source('crawlers_data', 'cow_api_trade_fees') }}
WHERE fee_amount != '0'
  AND fee_amount != ''
