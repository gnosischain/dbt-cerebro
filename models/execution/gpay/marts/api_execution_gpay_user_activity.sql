{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier0','api:gpay_user_activity']
  )
}}

SELECT
    wallet_address,
    block_timestamp AS timestamp,
    date,
    action,
    symbol,
    round(toFloat64(amount), 6)     AS amount,
    round(toFloat64(amount_usd), 2) AS amount_usd,
    counterparty
FROM {{ ref('int_execution_gpay_activity') }}
ORDER BY block_timestamp DESC
