{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier0','api:gpay_wallet_list','granularity:all_time']
  )
}}

SELECT address AS wallet_address
FROM {{ ref('stg_gpay__wallets') }}
ORDER BY address
