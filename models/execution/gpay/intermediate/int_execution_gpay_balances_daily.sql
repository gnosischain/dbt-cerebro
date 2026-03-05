{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    engine='ReplacingMergeTree()',
    order_by='(date, address, symbol)',
    partition_by='toStartOfMonth(date)',
    unique_key='(date, address, symbol)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','execution','gpay','balances_daily']
  )
}}

WITH gpay_wallets AS (
    SELECT address
    FROM {{ ref('stg_gpay__wallets') }}
)

SELECT
    b.date,
    b.address,
    b.symbol,
    b.balance,
    b.balance_usd
FROM {{ ref('int_execution_tokens_balances_daily') }} b
WHERE b.address IN (SELECT address FROM gpay_wallets)
  AND b.date >= '2023-06-01'
  AND b.date < today()
  {{ apply_monthly_incremental_filter('b.date', 'date', true) }}
