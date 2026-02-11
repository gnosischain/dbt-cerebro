{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    engine='ReplacingMergeTree()',
    order_by='(date, wallet_address)',
    partition_by='toStartOfMonth(date)',
    unique_key='(date, wallet_address)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','execution','gpay','cashback_daily']
  )
}}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}

WITH gpay_wallets AS (
    SELECT address
    FROM {{ ref('stg_gpay__wallets') }}
),

base AS (
    SELECT
        date,
        lower("to") AS wallet_address,
        amount_raw,
        transfer_count
    FROM {{ ref('int_execution_transfers_whitelisted_daily') }}
    WHERE lower("from") = '0xcdf50be9061086e2ecfe6e4a1bf9164d43568eec'
      AND lower(token_address) = '0x9c58bacc331c9aa871afd802db6379a98e80cedb'
      AND lower("to") IN (SELECT address FROM gpay_wallets)
      AND date < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(date) >= toDate('{{ start_month }}')
        AND toStartOfMonth(date) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('date', 'date', true) }}
      {% endif %}
)

SELECT
    b.date,
    b.wallet_address,
    b.amount_raw,
    b.amount_raw / POWER(10, 18) AS amount,
    (b.amount_raw / POWER(10, 18)) * coalesce(p.price, 0) AS amount_usd,
    b.transfer_count
FROM base b
LEFT JOIN {{ ref('int_execution_token_prices_daily') }} p
  ON p.date = b.date
 AND p.symbol = 'GNO'
ORDER BY b.date, b.wallet_address
