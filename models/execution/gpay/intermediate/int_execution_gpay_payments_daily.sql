{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    engine='ReplacingMergeTree()',
    order_by='(date, wallet_address, token_address)',
    partition_by='toStartOfMonth(date)',
    unique_key='(date, wallet_address, token_address)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','execution','gpay','payments_daily']
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
        lower("from")         AS wallet_address,
        lower(token_address)  AS token_address,
        symbol,
        amount_raw,
        transfer_count
    FROM {{ ref('int_execution_transfers_whitelisted_daily') }}
    WHERE lower("to") = '0x4822521e6135cd2599199c83ea35179229a172ee'
      AND lower("from") IN (SELECT address FROM gpay_wallets)
      AND date < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(date) >= toDate('{{ start_month }}')
        AND toStartOfMonth(date) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('date', 'date', true) }}
      {% endif %}
),

with_meta AS (
    SELECT
        b.date,
        b.wallet_address,
        b.token_address,
        b.symbol,
        coalesce(w.token_class, 'OTHER') AS token_class,
        b.amount_raw,
        b.amount_raw / POWER(10, coalesce(w.decimals, 18)) AS amount,
        b.transfer_count AS payment_count
    FROM base b
    INNER JOIN {{ ref('tokens_whitelist') }} w
      ON lower(w.address) = b.token_address
     AND b.date >= toDate(w.date_start)
     AND (w.date_end IS NULL OR b.date < toDate(w.date_end))
)

SELECT
    m.date,
    m.wallet_address,
    m.token_address,
    m.symbol,
    m.token_class,
    m.amount_raw,
    m.amount,
    m.amount * coalesce(p.price, 0) AS amount_usd,
    m.payment_count
FROM with_meta m
LEFT JOIN {{ ref('int_execution_token_prices_daily') }} p
  ON p.date = m.date
 AND p.symbol = m.symbol
ORDER BY m.date, m.wallet_address, m.token_address
