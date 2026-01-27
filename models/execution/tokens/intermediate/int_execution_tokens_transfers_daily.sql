{{ 
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    engine='ReplacingMergeTree()',
    order_by='(date, token_address)',
    partition_by='toStartOfMonth(date)',
    unique_key='(date, token_address)',
    settings={ 'allow_nullable_key': 1 },
    tags=['dev','execution','tokens','transfers_daily']
  ) 
}}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}

WITH base AS (
    SELECT
        date,
        lower(token_address) AS token_address,
        symbol,
        lower("from")        AS from_address,
        lower("to")          AS to_address,
        amount_raw,
        transfer_count
    FROM {{ ref('int_execution_transfers_whitelisted_daily') }}
    WHERE date < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(date) >= toDate('{{ start_month }}')
        AND toStartOfMonth(date) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('date', 'date', true) }}
      {% endif %}
),

with_class AS (
    SELECT
        b.date,
        b.token_address,
        b.symbol,
        coalesce(w.token_class, 'OTHER') AS token_class,
        w.decimals,
        b.from_address,
        b.to_address,
        b.amount_raw,
        b.transfer_count
    FROM base b
    LEFT JOIN {{ ref('tokens_whitelist') }} w
      ON lower(w.address) = b.token_address
     AND b.date >= toDate(w.date_start)
     AND (w.date_end IS NULL OR b.date < toDate(w.date_end))
),

agg AS (
    SELECT
        date,
        token_address,
        any(symbol)      AS symbol,
        any(token_class) AS token_class,
        sum(amount_raw / POWER(10, COALESCE(decimals, 18))) AS volume_token,
        sum(transfer_count) AS transfer_count,
        groupBitmapState(cityHash64(from_address)) AS ua_bitmap_state,
        uniqExact(from_address)                    AS active_senders,
        uniqExact(to_address)                      AS unique_receivers
    FROM with_class
    GROUP BY date, token_address
)

SELECT
    date,
    token_address,
    symbol,
    token_class,
    volume_token,
    transfer_count,
    ua_bitmap_state,
    active_senders,
    unique_receivers
FROM agg
ORDER BY date, token_address