{{ 
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    engine='ReplacingMergeTree()',
    order_by='(date, token_address)',
    partition_by='toStartOfMonth(date)',
    unique_key='(date, token_address)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','execution','tokens','transfers_daily']
  ) 
}}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}

WITH base AS (
    SELECT
        toDate(t.block_timestamp)      AS date,
        t.block_timestamp,
        lower(t.token_address)         AS token_address,
        t.symbol                       AS symbol,
        t.amount                       AS amount,
        t.amount_usd                   AS amount_usd,
        lower(t."from")                AS from_address,
        lower(t."to")                  AS to_address
    FROM {{ ref('int_transfers_erc20_whitelisted') }} t
    WHERE t.block_timestamp < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(t.block_timestamp) >= toDate('{{ start_month }}')
        AND toStartOfMonth(t.block_timestamp) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('t.block_timestamp', 'date', true) }}
      {% endif %}
),

with_class AS (
    SELECT
        b.date,
        b.token_address,
        b.symbol,
        coalesce(w.token_class, 'OTHER') AS token_class,
        b.amount,
        b.amount_usd,
        b.from_address,
        b.to_address
    FROM base b
    LEFT JOIN {{ ref('tokens_whitelist') }} w
      ON lower(w.address) = b.token_address
),

agg AS (
    SELECT
        date,
        token_address,
        any(symbol)      AS symbol,
        any(token_class) AS token_class,

        sum(amount)      AS volume_token,
        sum(amount_usd)  AS volume_usd,

        count()          AS transfer_count,

        groupBitmapState(cityHash64(from_address)) AS ua_bitmap_state,
        uniqExact(from_address)                    AS active_senders,

        uniqExact(to_address)                      AS unique_receivers
    FROM with_class
    GROUP BY
        date,
        token_address
)

SELECT
    date,
    token_address,
    symbol,
    token_class,
    volume_token,
    volume_usd,
    transfer_count,
    ua_bitmap_state,
    active_senders,
    unique_receivers
FROM agg
ORDER BY date, token_address