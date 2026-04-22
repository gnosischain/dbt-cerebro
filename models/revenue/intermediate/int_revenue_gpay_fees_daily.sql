{% set settlement_address = '0x4822521e6135cd2599199c83ea35179229a172ee' %}

{% set fee_bps_eure  = 20  %}  {# 0.20% #}
{% set fee_bps_gbpe  = 20  %}  {# 0.20% #}
{% set fee_bps_usdce = 100 %}  {# 1.00% #}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

{{
  config(
    materialized='incremental',
    incremental_strategy=('append' if start_month else 'delete+insert'),
    engine='ReplacingMergeTree()',
    order_by='(date, symbol, user)',
    partition_by='toStartOfMonth(date)',
    unique_key='(date, symbol, user)',
    settings={'allow_nullable_key': 1},
    tags=['production','revenue','revenue_gpay']
  )
}}

WITH transfers AS (
    SELECT
        t.date,
        lower(t."from") AS user,
        t.symbol,
        multiIf(
            t.symbol = 'EURe',   toFloat64({{ fee_bps_eure  }}) / 10000.0,
            t.symbol = 'GBPe',   toFloat64({{ fee_bps_gbpe  }}) / 10000.0,
            t.symbol = 'USDC.e', toFloat64({{ fee_bps_usdce }}) / 10000.0,
            toFloat64(0)
        ) AS fee_rate,
        sum(toFloat64(t.amount_raw) / pow(10, w.decimals)) AS amount_native
    FROM {{ ref('int_execution_transfers_whitelisted_daily') }} t
    INNER JOIN {{ ref('tokens_whitelist') }} w
        ON lower(w.address) = t.token_address
       AND t.date >= w.date_start
       AND (w.date_end IS NULL OR t.date < w.date_end)
    WHERE t.date < today()
      AND lower(t."to") = '{{ settlement_address }}'
      AND t.symbol IN ('EURe','GBPe','USDC.e')
      AND t.amount_raw IS NOT NULL
      AND t."from" IS NOT NULL
      {% if start_month and end_month %}
        AND toStartOfMonth(t.date) >= toDate('{{ start_month }}')
        AND toStartOfMonth(t.date) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('t.date', 'date', true, lookback_days=2) }}
      {% endif %}
    GROUP BY t.date, lower(t."from"), t.symbol, fee_rate
),

prices AS (
    SELECT date, symbol, price
    FROM {{ ref('int_execution_token_prices_daily') }}
    WHERE price IS NOT NULL
)

SELECT
    tr.date,
    tr.user,
    tr.symbol,
    round(sum(tr.amount_native * tr.fee_rate), 8)           AS fees_native,
    round(sum(tr.amount_native * tr.fee_rate * p.price), 8) AS fees,
    round(sum(tr.amount_native * p.price), 6)               AS volume_usd
FROM transfers tr
LEFT JOIN prices p
    ON p.date = tr.date AND p.symbol = tr.symbol
GROUP BY tr.date, tr.user, tr.symbol
