{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    engine='ReplacingMergeTree()',
    order_by='address',
    unique_key='address',
    settings={'allow_nullable_key': 1},
    tags=['production','execution','gpay']
  )
}}


{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}
{% set spender     = '0x4822521e6135cd2599199c83ea35179229a172ee' %}

WITH operational AS (
    SELECT lower(address) AS address
    FROM {{ ref('gpay_operational_wallets') }}
),

activated_wallets AS (
    SELECT
        "from"      AS pay_wallet,
        MIN(date)   AS activation_date
    FROM {{ ref('int_execution_transfers_whitelisted_daily') }}
    WHERE "to" = '{{ spender }}'
      AND date >= toDate('2023-12-01')
      {% if start_month and end_month %}
      AND toStartOfMonth(date) >= toDate('{{ start_month }}')
      AND toStartOfMonth(date) <= toDate('{{ end_month }}')
      {% else %}
      {{ apply_monthly_incremental_filter('date', 'activation_date', add_and=True) }}
      {% endif %}
    GROUP BY "from"
    HAVING pay_wallet NOT IN (SELECT address FROM operational)
    {% if is_incremental() %}
      AND pay_wallet NOT IN (SELECT address FROM {{ this }})
    {% endif %}
),

safe_setup AS (
    SELECT
        safe_address,
        MIN(block_timestamp) AS creation_time
    FROM {{ ref('int_execution_safes_owner_events') }}
    WHERE event_kind = 'safe_setup'
      AND safe_address IN (SELECT pay_wallet FROM activated_wallets)
    GROUP BY safe_address
)

SELECT
    s.safe_address   AS address,
    a.activation_date AS activation_date,
    s.creation_time AS creation_time
FROM safe_setup s
INNER JOIN activated_wallets a
    ON a.pay_wallet = s.safe_address
