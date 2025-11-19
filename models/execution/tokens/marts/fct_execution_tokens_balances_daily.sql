{{ 
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    engine='ReplacingMergeTree()',
    order_by='(date, token_address, address)',
    partition_by='toStartOfMonth(date)',
    unique_key='(date, token_address, address)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','execution','tokens','balances_daily']
  ) 
}}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}

WITH deltas AS (
    SELECT
        date,
        token_address,
        symbol,
        token_class,
        address,
        net_delta
    FROM {{ ref('int_execution_tokens_address_deltas_daily') }}
    WHERE date < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(date) >= toDate('{{ start_month }}')
        AND toStartOfMonth(date) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('date', 'date', 'true') }}
      {% endif %}
),


bounds AS (
    SELECT
        min(date) AS min_date,
        max(date) AS max_date
    FROM deltas
),

prev_balances AS (
    {% if is_incremental() %}

    SELECT
        token_address,
        address,
        max(balance) AS prev_balance
    FROM {{ this }}
    WHERE date < (SELECT min_date FROM bounds)
    GROUP BY
        token_address,
        address

    {% else %}

    SELECT
        cast('' AS String)   AS token_address,
        cast('' AS String)   AS address,
        cast(0  AS Float64)  AS prev_balance
    WHERE 0

    {% endif %}
),

calc AS (
    SELECT
        d.date,
        d.token_address,
        d.symbol,
        d.token_class,
        d.address,

        coalesce(p.prev_balance, 0) +
        sum(d.net_delta) OVER (
            PARTITION BY d.token_address, d.address
            ORDER BY d.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS balance
    FROM deltas d
    LEFT JOIN prev_balances p
      ON p.token_address = d.token_address
     AND p.address       = d.address
)

SELECT
    date,
    token_address,
    symbol,
    token_class,
    address,
    balance
FROM calc
ORDER BY
    date,
    token_address,
    address