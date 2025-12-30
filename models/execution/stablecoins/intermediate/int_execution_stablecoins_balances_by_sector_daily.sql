{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    engine='ReplacingMergeTree()',
    order_by='(date, token_address, sector)',
    partition_by='toStartOfMonth(date)',
    unique_key='(date, token_address, sector)',
    settings={ 'allow_nullable_key': 1 },
    tags=['dev','execution','stablecoins','balances_by_sector_daily']
  )
}}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}

WITH

balances_filtered AS (
    SELECT
        b.date,
        lower(b.token_address) AS token_address,
        upper(b.symbol) AS symbol,
        lower(b.address) AS address,
        b.balance,
        b.balance_usd
    FROM {{ ref('int_execution_tokens_balances_daily') }} b
    WHERE b.date < today()
      AND b.token_class = 'STABLECOIN'
      AND b.balance > 0
      AND lower(b.address) != '0x0000000000000000000000000000000000000000'
      {% if start_month and end_month %}
        AND toStartOfMonth(b.date) >= toDate('{{ start_month }}')
        AND toStartOfMonth(b.date) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('b.date', 'date', 'true') }}
      {% endif %}
),

labels AS (
    SELECT
        lower(address) AS address,
        sector
    FROM {{ ref('int_crawlers_data_labels') }}
),

joined AS (
    SELECT
        b.date,
        b.token_address,
        b.symbol,
        b.address,
        b.balance,
        b.balance_usd,
        COALESCE(nullIf(trim(l.sector), ''), 'Unknown') AS sector
    FROM balances_filtered b
    LEFT JOIN labels l ON b.address = l.address
),

agg AS (
    SELECT
        date,
        token_address,
        symbol,
        sector,
        SUM(balance) AS supply,
        SUM(balance_usd) AS supply_usd
    FROM joined
    GROUP BY
        date,
        token_address,
        symbol,
        sector
)

SELECT
    date,
    token_address,
    symbol,
    sector,
    supply,
    supply_usd
FROM agg
WHERE date < today()
ORDER BY date, token_address, sector

