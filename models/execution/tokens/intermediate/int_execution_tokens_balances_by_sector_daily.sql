{{
  config(
    materialized='incremental',
    incremental_strategy=('append' if (var('start_month', none) or var('incremental_end_date', none)) else 'delete+insert'),
    engine='ReplacingMergeTree()',
    order_by='(date, token_address, sector, token_class)',
    partition_by='toStartOfMonth(date)',
    unique_key='(date, token_address, sector, token_class)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','execution','tokens','balances_by_sector_daily','refill_append']
  )
}}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}

WITH

balances_filtered AS (
    SELECT
        b.date,
        lower(b.token_address) AS token_address,
        b.symbol AS symbol,
        b.token_class,
        lower(b.address) AS address,
        b.balance,
        b.balance_usd
    FROM {{ ref('int_execution_tokens_balances_daily') }} b
    WHERE b.date < today()
      AND b.balance > 0
      AND lower(b.address) != '0x0000000000000000000000000000000000000000'
      {% if start_month and end_month %}
        AND toStartOfMonth(b.date) >= toDate('{{ start_month }}')
        AND toStartOfMonth(b.date) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('b.date', 'date', 'true') }}
      {% endif %}
),

balance_addresses AS (
    SELECT DISTINCT
        address
    FROM balances_filtered
),

labels_ranked AS (
    SELECT
        address,
        project,
        sector,
        introduced_at,
        row_number() OVER (
            PARTITION BY address
            ORDER BY introduced_at DESC, project DESC, sector DESC
        ) AS rn
    FROM {{ ref('int_crawlers_data_labels') }}
    WHERE address IN (SELECT address FROM balance_addresses)
),

labels AS (
    SELECT
        address,
        sector
    FROM labels_ranked
    WHERE rn = 1
),

joined AS (
    SELECT
        b.date,
        b.token_address,
        b.symbol,
        b.token_class,
        b.address,
        b.balance,
        b.balance_usd,
        coalesce(nullIf(trim(l.sector), ''), 'Unknown') AS sector
    FROM balances_filtered b
    LEFT JOIN labels l ON b.address = l.address
),

agg AS (
    SELECT
        date,
        token_address,
        symbol,
        token_class,
        sector,
        SUM(balance) AS supply,
        SUM(balance_usd) AS supply_usd
    FROM joined
    GROUP BY
        date,
        token_address,
        symbol,
        token_class,
        sector
)

SELECT
    date,
    token_address,
    symbol,
    token_class,
    sector,
    supply,
    supply_usd
FROM agg
WHERE date < today()
