{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    engine='ReplacingMergeTree()',
    order_by='(date, token_address, balance_bucket, address_bucket)',
    partition_by='toStartOfMonth(date)',
    unique_key='(date, token_address, balance_bucket, address_bucket)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','execution','tokens','balance_cohorts_daily']
  )
}}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}
{% set address_bucket_start = var('address_bucket_start', none) %}
{% set address_bucket_end   = var('address_bucket_end', none) %}

WITH

balances_filtered AS (
    SELECT
        b.date,
        lower(b.token_address)                     AS token_address,
        upper(b.symbol)                            AS symbol,
        b.token_class,
        lower(b.address)                           AS address,
        cityHash64(lower(b.address)) % 1000        AS address_bucket,
        b.balance
    FROM {{ ref('int_execution_tokens_balances_daily') }} b
    WHERE b.date < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(b.date) >= toDate('{{ start_month }}')
        AND toStartOfMonth(b.date) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('b.date', 'date', 'true') }}
      {% endif %}
      {% if address_bucket_start is not none and address_bucket_end is not none %}
        AND (cityHash64(lower(b.address)) % 1000)
              BETWEEN {{ address_bucket_start }} AND {{ address_bucket_end }}
      {% endif %}
),

bounds AS (
    {% if start_month %}
    SELECT
        toDate('{{ start_month }}') AS min_date,
        least(
          addMonths(toDate('{{ start_month }}'), 1) - 1,
          addDays(today(), -1)
        ) AS max_date
    {% else %}
    SELECT
        min(date) AS min_date,
        max(date) AS max_date
    FROM balances_filtered
    {% endif %}
),

prev_state AS (
    {% if is_incremental() %}
    SELECT
        lower(b.token_address)                     AS token_address,
        upper(b.symbol)                            AS symbol,
        b.token_class,
        lower(b.address)                           AS address,
        cityHash64(lower(b.address)) % 1000        AS address_bucket,
        argMax(b.balance, b.date)                  AS balance
    FROM {{ ref('int_execution_tokens_balances_daily') }} b
    WHERE b.date < (SELECT min_date FROM bounds)
      AND b.balance != 0
      {% if address_bucket_start is not none and address_bucket_end is not none %}
        AND (cityHash64(lower(b.address)) % 1000)
              BETWEEN {{ address_bucket_start }} AND {{ address_bucket_end }}
      {% endif %}
    GROUP BY
        token_address,
        symbol,
        b.token_class,
        address,
        address_bucket
    {% else %}
    SELECT
        cast('' AS String)  AS token_address,
        cast('' AS String)  AS symbol,
        cast('' AS String)  AS token_class,
        cast('' AS String)  AS address,
        toInt32(0)          AS address_bucket,
        cast(0  AS Float64) AS balance
    WHERE 0
    {% endif %}
),

seed_sparse AS (
    SELECT
        date,
        token_address,
        symbol,
        token_class,
        address,
        address_bucket,
        balance
    FROM balances_filtered

    UNION ALL

    SELECT
        addDays(b.min_date, -1) AS date,   
        p.token_address,
        p.symbol,
        p.token_class,
        p.address,
        p.address_bucket,
        p.balance
    FROM prev_state p
    CROSS JOIN bounds b
),

addr_pairs AS (
    SELECT
        token_address,
        symbol,
        token_class,
        address,
        address_bucket
    FROM seed_sparse
    GROUP BY
        token_address,
        symbol,
        token_class,
        address,
        address_bucket
),

calendar AS (
    SELECT
        toDate(
          arrayJoin(
            range(
              toUInt32(addDays(min_date, -1)),  
              toUInt32(max_date) + 1           
            )
          )
        ) AS date
    FROM bounds
),

addr_calendar AS (
    SELECT
        c.date,
        a.token_address,
        a.symbol,
        a.token_class,
        a.address,
        a.address_bucket
    FROM calendar c
    CROSS JOIN addr_pairs a
),

dense_balances AS (
    SELECT
        ac.date,
        ac.token_address,
        ac.symbol,
        ac.token_class,
        ac.address,
        ac.address_bucket,
        last_value(s.balance) IGNORE NULLS
          OVER (
            PARTITION BY ac.token_address, ac.address
            ORDER BY ac.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
          ) AS balance
    FROM addr_calendar ac
    LEFT JOIN seed_sparse s
      ON s.date          = ac.date
     AND s.token_address = ac.token_address
     AND s.address       = ac.address
),

priced AS (
    SELECT
        d.date,
        d.token_address,
        d.symbol,
        d.token_class,
        d.address,
        d.address_bucket,
        d.balance,
        p.price                          AS price_usd,
        d.balance * p.price              AS balance_usd
    FROM dense_balances d
    LEFT JOIN {{ ref('int_execution_token_prices_daily') }} p
      ON p.date   = d.date
     AND p.symbol = d.symbol
    WHERE d.balance > 0
      AND d.date >= (SELECT min_date FROM bounds)  
      AND d.date <= (SELECT max_date FROM bounds)
      AND lower(d.address) != '0x0000000000000000000000000000000000000000'
),

bucketed AS (
    SELECT
        date,
        token_address,
        symbol,
        token_class,
        address,
        address_bucket,
        balance_usd,
        CASE
            WHEN balance_usd <       10       THEN '0-10'
            WHEN balance_usd <      100       THEN '10-100'
            WHEN balance_usd <     1000       THEN '100-1k'
            WHEN balance_usd <    10000       THEN '1k-10k'
            WHEN balance_usd <   100000       THEN '10k-100k'
            WHEN balance_usd <  1000000       THEN '100k-1M'
            ELSE                                  '1M+'
        END AS balance_bucket
    FROM priced
    WHERE balance_usd IS NOT NULL
),

agg AS (
    SELECT
        date,
        token_address,
        symbol,
        token_class,
        balance_bucket,
        address_bucket,
        countDistinct(address) AS holders_in_bucket,
        sum(balance_usd)       AS value_usd_in_bucket
    FROM bucketed
    GROUP BY
        date,
        token_address,
        symbol,
        token_class,
        balance_bucket,
        address_bucket
)

SELECT
    date,
    token_address,
    symbol,
    token_class,
    balance_bucket,
    address_bucket,
    holders_in_bucket,
    value_usd_in_bucket
FROM agg
WHERE date < today()