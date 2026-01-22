{{ 
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    engine='ReplacingMergeTree()',
    order_by='(date, token_address, address)',
    partition_by='toStartOfMonth(date)',
    unique_key='(date, token_address, address)',
    settings={ 'allow_nullable_key': 1 },
    tags=['dev','execution','tokens','balances_daily']
  ) 
}}

-- depends_on: {{ ref('int_execution_tokens_address_diffs_daily') }}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}
{% set symbol = var('symbol', none) %}
{% set symbol_exclude = var('symbol_exclude', none) %}


WITH deltas AS (
    SELECT
        date,
        token_address,
        symbol,
        token_class,
        address,
        net_delta_raw
    FROM {{ ref('int_execution_tokens_address_diffs_daily') }}
    WHERE date < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(date) >= toDate('{{ start_month }}')
        AND toStartOfMonth(date) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('date', 'date', 'true') }}
      {% endif %}
      {{ symbol_filter('symbol', symbol, 'include') }}
      {{ symbol_filter('symbol', symbol_exclude, 'exclude') }}
      
),

overall_max_date AS (
    SELECT 
    {% if end_month %}
        least(toLastDayOfMonth(toDate('{{ end_month }}')), yesterday())
    {% else %} 
    (
        SELECT max(toDate(date)) 
        FROM {{ ref('int_execution_tokens_address_diffs_daily') }}
        {{ apply_monthly_incremental_filter('date', 'date') }}
    )
    {% endif %} AS max_date
),

{% if is_incremental() %}
current_partition AS (
    SELECT 
        max(toStartOfMonth(date)) AS month
        ,max(date)  AS max_date
    FROM {{ this }}
    WHERE 1=1
      {{ symbol_filter('symbol', symbol, 'include') }}
      {{ symbol_filter('symbol', symbol_exclude, 'exclude') }}
),
prev_balances AS (
    SELECT 
        t1.token_address,
        t1.symbol,
        t1.token_class,
        t1.address,
        t1.balance_raw
    FROM {{ this }} t1
    CROSS JOIN current_partition t2
    WHERE 
        t1.date = t2.max_date
        {{ symbol_filter('t1.symbol', symbol, 'include') }}
        {{ symbol_filter('t1.symbol', symbol_exclude, 'exclude') }}
),

keys AS (
    SELECT DISTINCT 
        token_address,
        symbol,
        token_class,
        address
    FROM (
        SELECT
            token_address,
            symbol,
            token_class,
            address
        FROM prev_balances

        UNION ALL

        SELECT
            token_address,
            symbol,
            token_class,
            address
        FROM deltas
    )
),

calendar AS (
    SELECT
        k.token_address,
        k.symbol,
        k.token_class,
        k.address,
        addDays(cp.max_date + 1, offset) AS date
    FROM keys k
    CROSS JOIN current_partition cp
    CROSS JOIN overall_max_date o
    ARRAY JOIN range(
        dateDiff('day', cp.max_date, o.max_date)
    ) AS offset
),

{% else %}

calendar AS (
    SELECT
        token_address,
        symbol,
        token_class,
        address,
        addDays(min_date, offset) AS date
    FROM
    (
        SELECT
            d.token_address,
            d.symbol,
            d.token_class,
            d.address,
            min(d.date) AS min_date,
            dateDiff('day', min(d.date), any(o.max_date)) AS num_days
        FROM deltas d
        CROSS JOIN overall_max_date o
        GROUP BY 
            d.token_address,
            d.symbol,
            d.token_class,
            d.address
    )
    ARRAY JOIN range(num_days + 1) AS offset
),


{% endif %}


balances AS (
    SELECT
        c.date AS date,
        c.token_address AS token_address,
        c.symbol AS symbol,
        c.token_class AS token_class,
        c.address AS address,

        sum(COALESCE(d.net_delta_raw,toInt256(0))) OVER (
            PARTITION BY c.token_address, c.address
            ORDER BY c.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
        {% if is_incremental() %}
            + coalesce(p.balance_raw, toInt256(0)) 
        {% endif %}
        AS balance_raw
    FROM calendar c
    LEFT JOIN deltas d
      ON d.token_address = c.token_address
     AND d.address       = c.address
     AND d.date          = c.date
    {% if is_incremental() %}
    LEFT JOIN prev_balances p
      ON p.token_address = c.token_address
     AND p.address       = c.address
    {% endif %}
),

prices AS (
    SELECT
        p.date,
        p.symbol,
        p.price
    FROM {{ ref('int_execution_token_prices_daily') }} p
    WHERE date < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(date) >= toDate('{{ start_month }}')
        AND toStartOfMonth(date) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('date', 'date', 'true') }}
      {% endif %}
      {{ symbol_filter('symbol', symbol, 'include') }}
      {{ symbol_filter('symbol', symbol_exclude, 'exclude') }}
),

final AS (
    SELECT
        b.date AS date,
        b.token_address AS token_address,
        b.symbol AS symbol,
        b.token_class AS token_class,
        b.address AS address,
        b.balance_raw AS balance_raw,
        b.balance_raw/POWER(10, t.decimals) AS balance,
        (b.balance_raw/POWER(10, t.decimals)) * p.price AS balance_usd
    FROM balances b
    LEFT JOIN {{ ref('tokens_whitelist') }} t
      ON lower(t.address) = b.token_address
    LEFT JOIN prices p
      ON p.date = b.date
     AND upper(p.symbol) = upper(b.symbol)
    WHERE b.balance_raw != 0
)

SELECT
    date,
    token_address,
    symbol,
    token_class,
    address,
    balance_raw,
    balance,
    balance_usd
FROM final
