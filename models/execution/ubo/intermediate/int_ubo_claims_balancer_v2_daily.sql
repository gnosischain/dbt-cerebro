{{
    config(
        materialized='incremental',
        incremental_strategy=('append' if (var('start_month', none) or var('incremental_end_date', none)) else 'delete+insert'),
        engine='ReplacingMergeTree()',
        order_by='(date, container_address, ubo_address, token_address)',
        unique_key='(date, container_address, ubo_address, token_address)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        pre_hook=["SET join_use_nulls = 0"],
        post_hook=["SET join_use_nulls = 0"],
        tags=['dev','execution','ubo','claims','balancer']
    )
}}


{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}

WITH

daily_deltas AS (
    SELECT
        toDate(liq.block_timestamp)     AS date,
        lower(liq.provider)             AS ubo_address,
        tw.symbol                       AS symbol,
        sum(if(liq.event_type = 'mint',
                toInt256(liq.amount_raw),
               -toInt256(liq.amount_raw))) AS daily_delta_raw
    FROM {{ ref('stg_pools__dex_liquidity_balancer_v2') }} liq
    INNER JOIN {{ ref('tokens_whitelist') }} tw
        ON  lower(tw.address)           = lower(liq.token_address)
        AND toDate(liq.block_timestamp) >= tw.date_start
        AND (tw.date_end IS NULL OR toDate(liq.block_timestamp) < tw.date_end)
    WHERE liq.block_timestamp < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(liq.block_timestamp) >= toDate('{{ start_month }}')
        AND toStartOfMonth(liq.block_timestamp) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('liq.block_timestamp', 'date', 'true') }}
      {% endif %}
    GROUP BY date, ubo_address, symbol
),

overall_max_date AS (
    SELECT
        {% if end_month %}
            toLastDayOfMonth(toDate('{{ end_month }}'))
        {% else %}
            yesterday()
        {% endif %} AS max_date
),

{% if start_month and end_month and is_incremental() %}
prev_balances AS (
    SELECT
        t1.ubo_address,
        tw.symbol,
        t1.balance_raw
    FROM (SELECT ubo_address, token_address, balance_raw, date FROM {{ this }} FINAL) t1
    INNER JOIN {{ ref('tokens_whitelist') }} tw
        ON lower(tw.address) = lower(t1.token_address)
    WHERE t1.date = (
        SELECT max(date) FROM {{ this }} FINAL WHERE date < toDate('{{ start_month }}')
    )
),
{% elif is_incremental() %}
current_partition AS (
    SELECT max(date) AS max_date
    FROM {{ this }}
    WHERE date < yesterday()
),
prev_balances AS (
    SELECT
        t1.ubo_address,
        tw.symbol,
        t1.balance_raw
    FROM (SELECT ubo_address, token_address, balance_raw, date FROM {{ this }}) t1
    CROSS JOIN current_partition t2
    INNER JOIN {{ ref('tokens_whitelist') }} tw
        ON lower(tw.address) = lower(t1.token_address)
    WHERE t1.date = t2.max_date
),
{% endif %}

{% if is_incremental() %}
keys AS (
    SELECT DISTINCT ubo_address, symbol
    FROM (
        SELECT ubo_address, symbol FROM prev_balances
        UNION ALL
        SELECT ubo_address, symbol FROM daily_deltas
    )
),

calendar AS (
    SELECT
        k.ubo_address,
        k.symbol,
        {% if start_month and end_month %}
            addDays(
                (SELECT max(date) FROM {{ this }} FINAL WHERE date < toDate('{{ start_month }}')),
                offset + 1
            ) AS date
        {% else %}
            addDays(cp.max_date, offset + 1) AS date
        {% endif %}
    FROM keys k
    {% if not (start_month and end_month) %}
    CROSS JOIN current_partition cp
    {% endif %}
    CROSS JOIN overall_max_date o
    ARRAY JOIN range(
        toUInt32(dateDiff('day',
            {% if start_month and end_month %}
                (SELECT max(date) FROM {{ this }} FINAL WHERE date < toDate('{{ start_month }}')),
            {% else %}
                cp.max_date,
            {% endif %}
            o.max_date
        ))
    ) AS offset
),
{% else %}
calendar AS (
    SELECT
        ubo_address,
        symbol,
        addDays(min_date, offset) AS date
    FROM (
        SELECT
            d.ubo_address,
            d.symbol,
            min(d.date)                                   AS min_date,
            dateDiff('day', min(d.date), any(o.max_date)) AS num_days
        FROM daily_deltas d
        CROSS JOIN overall_max_date o
        GROUP BY d.ubo_address, d.symbol
    )
    ARRAY JOIN range(num_days + 1) AS offset
),
{% endif %}

balances AS (
    SELECT
        c.date        AS date,
        c.ubo_address AS ubo_address,
        c.symbol      AS symbol,
        sum(coalesce(d.daily_delta_raw, toInt256(0))) OVER (
            PARTITION BY c.ubo_address, c.symbol
            ORDER BY c.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
        {% if is_incremental() %}
            + coalesce(p.balance_raw, toInt256(0))
        {% endif %}
        AS balance_raw
    FROM calendar c
    LEFT JOIN daily_deltas d
        ON  d.ubo_address = c.ubo_address
        AND d.symbol      = c.symbol
        AND d.date        = c.date
    {% if is_incremental() %}
    LEFT JOIN prev_balances p
        ON  p.ubo_address = c.ubo_address
        AND p.symbol      = c.symbol
    {% endif %}
)

SELECT
    b.date                                                                  AS date,
    'Balancer V2'                                                           AS protocol,
    lower('0xba12222222228d8ba445958a75a0704d566bf2c8')                    AS container_address,
    lower(tw_canon.address)                                                 AS token_address,
    b.symbol                                                                AS symbol,
    tw_canon.token_class                                                    AS token_class,
    lower(b.ubo_address)                                                    AS ubo_address,
    toInt256(b.balance_raw)                                                 AS balance_raw,
    b.balance_raw / pow(10, tw_canon.decimals)                             AS balance,
    (b.balance_raw / pow(10, tw_canon.decimals)) * coalesce(pr.price, 0)  AS balance_usd
FROM balances b
INNER JOIN {{ ref('tokens_whitelist') }} tw_canon
    ON  tw_canon.symbol = b.symbol
    AND b.date          >= tw_canon.date_start
    AND (tw_canon.date_end IS NULL OR b.date < tw_canon.date_end)
ASOF LEFT JOIN (
    SELECT symbol, date, price
    FROM {{ ref('int_execution_token_prices_daily') }}
    ORDER BY symbol, date
) pr
    ON  pr.symbol = b.symbol
    AND b.date    >= pr.date
WHERE b.balance_raw > 0
