{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date, pool_address, token_address)',
        unique_key='(date, pool_address, token_address)',
        partition_by='toStartOfMonth(date)',
        pre_hook=["SET join_use_nulls = 0"],
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'pools', 'balances', 'intermediate']
    )
}}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}

WITH

pool_registry AS (
    SELECT
        lower(decoded_params['poolId']) AS pool_id,
        lower(decoded_params['poolAddress']) AS pool_address
    FROM {{ ref('contracts_BalancerV2_Vault_events') }}
    WHERE event_name = 'PoolRegistered'
      AND decoded_params['poolId'] IS NOT NULL
      AND decoded_params['poolAddress'] IS NOT NULL
),

raw_deltas AS (
    SELECT
        toDate(toStartOfDay(e.block_timestamp)) AS date,
        if(r.pool_address != '', r.pool_address, e.pool_id) AS pool_address,
        lower(e.token_address) AS token_address,
        e.delta_amount_raw,
        toInt256(0) AS fee_amount_raw
    FROM {{ ref('stg_pools__balancer_v2_events') }} e
    LEFT JOIN pool_registry r
        ON r.pool_id = e.pool_id
    WHERE e.delta_amount_raw IS NOT NULL
      AND e.token_address IS NOT NULL
      AND e.pool_id IS NOT NULL
      AND e.block_timestamp < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(e.block_timestamp) >= toDate('{{ start_month }}')
        AND toStartOfMonth(e.block_timestamp) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('e.block_timestamp', 'date', 'true') }}
      {% endif %}
),

daily_deltas AS (
    SELECT
        date,
        pool_address,
        token_address,
        sum(delta_amount_raw) AS daily_delta_raw,
        sum(delta_amount_raw - fee_amount_raw) AS daily_reserve_delta_raw,
        sum(fee_amount_raw) AS daily_fee_delta_raw
    FROM raw_deltas
    WHERE lower(token_address) NOT IN (
        '0x0000000000000000000000000000000000000000',
        '0x0000000000000000000000000000000000000001'
    )
      AND lower(token_address) != lower(pool_address)
    GROUP BY date, pool_address, token_address
),

overall_max_date AS (
    SELECT
        least(
            {% if end_month %}
                toLastDayOfMonth(toDate('{{ end_month }}')),
            {% else %}
                today(),
            {% endif %}
            yesterday(),
            (
                SELECT max(toDate(toStartOfDay(block_timestamp)))
                FROM {{ ref('stg_pools__balancer_v2_events') }}
                {% if end_month %}
                WHERE toStartOfMonth(block_timestamp) <= toDate('{{ end_month }}')
                {% endif %}
            )
        ) AS max_date
),

{% if start_month and end_month %}
prev_balances AS (
    SELECT
        pool_address,
        token_address,
        token_amount_raw AS balance_raw,
        reserve_amount_raw AS reserve_raw,
        fee_amount_raw AS fee_raw
    FROM {{ this }} FINAL
    WHERE date = (
        SELECT max(date)
        FROM {{ this }} FINAL
        WHERE date < toDate('{{ start_month }}')
    )
),
{% elif is_incremental() %}
current_partition AS (
    SELECT
        max(date) AS max_date
    FROM {{ this }}
    WHERE date < yesterday()
),
prev_balances AS (
    SELECT
        t1.pool_address,
        t1.token_address,
        t1.token_amount_raw AS balance_raw,
        t1.reserve_amount_raw AS reserve_raw,
        t1.fee_amount_raw AS fee_raw
    FROM {{ this }} t1
    CROSS JOIN current_partition t2
    WHERE t1.date = t2.max_date
),
{% endif %}

{% if is_incremental() %}
keys AS (
    SELECT DISTINCT
        pool_address,
        token_address
    FROM (
        SELECT pool_address, token_address FROM prev_balances
        UNION ALL
        SELECT pool_address, token_address FROM daily_deltas
    )
),

calendar AS (
    SELECT
        k.pool_address,
        k.token_address,
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
        pool_address,
        token_address,
        addDays(min_date, offset) AS date
    FROM (
        SELECT
            d.pool_address,
            d.token_address,
            min(d.date) AS min_date,
            dateDiff('day', min(d.date), any(o.max_date)) AS num_days
        FROM daily_deltas d
        CROSS JOIN overall_max_date o
        GROUP BY d.pool_address, d.token_address
    )
    ARRAY JOIN range(num_days + 1) AS offset
),
{% endif %}

balances AS (
    SELECT
        c.date AS date,
        c.pool_address AS pool_address,
        c.token_address AS token_address,
        sum(coalesce(d.daily_delta_raw, toInt256(0))) OVER (
            PARTITION BY c.pool_address, c.token_address
            ORDER BY c.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
        {% if is_incremental() %}
            + coalesce(p.balance_raw, toInt256(0))
        {% endif %}
        AS balance_raw,
        sum(coalesce(d.daily_reserve_delta_raw, toInt256(0))) OVER (
            PARTITION BY c.pool_address, c.token_address
            ORDER BY c.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
        {% if is_incremental() %}
            + coalesce(p.reserve_raw, toInt256(0))
        {% endif %}
        AS reserve_raw,
        sum(coalesce(d.daily_fee_delta_raw, toInt256(0))) OVER (
            PARTITION BY c.pool_address, c.token_address
            ORDER BY c.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
        {% if is_incremental() %}
            + coalesce(p.fee_raw, toInt256(0))
        {% endif %}
        AS fee_raw
    FROM calendar c
    LEFT JOIN daily_deltas d
        ON d.pool_address = c.pool_address
       AND d.token_address = c.token_address
       AND d.date = c.date
    {% if is_incremental() %}
    LEFT JOIN prev_balances p
        ON p.pool_address = c.pool_address
       AND p.token_address = c.token_address
    {% endif %}
),

enriched AS (
    SELECT
        b.date AS date,
        'Balancer V2' AS protocol,
        concat('0x', replaceAll(lower(b.pool_address), '0x', '')) AS pool_address,
        replaceAll(lower(b.pool_address), '0x', '') AS pool_address_no0x,
        lower(b.token_address) AS token_address,
        tm.token AS token,
        b.balance_raw AS token_amount_raw,
        b.balance_raw / POWER(10, if(tm.decimals > 0, tm.decimals, 18)) AS token_amount,
        b.reserve_raw AS reserve_amount_raw,
        b.reserve_raw / POWER(10, if(tm.decimals > 0, tm.decimals, 18)) AS reserve_amount,
        b.fee_raw AS fee_amount_raw,
        b.fee_raw / POWER(10, if(tm.decimals > 0, tm.decimals, 18)) AS fee_amount,
        pr.price AS price_usd,
        (b.reserve_raw / POWER(10, if(tm.decimals > 0, tm.decimals, 18))) * pr.price AS tvl_component_usd
    FROM balances b
    LEFT JOIN {{ ref('stg_pools__tokens_meta') }} tm
        ON tm.token_address = lower(b.token_address)
       AND b.date >= toDate(tm.date_start)
    ASOF LEFT JOIN (
        SELECT symbol, date, price FROM {{ ref('int_execution_token_prices_daily') }} ORDER BY symbol, date
    ) pr
        ON pr.symbol = tm.token
       AND b.date >= pr.date
    WHERE b.balance_raw != 0
)

SELECT
    date,
    protocol,
    pool_address,
    pool_address_no0x,
    token_address,
    token,
    token_amount_raw,
    token_amount,
    reserve_amount_raw,
    reserve_amount,
    fee_amount_raw,
    fee_amount,
    price_usd,
    tvl_component_usd,
    (sum(tvl_component_usd) OVER (PARTITION BY date, pool_address) - tvl_component_usd)
        / nullIf(reserve_amount, 0) AS pool_implied_price_usd
FROM enriched
