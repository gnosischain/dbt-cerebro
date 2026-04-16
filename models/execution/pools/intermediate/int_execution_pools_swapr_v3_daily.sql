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

pools AS (
    SELECT
        pool_address,
        replaceAll(pool_address, '0x', '') AS pool_address_no0x,
        token0_address,
        token1_address
    FROM {{ ref('stg_pools__v3_pool_registry') }}
    WHERE protocol = 'Swapr V3'
      AND pool_address IN (
          SELECT lower(address)
          FROM {{ ref('contracts_whitelist') }}
          WHERE contract_type = 'SwaprPool'
      )
),

/* -- Swapr V3 dynamic fee schedule -- */
fee_events AS (
    SELECT
        replaceAll(lower(contract_address), '0x', '') AS pool_address_no0x,
        (toUInt64(toUnixTimestamp(block_timestamp)) * toUInt64(4294967296) + toUInt64(log_index)) AS event_order,
        toUInt32OrNull(decoded_params['fee']) AS fee_ppm
    FROM {{ ref('contracts_Swapr_v3_AlgebraPool_events') }}
    WHERE event_name = 'Fee'
      AND decoded_params['fee'] IS NOT NULL
),

first_fee AS (
    SELECT
        pool_address_no0x,
        argMin(fee_ppm, event_order) AS first_fee_ppm
    FROM fee_events
    WHERE fee_ppm IS NOT NULL
    GROUP BY pool_address_no0x
),

{#- ASOF join for dynamic fees, then fee split + daily aggregation.
    The ASOF join requires a subquery with ORDER BY, so we nest it. -#}
daily_deltas AS (
    SELECT
        date,
        pool_address,
        token_address,
        sum(delta_amount_raw) AS daily_delta_raw,
        sum(multiIf(
            delta_category = 'swap_in',
                delta_amount_raw - intDiv(delta_amount_raw * toInt256(effective_fee_ppm), toInt256(1000000)),
            delta_category IN ('fee_collection', 'flash_fee'),
                toInt256(0),
            delta_amount_raw
        )) AS daily_reserve_delta_raw,
        sum(multiIf(
            delta_category = 'swap_in',
                intDiv(delta_amount_raw * toInt256(effective_fee_ppm), toInt256(1000000)),
            delta_category IN ('fee_collection', 'flash_fee'),
                delta_amount_raw,
            toInt256(0)
        )) AS daily_fee_delta_raw
    FROM (
        SELECT
            sw.date AS date,
            sw.pool_address AS pool_address,
            sw.token_address AS token_address,
            sw.delta_amount_raw AS delta_amount_raw,
            sw.delta_category AS delta_category,
            toUInt32(if(f.fee_ppm > 0, f.fee_ppm, coalesce(ff.first_fee_ppm, 0))) AS effective_fee_ppm
        FROM (
            SELECT
                concat('0x', e.pool_address) AS pool_address,
                e.pool_address AS pool_address_no0x,
                multiIf(
                    e.token_position = 'token0', p.token0_address,
                    e.token_position = 'token1', p.token1_address,
                    NULL
                ) AS token_address,
                toDate(toStartOfDay(e.block_timestamp)) AS date,
                e.delta_amount_raw AS delta_amount_raw,
                e.delta_category AS delta_category,
                (toUInt64(toUnixTimestamp(e.block_timestamp)) * toUInt64(4294967296) + toUInt64(e.log_index)) AS event_order
            FROM {{ ref('stg_pools__swapr_v3_events') }} e
            INNER JOIN pools p
                ON p.pool_address_no0x = e.pool_address
            WHERE e.block_timestamp < today()
              AND multiIf(
                  e.token_position = 'token0', p.token0_address,
                  e.token_position = 'token1', p.token1_address,
                  NULL
              ) IS NOT NULL
              {% if start_month and end_month %}
                AND toStartOfMonth(e.block_timestamp) >= toDate('{{ start_month }}')
                AND toStartOfMonth(e.block_timestamp) <= toDate('{{ end_month }}')
              {% else %}
                {{ apply_monthly_incremental_filter('e.block_timestamp', 'date', 'true') }}
              {% endif %}
            ORDER BY pool_address_no0x, event_order
        ) sw
        ASOF LEFT JOIN (
            SELECT * FROM fee_events
            ORDER BY pool_address_no0x, event_order
        ) f
            ON sw.pool_address_no0x = f.pool_address_no0x
           AND sw.event_order >= f.event_order
        LEFT JOIN first_fee ff
            ON ff.pool_address_no0x = sw.pool_address_no0x
    )
    GROUP BY date, pool_address, token_address
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
prev_balances AS (
    SELECT
        pool_address,
        token_address,
        token_amount_raw AS balance_raw,
        reserve_amount_raw AS reserve_raw,
        fee_amount_raw AS fee_raw
    FROM {{ this }} FINAL
    WHERE date = (SELECT max(date) FROM {{ this }} FINAL)
),
{% endif %}

balances AS (
    SELECT
        date,
        pool_address,
        token_address,
        sum(daily_delta_raw) OVER (
            PARTITION BY pool_address, token_address
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
        {% if is_incremental() %}
            + coalesce(p.balance_raw, toInt256(0))
        {% endif %}
        AS balance_raw,
        sum(daily_reserve_delta_raw) OVER (
            PARTITION BY pool_address, token_address
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
        {% if is_incremental() %}
            + coalesce(p.reserve_raw, toInt256(0))
        {% endif %}
        AS reserve_raw,
        sum(daily_fee_delta_raw) OVER (
            PARTITION BY pool_address, token_address
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
        {% if is_incremental() %}
            + coalesce(p.fee_raw, toInt256(0))
        {% endif %}
        AS fee_raw
    FROM daily_deltas d
    {% if is_incremental() %}
    LEFT JOIN prev_balances p
        ON d.pool_address = p.pool_address
        AND d.token_address = p.token_address
    {% endif %}
),

enriched AS (
    SELECT
        b.date AS date,
        'Swapr V3' AS protocol,
        b.pool_address AS pool_address,
        replaceAll(b.pool_address, '0x', '') AS pool_address_no0x,
        b.token_address AS token_address,
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
        ON tm.token_address = b.token_address
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
