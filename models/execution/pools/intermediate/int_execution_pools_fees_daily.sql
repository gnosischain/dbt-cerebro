{{
    config(
        materialized='incremental',
        incremental_strategy=('append' if var('start_month', none) else 'delete+insert'),
        engine='ReplacingMergeTree()',
        order_by='(date, protocol, pool_address, token_address)',
        unique_key='(date, protocol, pool_address, token_address)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'pools', 'fees', 'accrued', 'intermediate'],
        pre_hook=["SET join_use_nulls = 0"],
        post_hook=["SET join_use_nulls = 0"]
    )
}}
{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}

WITH
constants AS (
    SELECT
        toUInt64(4294967296) AS log_index_factor, -- 2^32
        toUInt64(1000000) AS fee_denom
),

/* -----------------------------
   Uniswap V3: static fee tier per pool
------------------------------ */
uniswap_v3_fee_tiers AS (
    SELECT DISTINCT
        replaceAll(lower(decoded_params['pool']), '0x', '') AS pool_address_no0x,
        toUInt32OrNull(decoded_params['fee']) AS fee_ppm
    FROM {{ ref('contracts_UniswapV3_Factory_events') }}
    WHERE event_name = 'PoolCreated'
      AND decoded_params['pool'] IS NOT NULL
      AND decoded_params['fee'] IS NOT NULL
),

uniswap_v3_swap_fees_token AS (
    SELECT
        toDate(e.block_timestamp) AS date,
        'Uniswap V3' AS protocol,
        e.pool_address AS pool_address_no0x,
        e.token_position,
        toUInt256(intDiv(e.delta_amount_raw * toInt256(ft.fee_ppm), toInt256((SELECT fee_denom FROM constants)))) AS fee_raw,
        toUInt256(e.delta_amount_raw) AS volume_raw
    FROM {{ ref('stg_pools__uniswap_v3_events') }} e
    INNER JOIN uniswap_v3_fee_tiers ft
        ON ft.pool_address_no0x = e.pool_address
    WHERE e.delta_category = 'swap_in'
      AND ft.fee_ppm IS NOT NULL
      {% if start_month and end_month %}
        AND toStartOfMonth(e.block_timestamp) >= toDate('{{ start_month }}')
        AND toStartOfMonth(e.block_timestamp) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('e.block_timestamp', 'date', 'true') }}
      {% endif %}
),

uniswap_v3_flash_fees_token AS (
    SELECT
        toDate(e.block_timestamp) AS date,
        'Uniswap V3' AS protocol,
        e.pool_address AS pool_address_no0x,
        e.token_position,
        toUInt256(e.delta_amount_raw) AS fee_raw,
        toUInt256(0) AS volume_raw
    FROM {{ ref('stg_pools__uniswap_v3_events') }} e
    WHERE e.delta_category = 'flash_fee'
      AND e.delta_amount_raw > toInt256(0)
      {% if start_month and end_month %}
        AND toStartOfMonth(e.block_timestamp) >= toDate('{{ start_month }}')
        AND toStartOfMonth(e.block_timestamp) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('e.block_timestamp', 'date', 'true') }}
      {% endif %}
),

uniswap_v3_fees_token AS (
    SELECT * FROM uniswap_v3_swap_fees_token
    UNION ALL
    SELECT * FROM uniswap_v3_flash_fees_token
),

/* -----------------------------
   Swapr V3 (Algebra): dynamic fee schedule
------------------------------ */
swapr_v3_fee_events AS (
    SELECT
        replaceAll(lower(e.contract_address), '0x', '') AS pool_address_no0x,
        e.block_timestamp,
        e.log_index,
        (toUInt64(toUnixTimestamp(e.block_timestamp)) * (SELECT log_index_factor FROM constants) + toUInt64(e.log_index)) AS event_order,
        toUInt32OrNull(e.decoded_params['fee']) AS fee_ppm
    FROM {{ ref('contracts_Swapr_v3_AlgebraPool_events') }} e
    WHERE e.event_name = 'Fee'
      AND e.block_timestamp < today()
      AND e.decoded_params['fee'] IS NOT NULL
),

swapr_v3_first_fee AS (
    SELECT
        pool_address_no0x,
        argMin(fee_ppm, event_order) AS first_fee_ppm
    FROM swapr_v3_fee_events
    WHERE fee_ppm IS NOT NULL
    GROUP BY pool_address_no0x
),

swapr_v3_swaps_with_fee AS (
    SELECT
        s.date AS date,
        s.pool_address AS pool_address_no0x,
        s.token_position AS token_position,
        s.delta_amount_raw AS delta_amount_raw,
        if(f.fee_ppm > 0, f.fee_ppm, ff.first_fee_ppm) AS fee_ppm
    FROM (
        SELECT
            toDate(block_timestamp) AS date,
            pool_address,
            block_timestamp,
            log_index,
            token_position,
            delta_amount_raw,
            (toUInt64(toUnixTimestamp(block_timestamp)) * (SELECT log_index_factor FROM constants) + toUInt64(log_index)) AS event_order
        FROM {{ ref('stg_pools__swapr_v3_events') }}
        WHERE delta_category = 'swap_in'
          {% if start_month and end_month %}
            AND toStartOfMonth(block_timestamp) >= toDate('{{ start_month }}')
            AND toStartOfMonth(block_timestamp) <= toDate('{{ end_month }}')
          {% else %}
            {{ apply_monthly_incremental_filter('block_timestamp', 'date', 'true') }}
          {% endif %}
        ORDER BY pool_address, event_order
    ) s
    ASOF LEFT JOIN (
        SELECT * FROM swapr_v3_fee_events
        ORDER BY pool_address_no0x, event_order
    ) f
      ON s.pool_address = f.pool_address_no0x
     AND s.event_order >= f.event_order
    LEFT JOIN swapr_v3_first_fee ff
      ON ff.pool_address_no0x = s.pool_address
    WHERE coalesce(f.fee_ppm, ff.first_fee_ppm) IS NOT NULL
),

swapr_v3_swap_fees_token AS (
    SELECT
        date,
        'Swapr V3' AS protocol,
        pool_address_no0x,
        token_position,
        toUInt256(intDiv(delta_amount_raw * toInt256(fee_ppm), toInt256((SELECT fee_denom FROM constants)))) AS fee_raw,
        toUInt256(delta_amount_raw) AS volume_raw
    FROM swapr_v3_swaps_with_fee
),

swapr_v3_flash_fees_token AS (
    SELECT
        toDate(e.block_timestamp) AS date,
        'Swapr V3' AS protocol,
        e.pool_address AS pool_address_no0x,
        e.token_position,
        toUInt256(e.delta_amount_raw) AS fee_raw,
        toUInt256(0) AS volume_raw
    FROM {{ ref('stg_pools__swapr_v3_events') }} e
    WHERE e.delta_category = 'flash_fee'
      AND e.delta_amount_raw > toInt256(0)
      {% if start_month and end_month %}
        AND toStartOfMonth(e.block_timestamp) >= toDate('{{ start_month }}')
        AND toStartOfMonth(e.block_timestamp) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('e.block_timestamp', 'date', 'true') }}
      {% endif %}
),

swapr_v3_fees_token AS (
    SELECT * FROM swapr_v3_swap_fees_token
    UNION ALL
    SELECT * FROM swapr_v3_flash_fees_token
),

/* -----------------------------
   Balancer V3: explicit fee amounts from Swap events
------------------------------ */
balancer_v3_swap_fees AS (
    SELECT
        toDate(toStartOfDay(e.block_timestamp)) AS date,
        'Balancer V3' AS protocol,
        concat('0x', replaceAll(lower(e.decoded_params['pool']), '0x', '')) AS pool_address,
        lower(e.decoded_params['tokenIn']) AS token_address,
        toUInt256OrNull(e.decoded_params['swapFeeAmount']) AS fee_raw,
        toUInt256OrNull(e.decoded_params['amountIn']) AS volume_raw
    FROM {{ ref('contracts_BalancerV3_Vault_events') }} e
    WHERE e.event_name = 'Swap'
      AND e.block_timestamp < today()
      AND e.decoded_params['pool'] IS NOT NULL
      AND e.decoded_params['tokenIn'] IS NOT NULL
      AND e.decoded_params['swapFeeAmount'] IS NOT NULL
      AND e.decoded_params['amountIn'] IS NOT NULL
      {% if start_month and end_month %}
        AND toStartOfMonth(e.block_timestamp) >= toDate('{{ start_month }}')
        AND toStartOfMonth(e.block_timestamp) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('e.block_timestamp', 'date', 'true') }}
      {% endif %}
),

/* -----------------------------
   Token mapping + aggregation
------------------------------ */
all_fees_token AS (
    SELECT * FROM uniswap_v3_fees_token
    UNION ALL
    SELECT * FROM swapr_v3_fees_token
),

all_fees_with_token AS (
    SELECT
        fw.date AS date,
        fw.protocol AS protocol,
        fw.pool_address AS pool_address,
        fw.token_address AS token_address,
        tm.token AS token,
        (toFloat64(fw.fee_raw) / pow(10, if(tm.decimals > 0, tm.decimals, 18))) AS fee_amount,
        (toFloat64(fw.volume_raw) / pow(10, if(tm.decimals > 0, tm.decimals, 18))) AS volume_amount
    FROM (
        SELECT
            af.date AS date,
            af.protocol AS protocol,
            concat('0x', af.pool_address_no0x) AS pool_address,
            multiIf(
                af.token_position = 'token0', m.token0_address,
                af.token_position = 'token1', m.token1_address,
                NULL
            ) AS token_address,
            af.fee_raw AS fee_raw,
            af.volume_raw AS volume_raw
        FROM all_fees_token af
        INNER JOIN {{ ref('stg_pools__v3_pool_registry') }} m
          ON m.protocol = af.protocol
         AND m.pool_address = concat('0x', af.pool_address_no0x)
        WHERE af.fee_raw IS NOT NULL
    ) fw
    LEFT JOIN {{ ref('stg_pools__tokens_meta') }} tm
      ON tm.token_address = fw.token_address
     AND fw.date >= toDate(tm.date_start)
    WHERE fw.token_address IS NOT NULL
      AND tm.token IS NOT NULL
      AND tm.token != ''

    UNION ALL

    SELECT
        bf.date AS date,
        bf.protocol AS protocol,
        bf.pool_address AS pool_address,
        bf.token_address AS token_address,
        tm.token AS token,
        (toFloat64(bf.fee_raw) / pow(10, if(tm.decimals > 0, tm.decimals, 18))) AS fee_amount,
        (toFloat64(bf.volume_raw) / pow(10, if(tm.decimals > 0, tm.decimals, 18))) AS volume_amount
    FROM balancer_v3_swap_fees bf
    LEFT JOIN {{ ref('stg_pools__balancer_v3_token_map') }} wm
      ON wm.wrapper_address = bf.token_address
    LEFT JOIN {{ ref('stg_pools__tokens_meta') }} tm
      ON tm.token_address = coalesce(nullIf(wm.underlying_address, ''), bf.token_address)
     AND bf.date >= toDate(tm.date_start)
    WHERE bf.token_address IS NOT NULL
      AND bf.fee_raw IS NOT NULL
      AND tm.token IS NOT NULL
      AND tm.token != ''
),

fees_daily AS (
    SELECT
        f.date,
        f.protocol,
        f.pool_address,
        f.token_address,
        f.token,
        sum(f.fee_amount) AS fee_amount,
        sum(f.fee_amount * p.price) AS fees_usd,
        sum(f.volume_amount) AS volume_amount,
        sum(f.volume_amount * p.price) AS volume_usd
    FROM all_fees_with_token f
    ASOF LEFT JOIN (
        SELECT * FROM {{ ref('int_execution_token_prices_daily') }} ORDER BY symbol, date
    ) p
      ON p.symbol = f.token
     AND f.date >= p.date
    GROUP BY f.date, f.protocol, f.pool_address, f.token_address, f.token
)

SELECT
    date,
    protocol,
    pool_address,
    token_address,
    token,
    fee_amount,
    fees_usd,
    volume_amount,
    volume_usd
FROM fees_daily
WHERE date < today()
