{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date, protocol, pool_address, token_address)',
        unique_key='(date, protocol, pool_address, token_address)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['dev', 'execution', 'yields', 'pools', 'fees', 'accrued', 'intermediate']
    )
}}

{#-
  Accrued pool fees + trading volume from Swap + Flash at pool×token grain.
  Output: (date, protocol, pool_address, token_address, token, fee_amount, fees_usd, volume_amount, volume_usd)

  Notes:
  - Swap event amounts are signed int256 but stored as unsigned-looking strings in decoded_params.
    We reconstruct signed values via two's complement.
  - Fees are applied in ppm with 1e6 denominator.
  - Volume = gross incoming token amount per swap (the positive side). Flash loans contribute 0 volume.
  - Swapr V3 (Algebra) fee is dynamic (Fee events); we apply the latest fee as-of each swap,
    and backfill swaps before first Fee with the pool's first observed fee.
-#}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}

WITH
constants AS (
    SELECT
        toUInt256('57896044618658097711785492504343953926634992332820282019728792003956564819967') AS max_int256,
        toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639936') AS two_256,
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

uniswap_v3_swaps_raw AS (
    SELECT
        toDate(toStartOfDay(e.block_timestamp)) AS date,
        replaceAll(lower(e.contract_address), '0x', '') AS pool_address_no0x,
        e.block_timestamp,
        e.log_index,
        (toUInt64(toUnixTimestamp(e.block_timestamp)) * (SELECT log_index_factor FROM constants) + toUInt64(e.log_index)) AS event_order,
        toUInt256OrNull(e.decoded_params['amount0']) AS amount0_u,
        toUInt256OrNull(e.decoded_params['amount1']) AS amount1_u
    FROM {{ ref('contracts_UniswapV3_Pool_events') }} e
    WHERE e.event_name = 'Swap'
      AND e.block_timestamp < today()
      AND e.decoded_params['amount0'] IS NOT NULL
      AND e.decoded_params['amount1'] IS NOT NULL
      {% if start_month and end_month %}
        AND toStartOfMonth(e.block_timestamp) >= toDate('{{ start_month }}')
        AND toStartOfMonth(e.block_timestamp) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('e.block_timestamp', 'date', 'true') }}
      {% endif %}
),

uniswap_v3_swaps AS (
    SELECT
        s.date,
        s.pool_address_no0x,
        if(
            s.amount0_u > (SELECT max_int256 FROM constants),
            -toInt256((SELECT two_256 FROM constants) - s.amount0_u),
            toInt256(s.amount0_u)
        ) AS amount0,
        if(
            s.amount1_u > (SELECT max_int256 FROM constants),
            -toInt256((SELECT two_256 FROM constants) - s.amount1_u),
            toInt256(s.amount1_u)
        ) AS amount1,
        ft.fee_ppm AS fee_ppm
    FROM uniswap_v3_swaps_raw s
    INNER JOIN uniswap_v3_fee_tiers ft
        ON ft.pool_address_no0x = s.pool_address_no0x
    WHERE s.amount0_u IS NOT NULL
      AND s.amount1_u IS NOT NULL
      AND ft.fee_ppm IS NOT NULL
),

uniswap_v3_swap_fees_token AS (
    SELECT
        date,
        'Uniswap V3' AS protocol,
        pool_address_no0x,
        'token0' AS token_position,
        toUInt256(intDiv(greatest(amount0, toInt256(0)) * toInt256(fee_ppm), toInt256((SELECT fee_denom FROM constants)))) AS fee_raw,
        toUInt256(greatest(amount0, toInt256(0))) AS volume_raw
    FROM uniswap_v3_swaps

    UNION ALL

    SELECT
        date,
        'Uniswap V3' AS protocol,
        pool_address_no0x,
        'token1' AS token_position,
        toUInt256(intDiv(greatest(amount1, toInt256(0)) * toInt256(fee_ppm), toInt256((SELECT fee_denom FROM constants)))) AS fee_raw,
        toUInt256(greatest(amount1, toInt256(0))) AS volume_raw
    FROM uniswap_v3_swaps
),

uniswap_v3_flash_fees_token AS (
    SELECT
        toDate(toStartOfDay(e.block_timestamp)) AS date,
        'Uniswap V3' AS protocol,
        replaceAll(lower(e.contract_address), '0x', '') AS pool_address_no0x,
        'token0' AS token_position,
        toUInt256(
            toUInt256OrNull(e.decoded_params['paid0']) - toUInt256OrNull(e.decoded_params['amount0'])
        ) AS fee_raw,
        toUInt256(0) AS volume_raw
    FROM {{ ref('contracts_UniswapV3_Pool_events') }} e
    WHERE e.event_name = 'Flash'
      AND e.block_timestamp < today()
      AND e.decoded_params['paid0'] IS NOT NULL
      AND e.decoded_params['amount0'] IS NOT NULL
      AND toUInt256OrNull(e.decoded_params['paid0']) >= toUInt256OrNull(e.decoded_params['amount0'])
      {% if start_month and end_month %}
        AND toStartOfMonth(e.block_timestamp) >= toDate('{{ start_month }}')
        AND toStartOfMonth(e.block_timestamp) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('e.block_timestamp', 'date', 'true') }}
      {% endif %}

    UNION ALL

    SELECT
        toDate(toStartOfDay(e.block_timestamp)) AS date,
        'Uniswap V3' AS protocol,
        replaceAll(lower(e.contract_address), '0x', '') AS pool_address_no0x,
        'token1' AS token_position,
        toUInt256(
            toUInt256OrNull(e.decoded_params['paid1']) - toUInt256OrNull(e.decoded_params['amount1'])
        ) AS fee_raw,
        toUInt256(0) AS volume_raw
    FROM {{ ref('contracts_UniswapV3_Pool_events') }} e
    WHERE e.event_name = 'Flash'
      AND e.block_timestamp < today()
      AND e.decoded_params['paid1'] IS NOT NULL
      AND e.decoded_params['amount1'] IS NOT NULL
      AND toUInt256OrNull(e.decoded_params['paid1']) >= toUInt256OrNull(e.decoded_params['amount1'])
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

swapr_v3_swaps_raw AS (
    SELECT
        toDate(toStartOfDay(e.block_timestamp)) AS date,
        replaceAll(lower(e.contract_address), '0x', '') AS pool_address_no0x,
        e.block_timestamp,
        e.log_index,
        (toUInt64(toUnixTimestamp(e.block_timestamp)) * (SELECT log_index_factor FROM constants) + toUInt64(e.log_index)) AS event_order,
        toUInt256OrNull(e.decoded_params['amount0']) AS amount0_u,
        toUInt256OrNull(e.decoded_params['amount1']) AS amount1_u
    FROM {{ ref('contracts_Swapr_v3_AlgebraPool_events') }} e
    WHERE e.event_name = 'Swap'
      AND e.block_timestamp < today()
      AND e.decoded_params['amount0'] IS NOT NULL
      AND e.decoded_params['amount1'] IS NOT NULL
      {% if start_month and end_month %}
        AND toStartOfMonth(e.block_timestamp) >= toDate('{{ start_month }}')
        AND toStartOfMonth(e.block_timestamp) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('e.block_timestamp', 'date', 'true') }}
      {% endif %}
),

swapr_v3_swaps_with_fee AS (
    SELECT
        s.date,
        s.pool_address_no0x,
        if(
            s.amount0_u > (SELECT max_int256 FROM constants),
            -toInt256((SELECT two_256 FROM constants) - s.amount0_u),
            toInt256(s.amount0_u)
        ) AS amount0,
        if(
            s.amount1_u > (SELECT max_int256 FROM constants),
            -toInt256((SELECT two_256 FROM constants) - s.amount1_u),
            toInt256(s.amount1_u)
        ) AS amount1,
        coalesce(f.fee_ppm, ff.first_fee_ppm) AS fee_ppm
    FROM (
        SELECT * FROM swapr_v3_swaps_raw
        ORDER BY pool_address_no0x, event_order
    ) s
    ASOF LEFT JOIN (
        SELECT * FROM swapr_v3_fee_events
        ORDER BY pool_address_no0x, event_order
    ) f
      ON s.pool_address_no0x = f.pool_address_no0x
     AND s.event_order >= f.event_order
    LEFT JOIN swapr_v3_first_fee ff
      ON ff.pool_address_no0x = s.pool_address_no0x
    WHERE coalesce(f.fee_ppm, ff.first_fee_ppm) IS NOT NULL
),

swapr_v3_swap_fees_token AS (
    SELECT
        date,
        'Swapr V3' AS protocol,
        s.pool_address_no0x,
        'token0' AS token_position,
        toUInt256(intDiv(greatest(amount0, toInt256(0)) * toInt256(fee_ppm), toInt256((SELECT fee_denom FROM constants)))) AS fee_raw,
        toUInt256(greatest(amount0, toInt256(0))) AS volume_raw
    FROM swapr_v3_swaps_with_fee s

    UNION ALL

    SELECT
        date,
        'Swapr V3' AS protocol,
        s.pool_address_no0x,
        'token1' AS token_position,
        toUInt256(intDiv(greatest(amount1, toInt256(0)) * toInt256(fee_ppm), toInt256((SELECT fee_denom FROM constants)))) AS fee_raw,
        toUInt256(greatest(amount1, toInt256(0))) AS volume_raw
    FROM swapr_v3_swaps_with_fee s
),

swapr_v3_flash_fees_token AS (
    SELECT
        toDate(toStartOfDay(e.block_timestamp)) AS date,
        'Swapr V3' AS protocol,
        replaceAll(lower(e.contract_address), '0x', '') AS pool_address_no0x,
        'token0' AS token_position,
        toUInt256(
            toUInt256OrNull(e.decoded_params['paid0']) - toUInt256OrNull(e.decoded_params['amount0'])
        ) AS fee_raw,
        toUInt256(0) AS volume_raw
    FROM {{ ref('contracts_Swapr_v3_AlgebraPool_events') }} e
    WHERE e.event_name = 'Flash'
      AND e.block_timestamp < today()
      AND e.decoded_params['paid0'] IS NOT NULL
      AND e.decoded_params['amount0'] IS NOT NULL
      AND toUInt256OrNull(e.decoded_params['paid0']) >= toUInt256OrNull(e.decoded_params['amount0'])
      {% if start_month and end_month %}
        AND toStartOfMonth(e.block_timestamp) >= toDate('{{ start_month }}')
        AND toStartOfMonth(e.block_timestamp) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('e.block_timestamp', 'date', 'true') }}
      {% endif %}

    UNION ALL

    SELECT
        toDate(toStartOfDay(e.block_timestamp)) AS date,
        'Swapr V3' AS protocol,
        replaceAll(lower(e.contract_address), '0x', '') AS pool_address_no0x,
        'token1' AS token_position,
        toUInt256(
            toUInt256OrNull(e.decoded_params['paid1']) - toUInt256OrNull(e.decoded_params['amount1'])
        ) AS fee_raw,
        toUInt256(0) AS volume_raw
    FROM {{ ref('contracts_Swapr_v3_AlgebraPool_events') }} e
    WHERE e.event_name = 'Flash'
      AND e.block_timestamp < today()
      AND e.decoded_params['paid1'] IS NOT NULL
      AND e.decoded_params['amount1'] IS NOT NULL
      AND toUInt256OrNull(e.decoded_params['paid1']) >= toUInt256OrNull(e.decoded_params['amount1'])
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
   Token mapping + aggregation
------------------------------ */
all_fees_token AS (
    SELECT * FROM uniswap_v3_fees_token
    UNION ALL
    SELECT * FROM swapr_v3_fees_token
),

fees_daily AS (
    SELECT
        f.date,
        f.protocol,
        f.pool_address,
        f.token_address,
        f.token,
        sum(f.fee_amount) AS fee_amount,
        sum(f.fee_amount * p.price_usd) AS fees_usd,
        sum(f.volume_amount) AS volume_amount,
        sum(f.volume_amount * p.price_usd) AS volume_usd
    FROM (
        SELECT
            fw.date AS date,
            fw.protocol AS protocol,
            fw.pool_address AS pool_address,
            fw.token_address AS token_address,
            tm.token AS token,
            (toFloat64(fw.fee_raw) / pow(10, coalesce(tm.decimals, 18))) AS fee_amount,
            (toFloat64(fw.volume_raw) / pow(10, coalesce(tm.decimals, 18))) AS volume_amount
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
        LEFT JOIN {{ ref('stg_yields__tokens_meta') }} tm
          ON tm.token_address = fw.token_address
         AND fw.date >= toDate(tm.date_start)
         AND (tm.date_end IS NULL OR fw.date < toDate(tm.date_end))
        WHERE fw.token_address IS NOT NULL
          AND tm.token IS NOT NULL
          AND tm.token != ''
    ) f
    ASOF LEFT JOIN (
        SELECT * FROM {{ ref('stg_yields__token_prices_daily') }} ORDER BY token, date
    ) p
      ON p.token = f.token
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

