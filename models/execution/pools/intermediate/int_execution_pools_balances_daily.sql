{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date, protocol, pool_address, token_address)',
        unique_key='(date, protocol, pool_address, token_address)',
        partition_by='toStartOfMonth(date)',
        pre_hook=["SET join_use_nulls = 0"],
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'pools', 'balances', 'intermediate']
    )
}}

{#-
  Daily pool token balances for all supported DEX protocols.

  V3 pools (Uniswap V3 + Swapr V3):
    - Built from event deltas (Mint, Burn, Swap, Collect, Flash) via staging models.
    - No dependency on int_execution_tokens_balances_daily.
    - Separates reserves (TVL-contributing) from unclaimed fees:
      * reserve_amount: Mint/Burn liquidity + swap deltas net of fees
      * fee_amount: swap fee income - collected fees + flash loan fees
    - Fee rates: static from PoolCreated for UniV3, dynamic via Fee events for Swapr.

  Balancer V2/V3:
    - Built from delta events (PoolBalanceChanged, Swap, PoolBalanceManaged).
    - Fee separation not yet implemented (reserve_amount = token_amount, fee_amount = 0).

  IMPORTANT: Requires full refresh after initial schema migration (new reserve/fee columns).
-#}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}

WITH

/* ========================================
   V3 Pools: Uniswap V3 + Swapr V3
   Event-delta approach with fee separation
   ======================================== */

v3_pools AS (
    SELECT
        protocol,
        pool_address,
        replaceAll(pool_address, '0x', '') AS pool_address_no0x,
        token0_address,
        token1_address,
        fee_tier_ppm
    FROM {{ ref('stg_pools__v3_pool_registry') }}
    WHERE pool_address IN (
        SELECT lower(address)
        FROM {{ ref('contracts_whitelist') }}
        WHERE contract_type IN ('UniswapV3Pool', 'SwaprPool')
    )
),

/* -- Swapr V3 dynamic fee schedule -- */
swapr_v3_fee_events AS (
    SELECT
        replaceAll(lower(contract_address), '0x', '') AS pool_address_no0x,
        (toUInt64(toUnixTimestamp(block_timestamp)) * toUInt64(4294967296) + toUInt64(log_index)) AS event_order,
        toUInt32OrNull(decoded_params['fee']) AS fee_ppm
    FROM {{ ref('contracts_Swapr_v3_AlgebraPool_events') }}
    WHERE event_name = 'Fee'
      AND decoded_params['fee'] IS NOT NULL
),

swapr_v3_first_fee AS (
    SELECT
        pool_address_no0x,
        argMin(fee_ppm, event_order) AS first_fee_ppm
    FROM swapr_v3_fee_events
    WHERE fee_ppm IS NOT NULL
    GROUP BY pool_address_no0x
),

{#- UniV3: JOIN + fee split + daily aggregation in one CTE.
    Avoids ClickHouse CTE scoping issues with JOINed CTEs referenced downstream. -#}
uniswap_v3_daily_deltas AS (
    SELECT
        toStartOfDay(e.block_timestamp) AS date,
        'Uniswap V3' AS protocol,
        concat('0x', e.pool_address) AS pool_address,
        multiIf(
            e.token_position = 'token0', p.token0_address,
            e.token_position = 'token1', p.token1_address,
            NULL
        ) AS token_address,
        sum(e.delta_amount_raw) AS daily_delta_raw,
        sum(multiIf(
            e.delta_category = 'swap_in',
                e.delta_amount_raw - intDiv(e.delta_amount_raw * toInt256(p.fee_tier_ppm), toInt256(1000000)),
            e.delta_category IN ('fee_collection', 'flash_fee'),
                toInt256(0),
            e.delta_amount_raw
        )) AS daily_reserve_delta_raw,
        sum(multiIf(
            e.delta_category = 'swap_in',
                intDiv(e.delta_amount_raw * toInt256(p.fee_tier_ppm), toInt256(1000000)),
            e.delta_category IN ('fee_collection', 'flash_fee'),
                e.delta_amount_raw,
            toInt256(0)
        )) AS daily_fee_delta_raw
    FROM {{ ref('stg_pools__uniswap_v3_events') }} e
    INNER JOIN v3_pools p
        ON p.pool_address_no0x = e.pool_address
       AND p.protocol = 'Uniswap V3'
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
    GROUP BY date, protocol, pool_address, token_address
),

{#- Swapr: ASOF join for dynamic fees, then fee split + daily aggregation.
    The ASOF join requires a subquery with ORDER BY, so we nest it. -#}
swapr_v3_daily_deltas AS (
    SELECT
        date,
        protocol,
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
            sw.protocol AS protocol,
            sw.pool_address AS pool_address,
            sw.token_address AS token_address,
            sw.delta_amount_raw AS delta_amount_raw,
            sw.delta_category AS delta_category,
            toUInt32(if(f.fee_ppm > 0, f.fee_ppm, coalesce(ff.first_fee_ppm, 0))) AS effective_fee_ppm
        FROM (
            SELECT
                'Swapr V3' AS protocol,
                concat('0x', e.pool_address) AS pool_address,
                e.pool_address AS pool_address_no0x,
                multiIf(
                    e.token_position = 'token0', p.token0_address,
                    e.token_position = 'token1', p.token1_address,
                    NULL
                ) AS token_address,
                toStartOfDay(e.block_timestamp) AS date,
                e.delta_amount_raw AS delta_amount_raw,
                e.delta_category AS delta_category,
                (toUInt64(toUnixTimestamp(e.block_timestamp)) * toUInt64(4294967296) + toUInt64(e.log_index)) AS event_order
            FROM {{ ref('stg_pools__swapr_v3_events') }} e
            INNER JOIN v3_pools p
                ON p.pool_address_no0x = e.pool_address
               AND p.protocol = 'Swapr V3'
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
            SELECT * FROM swapr_v3_fee_events
            ORDER BY pool_address_no0x, event_order
        ) f
            ON sw.pool_address_no0x = f.pool_address_no0x
           AND sw.event_order >= f.event_order
        LEFT JOIN swapr_v3_first_fee ff
            ON ff.pool_address_no0x = sw.pool_address_no0x
    )
    GROUP BY date, protocol, pool_address, token_address
),

v3_daily_deltas AS (
    SELECT * FROM uniswap_v3_daily_deltas
    UNION ALL
    SELECT * FROM swapr_v3_daily_deltas
),

{% if start_month and end_month %}
v3_prev_balances AS (
    SELECT
        protocol,
        pool_address,
        token_address,
        token_amount_raw AS balance_raw,
        reserve_amount_raw AS reserve_raw,
        fee_amount_raw AS fee_raw
    FROM {{ this }} FINAL
    WHERE protocol IN ('Uniswap V3', 'Swapr V3')
      AND date = (
        SELECT max(date)
        FROM {{ this }} FINAL
        WHERE date < toDate('{{ start_month }}')
          AND protocol IN ('Uniswap V3', 'Swapr V3')
      )
),
{% elif is_incremental() %}
v3_prev_balances AS (
    SELECT
        protocol,
        pool_address,
        token_address,
        token_amount_raw AS balance_raw,
        reserve_amount_raw AS reserve_raw,
        fee_amount_raw AS fee_raw
    FROM {{ this }} FINAL
    WHERE protocol IN ('Uniswap V3', 'Swapr V3')
      AND date = (
        SELECT max(date)
        FROM {{ this }} FINAL
        WHERE protocol IN ('Uniswap V3', 'Swapr V3')
      )
),
{% endif %}

v3_balances AS (
    SELECT
        date,
        protocol,
        pool_address,
        token_address,
        sum(daily_delta_raw) OVER (
            PARTITION BY protocol, pool_address, token_address
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
        {% if is_incremental() %}
            + coalesce(p.balance_raw, toInt256(0))
        {% endif %}
        AS balance_raw,
        sum(daily_reserve_delta_raw) OVER (
            PARTITION BY protocol, pool_address, token_address
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
        {% if is_incremental() %}
            + coalesce(p.reserve_raw, toInt256(0))
        {% endif %}
        AS reserve_raw,
        sum(daily_fee_delta_raw) OVER (
            PARTITION BY protocol, pool_address, token_address
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
        {% if is_incremental() %}
            + coalesce(p.fee_raw, toInt256(0))
        {% endif %}
        AS fee_raw
    FROM v3_daily_deltas d
    {% if is_incremental() %}
    LEFT JOIN v3_prev_balances p
        ON d.protocol = p.protocol
        AND d.pool_address = p.pool_address
        AND d.token_address = p.token_address
    {% endif %}
),

v3_balances_final AS (
    SELECT
        b.date AS date,
        b.protocol AS protocol,
        b.pool_address AS pool_address,
        b.token_address AS token_address,
        b.balance_raw AS token_amount_raw,
        b.balance_raw / POWER(10, if(t.decimals > 0, t.decimals, 18)) AS token_amount,
        b.reserve_raw AS reserve_amount_raw,
        b.reserve_raw / POWER(10, if(t.decimals > 0, t.decimals, 18)) AS reserve_amount,
        b.fee_raw AS fee_amount_raw,
        b.fee_raw / POWER(10, if(t.decimals > 0, t.decimals, 18)) AS fee_amount
    FROM v3_balances b
    LEFT JOIN {{ ref('tokens_whitelist') }} t
        ON lower(t.address) = b.token_address
       AND b.date >= toDate(t.date_start)
    WHERE b.balance_raw != 0
),

/* ========================================
   Balancer V2 + V3
   Delta-based approach (unchanged)
   Fee separation not yet implemented
   ======================================== */

balancer_v2_pool_registry AS (
    SELECT
        lower(decoded_params['poolId']) AS pool_id,
        lower(decoded_params['poolAddress']) AS pool_address
    FROM {{ ref('contracts_BalancerV2_Vault_events') }}
    WHERE event_name = 'PoolRegistered'
      AND decoded_params['poolId'] IS NOT NULL
      AND decoded_params['poolAddress'] IS NOT NULL
),

balancer_v3_swap_tokens AS (
    SELECT
        pool_address,
        arraySort(groupUniqArray(token_address)) AS swap_tokens
    FROM {{ ref('stg_pools__balancer_v3_events') }}
    WHERE event_type = 'Swap'
      AND pool_address IS NOT NULL
      AND token_address IS NOT NULL
    GROUP BY pool_address
),

balancer_v3_tokenconfig_raw AS (
    SELECT
        replaceAll(lower(decoded_params['pool']), '0x', '') AS pool_address,
        token_idx AS token_index,
        lower(concat('0x', right(replaceAll(replaceAll(token_val, '"', ''), '0x', ''), 40))) AS token_address,
        token_address IN (
            '0x0000000000000000000000000000000000000000',
            '0x0000000000000000000000000000000000000001'
        ) AS is_sentinel,
        block_timestamp,
        log_index
    FROM {{ ref('contracts_BalancerV3_Vault_events') }}
    ARRAY JOIN
        range(length(JSONExtractArrayRaw(ifNull(decoded_params['tokenConfig'], '[]')))) AS token_idx,
        JSONExtractArrayRaw(ifNull(decoded_params['tokenConfig'], '[]')) AS token_val
    WHERE event_name = 'PoolRegistered'
      AND decoded_params['pool'] IS NOT NULL
      AND decoded_params['tokenConfig'] IS NOT NULL
),

balancer_v3_tokenconfig AS (
    SELECT
        pool_address,
        token_index,
        token_address,
        is_sentinel
    FROM (
        SELECT
            pool_address,
            token_index,
            token_address,
            is_sentinel,
            row_number() OVER (
                PARTITION BY pool_address, token_index
                ORDER BY block_timestamp DESC, log_index DESC
            ) AS rn
        FROM balancer_v3_tokenconfig_raw
    )
    WHERE rn = 1
),

balancer_v3_tokenconfig_stats AS (
    SELECT
        pool_address,
        countIf(not is_sentinel) AS valid_cnt,
        anyIf(token_address, not is_sentinel) AS any_valid_token
    FROM balancer_v3_tokenconfig
    GROUP BY pool_address
),

balancer_v3_pool_tokens AS (
    SELECT
        pool_address,
        token_index,
        protocol,
        token_address
    FROM (
        SELECT
            c.pool_address AS pool_address,
            c.token_index AS token_index,
            'Balancer V3' AS protocol,
            multiIf(
                not c.is_sentinel,
                c.token_address,
                length(ifNull(s.swap_tokens, [])) = 2 AND st.valid_cnt = 1,
                if(st.any_valid_token = s.swap_tokens[1], s.swap_tokens[2], s.swap_tokens[1]),
                length(ifNull(s.swap_tokens, [])) = 2 AND st.valid_cnt = 0,
                s.swap_tokens[toInt32(c.token_index) + 1],
                NULL
            ) AS token_address
        FROM balancer_v3_tokenconfig c
        LEFT JOIN balancer_v3_swap_tokens s
            ON s.pool_address = c.pool_address
        LEFT JOIN balancer_v3_tokenconfig_stats st
            ON st.pool_address = c.pool_address
    )
    WHERE token_address IS NOT NULL
),

balancer_v2_deltas AS (
    SELECT
        e.block_timestamp,
        e.transaction_hash,
        e.log_index,
        'Balancer V2' AS protocol,
        lower(e.token_address) AS token_address,
        e.delta_amount_raw,
        toInt256(0) AS fee_amount_raw,
        if(r.pool_address != '', r.pool_address, e.pool_id) AS pool_address
    FROM {{ ref('stg_pools__balancer_v2_events') }} e
    LEFT JOIN balancer_v2_pool_registry r
        ON r.pool_id = e.pool_id
    WHERE e.delta_amount_raw IS NOT NULL
      AND e.token_address IS NOT NULL
      AND e.pool_id IS NOT NULL
),

balancer_v3_deltas_pool AS (
    SELECT
        e.block_timestamp AS block_timestamp,
        e.transaction_hash AS transaction_hash,
        e.log_index AS log_index,
        p.protocol AS protocol,
        p.token_address AS token_address,
        e.delta_amount_raw AS delta_amount_raw,
        e.fee_amount_raw AS fee_amount_raw,
        p.pool_address AS pool_address
    FROM {{ ref('stg_pools__balancer_v3_events') }} e
    INNER JOIN balancer_v3_pool_tokens p
        ON e.pool_address = p.pool_address
       AND e.token_index = p.token_index
    WHERE e.delta_amount_raw IS NOT NULL
      AND e.token_index IS NOT NULL
      AND e.pool_address IS NOT NULL
),

balancer_v3_deltas_swap AS (
    SELECT
        e.block_timestamp AS block_timestamp,
        e.transaction_hash AS transaction_hash,
        e.log_index AS log_index,
        'Balancer V3' AS protocol,
        lower(e.token_address) AS token_address,
        e.delta_amount_raw AS delta_amount_raw,
        e.fee_amount_raw AS fee_amount_raw,
        e.pool_address AS pool_address
    FROM {{ ref('stg_pools__balancer_v3_events') }} e
    WHERE e.event_type = 'Swap'
      AND e.delta_amount_raw IS NOT NULL
      AND e.token_address IS NOT NULL
      AND e.pool_address IS NOT NULL
),

balancer_deltas AS (
    SELECT
        pool_address,
        toStartOfDay(block_timestamp) AS date,
        block_timestamp,
        protocol,
        token_address,
        delta_amount_raw,
        fee_amount_raw
    FROM (
        SELECT * FROM balancer_v2_deltas
        UNION ALL
        SELECT * FROM balancer_v3_deltas_pool
        UNION ALL
        SELECT * FROM balancer_v3_deltas_swap
    )
    WHERE delta_amount_raw IS NOT NULL
      AND token_address IS NOT NULL
      AND pool_address IS NOT NULL
      AND block_timestamp < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(block_timestamp) >= toDate('{{ start_month }}')
        AND toStartOfMonth(block_timestamp) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('block_timestamp', 'date', 'true') }}
      {% endif %}
),

balancer_daily_deltas AS (
    SELECT
        date,
        protocol,
        pool_address,
        token_address,
        sum(delta_amount_raw) AS daily_delta_raw,
        sum(delta_amount_raw - fee_amount_raw) AS daily_reserve_delta_raw,
        sum(fee_amount_raw) AS daily_fee_delta_raw
    FROM balancer_deltas
    GROUP BY date, protocol, pool_address, token_address
),

{% if start_month and end_month %}
balancer_prev_balances AS (
    SELECT
        protocol,
        pool_address,
        token_address,
        token_amount_raw AS balance_raw,
        reserve_amount_raw AS reserve_raw,
        fee_amount_raw AS fee_raw
    FROM {{ this }} FINAL
    WHERE protocol IN ('Balancer V2', 'Balancer V3')
      AND date = (
        SELECT max(date)
        FROM {{ this }} FINAL
        WHERE date < toDate('{{ start_month }}')
          AND protocol IN ('Balancer V2', 'Balancer V3')
      )
),
{% elif is_incremental() %}
balancer_prev_balances AS (
    SELECT
        protocol,
        pool_address,
        token_address,
        token_amount_raw AS balance_raw,
        reserve_amount_raw AS reserve_raw,
        fee_amount_raw AS fee_raw
    FROM {{ this }} FINAL
    WHERE protocol IN ('Balancer V2', 'Balancer V3')
      AND date = (
        SELECT max(date)
        FROM {{ this }} FINAL
        WHERE protocol IN ('Balancer V2', 'Balancer V3')
      )
),
{% endif %}

balancer_balances AS (
    SELECT
        date,
        protocol,
        pool_address,
        token_address,
        sum(daily_delta_raw) OVER (
            PARTITION BY protocol, pool_address, token_address
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
        {% if is_incremental() %}
            + coalesce(p.balance_raw, toInt256(0))
        {% endif %}
        AS balance_raw,
        sum(daily_reserve_delta_raw) OVER (
            PARTITION BY protocol, pool_address, token_address
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
        {% if is_incremental() %}
            + coalesce(p.reserve_raw, toInt256(0))
        {% endif %}
        AS reserve_raw,
        sum(daily_fee_delta_raw) OVER (
            PARTITION BY protocol, pool_address, token_address
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
        {% if is_incremental() %}
            + coalesce(p.fee_raw, toInt256(0))
        {% endif %}
        AS fee_raw
    FROM balancer_daily_deltas d
    {% if is_incremental() %}
    LEFT JOIN balancer_prev_balances p
        ON d.protocol = p.protocol
        AND d.pool_address = p.pool_address
        AND d.token_address = p.token_address
    {% endif %}
),

balancer_balances_final AS (
    SELECT
        b.date AS date,
        b.protocol AS protocol,
        b.pool_address AS pool_address,
        b.token_address AS token_address,
        b.balance_raw AS token_amount_raw,
        b.balance_raw / POWER(10, if(t.decimals > 0, t.decimals, if(wm.wrapper_decimals > 0, wm.wrapper_decimals, 18))) AS token_amount,
        b.reserve_raw AS reserve_amount_raw,
        b.reserve_raw / POWER(10, if(t.decimals > 0, t.decimals, if(wm.wrapper_decimals > 0, wm.wrapper_decimals, 18))) AS reserve_amount,
        b.fee_raw AS fee_amount_raw,
        b.fee_raw / POWER(10, if(t.decimals > 0, t.decimals, if(wm.wrapper_decimals > 0, wm.wrapper_decimals, 18))) AS fee_amount
    FROM balancer_balances b
    LEFT JOIN {{ ref('stg_pools__balancer_v3_token_map') }} wm
        ON wm.wrapper_address = b.token_address
    LEFT JOIN {{ ref('tokens_whitelist') }} t
        ON lower(t.address) = coalesce(nullIf(wm.underlying_address, ''), b.token_address)
       AND b.date >= toDate(t.date_start)
    WHERE b.balance_raw != 0
),

/* ========================================
   Combine all protocols
   ======================================== */

all_balances AS (
    SELECT
        date, protocol, pool_address, token_address,
        token_amount_raw, token_amount,
        reserve_amount_raw, reserve_amount,
        fee_amount_raw, fee_amount
    FROM v3_balances_final

    UNION ALL

    SELECT
        date, protocol, pool_address, token_address,
        token_amount_raw, token_amount,
        reserve_amount_raw, reserve_amount,
        fee_amount_raw, fee_amount
    FROM balancer_balances_final
),

final AS (
    SELECT
        *
    FROM all_balances
    WHERE NOT (
        protocol IN ('Balancer V2', 'Balancer V3')
        AND (
            (protocol = 'Balancer V2' AND lower(token_address) = lower(pool_address))
            OR (protocol = 'Balancer V3' AND lower(token_address) = concat('0x', lower(pool_address)))
        )
    )
      AND lower(token_address) NOT IN (
        '0x0000000000000000000000000000000000000000',
        '0x0000000000000000000000000000000000000001'
      )
)

SELECT
    date,
    protocol,
    pool_address,
    token_address,
    token_amount_raw,
    token_amount,
    reserve_amount_raw,
    reserve_amount,
    fee_amount_raw,
    fee_amount
FROM final
