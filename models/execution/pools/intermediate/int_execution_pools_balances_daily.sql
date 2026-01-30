{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date, protocol, pool_address, token_address)',
        unique_key='(date, protocol, pool_address, token_address)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['dev', 'execution', 'pools', 'balances', 'intermediate']
    )
}}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}

WITH
contracts_whitelist AS (
    SELECT
        lower(address) AS pool_address,
        contract_type
    FROM {{ ref('contracts_whitelist') }}
),

uniswap_v3_pools AS (
    SELECT DISTINCT
        multiIf(
            startsWith(lower(decoded_params['pool']), '0x'),
            lower(decoded_params['pool']),
            concat('0x', replaceAll(lower(decoded_params['pool']), '0x', ''))
        ) AS pool_address,
        lower(decoded_params['token0']) AS token0,
        lower(decoded_params['token1']) AS token1,
        'Uniswap V3' AS protocol
    FROM {{ ref('contracts_UniswapV3_Factory_events') }} f
    INNER JOIN contracts_whitelist w
      ON w.pool_address = multiIf(
          startsWith(lower(f.decoded_params['pool']), '0x'),
          lower(f.decoded_params['pool']),
          concat('0x', replaceAll(lower(f.decoded_params['pool']), '0x', ''))
      )
     AND w.contract_type = 'UniswapV3Pool'
    WHERE event_name = 'PoolCreated'
      AND decoded_params['pool'] IS NOT NULL
      AND decoded_params['token0'] IS NOT NULL
      AND decoded_params['token1'] IS NOT NULL
),

swapr_v3_pools AS (
    SELECT DISTINCT
        multiIf(
            startsWith(lower(decoded_params['pool']), '0x'),
            lower(decoded_params['pool']),
            concat('0x', replaceAll(lower(decoded_params['pool']), '0x', ''))
        ) AS pool_address,
        lower(decoded_params['token0']) AS token0,
        lower(decoded_params['token1']) AS token1,
        'Swapr V3' AS protocol
    FROM {{ ref('contracts_Swapr_v3_AlgebraFactory_events') }} f
    INNER JOIN contracts_whitelist w
      ON w.pool_address = multiIf(
          startsWith(lower(f.decoded_params['pool']), '0x'),
          lower(f.decoded_params['pool']),
          concat('0x', replaceAll(lower(f.decoded_params['pool']), '0x', ''))
      )
     AND w.contract_type = 'SwaprPool'
    WHERE event_name = 'Pool'
      AND decoded_params['pool'] IS NOT NULL
      AND decoded_params['token0'] IS NOT NULL
      AND decoded_params['token1'] IS NOT NULL
),

pool_tokens AS (
    SELECT
        protocol,
        pool_address,
        token0 AS token_address
    FROM uniswap_v3_pools

    UNION ALL

    SELECT
        protocol,
        pool_address,
        token1 AS token_address
    FROM uniswap_v3_pools

    UNION ALL

    SELECT
        protocol,
        pool_address,
        token0 AS token_address
    FROM swapr_v3_pools

    UNION ALL

    SELECT
        protocol,
        pool_address,
        token1 AS token_address
    FROM swapr_v3_pools
),

balances_daily AS (
    SELECT
        date,
        lower(token_address) AS token_address,
        lower(address) AS address,
        balance_raw,
        balance AS token_amount
    FROM {{ ref('int_execution_tokens_balances_daily') }}
    WHERE date < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(date) >= toDate('{{ start_month }}')
        AND toStartOfMonth(date) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('date', 'date', 'true') }}
      {% endif %}
),

transfer_based_balances AS (
    SELECT
        b.date AS date,
        p.protocol AS protocol,
        p.pool_address AS pool_address,
        p.token_address AS token_address,
        b.balance_raw AS token_amount_raw,
        b.token_amount AS token_amount
    FROM balances_daily b
    INNER JOIN pool_tokens p
        ON b.address = p.pool_address
       AND b.token_address = p.token_address
),

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
                -- If the config contains a sentinel slot, infer it from swap tokens (2-token pools).
                length(ifNull(s.swap_tokens, [])) = 2 AND st.valid_cnt = 1,
                if(st.any_valid_token = s.swap_tokens[1], s.swap_tokens[2], s.swap_tokens[1]),
                -- If both slots are sentinel (rare), fall back to swap token ordering by index.
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
        coalesce(r.pool_address, e.pool_id) AS pool_address
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
        delta_amount_raw
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
        sum(delta_amount_raw) AS daily_delta_raw
    FROM balancer_deltas
    GROUP BY date, protocol, pool_address, token_address
),

{% if start_month and end_month %}
balancer_prev_balances AS (
    SELECT
        protocol,
        pool_address,
        token_address,
        token_amount_raw AS balance_raw
    FROM {{ this }}
    WHERE protocol IN ('Balancer V2', 'Balancer V3')
      AND date = (
        SELECT max(date)
        FROM {{ this }}
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
        token_amount_raw AS balance_raw
    FROM {{ this }}
    WHERE protocol IN ('Balancer V2', 'Balancer V3')
      AND date = (
        SELECT max(date)
        FROM {{ this }}
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
        AS balance_raw
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
        b.balance_raw / POWER(10, COALESCE(t.decimals, 18)) AS token_amount
    FROM balancer_balances b
    LEFT JOIN {{ ref('tokens_whitelist') }} t
        ON lower(t.address) = b.token_address
       AND b.date >= toDate(t.date_start)
       AND (t.date_end IS NULL OR b.date < toDate(t.date_end))
    WHERE b.balance_raw != 0
),

all_balances AS (
    SELECT
        date,
        protocol,
        pool_address,
        token_address,
        token_amount_raw,
        token_amount
    FROM transfer_based_balances

    UNION ALL

    SELECT
        date,
        protocol,
        pool_address,
        token_address,
        token_amount_raw,
        token_amount
    FROM balancer_balances_final
),

final AS (
    SELECT
        *
    FROM all_balances
    WHERE NOT (
        protocol IN ('Balancer V2', 'Balancer V3')
        AND (
            -- Balancer V2 pool_address is a normal 0x address
            (protocol = 'Balancer V2' AND lower(token_address) = lower(pool_address))
            -- Balancer V3 pool_address is stored without 0x in this project
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
    token_amount
FROM final
