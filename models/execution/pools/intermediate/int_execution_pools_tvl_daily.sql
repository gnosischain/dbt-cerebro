{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date, protocol, pool_address, token_address)',
        unique_key='(date, protocol, pool_address, token_address)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'pools', 'tvl', 'intermediate']
    )
}}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}

WITH

uniswap_v3_pools AS (
    SELECT DISTINCT
        replaceAll(lower(decoded_params['pool']), '0x', '') AS pool_address,
        lower(decoded_params['token0']) AS token0,
        lower(decoded_params['token1']) AS token1,
        'Uniswap V3' AS protocol
    FROM {{ ref('contracts_UniswapV3_Factory_events') }}
    WHERE event_name = 'PoolCreated'
      AND decoded_params['pool'] IS NOT NULL
      AND decoded_params['token0'] IS NOT NULL
      AND decoded_params['token1'] IS NOT NULL
),

swapr_v3_pools AS (
    SELECT DISTINCT
        replaceAll(lower(decoded_params['pool']), '0x', '') AS pool_address,
        lower(decoded_params['token0']) AS token0,
        lower(decoded_params['token1']) AS token1,
        'Swapr V3' AS protocol
    FROM {{ ref('contracts_Swapr_v3_AlgebraFactory_events') }}
    WHERE event_name = 'Pool'
      AND decoded_params['pool'] IS NOT NULL
      AND decoded_params['token0'] IS NOT NULL
      AND decoded_params['token1'] IS NOT NULL
),

balancer_v3_pool_tokens AS (
    SELECT
        replaceAll(lower(decoded_params['pool']), '0x', '') AS pool_address,
        lower(JSONExtractString(token_val, '')) AS token_address,
        row_number() OVER (PARTITION BY lower(decoded_params['pool']) ORDER BY token_idx) - 1 AS token_index,
        'Balancer V3' AS protocol
    FROM {{ ref('contracts_BalancerV3_Vault_events') }}
    ARRAY JOIN 
        range(length(JSONExtractArrayRaw(ifNull(decoded_params['tokenConfig'], '[]')))) AS token_idx,
        JSONExtractArrayRaw(ifNull(decoded_params['tokenConfig'], '[]')) AS token_val
    WHERE event_name = 'PoolRegistered'
      AND decoded_params['pool'] IS NOT NULL
      AND decoded_params['tokenConfig'] IS NOT NULL
      AND JSONExtractString(token_val, '') != '0x0000000000000000000000000000000000000000000000000000000000000000'
),

balancer_v3_pools AS (
    SELECT
        pool_address,
        token_address,
        token_index,
        protocol
    FROM balancer_v3_pool_tokens
),

uniswap_v3_deltas AS (
    SELECT
        e.pool_address,
        e.block_timestamp,
        e.transaction_hash,
        e.log_index,
        p.protocol,
        CASE 
            WHEN e.token_position = 'token0' THEN p.token0
            WHEN e.token_position = 'token1' THEN p.token1
        END AS token_address,
        e.delta_amount_raw
    FROM {{ ref('stg_pools__uniswap_v3_events') }} e
    INNER JOIN uniswap_v3_pools p
        ON e.pool_address = p.pool_address
    WHERE e.delta_amount_raw IS NOT NULL
),

swapr_v3_deltas AS (
    SELECT
        e.pool_address,
        e.block_timestamp,
        e.transaction_hash,
        e.log_index,
        p.protocol,
        CASE 
            WHEN e.token_position = 'token0' THEN p.token0
            WHEN e.token_position = 'token1' THEN p.token1
        END AS token_address,
        e.delta_amount_raw
    FROM {{ ref('stg_pools__swapr_v3_events') }} e
    INNER JOIN swapr_v3_pools p
        ON e.pool_address = p.pool_address
    WHERE e.delta_amount_raw IS NOT NULL
),

balancer_v2_deltas AS (
    SELECT
        e.pool_id AS pool_address,
        e.block_timestamp,
        e.transaction_hash,
        e.log_index,
        'Balancer V2' AS protocol,
        e.token_address,
        e.delta_amount_raw
    FROM {{ ref('stg_pools__balancer_v2_events') }} e
    WHERE e.delta_amount_raw IS NOT NULL
      AND e.token_address IS NOT NULL
),

balancer_v3_deltas_pool AS (
    SELECT
        e.pool_address,
        e.block_timestamp,
        e.transaction_hash,
        e.log_index,
        p.protocol,
        p.token_address,
        e.delta_amount_raw
    FROM {{ ref('stg_pools__balancer_v3_events') }} e
    INNER JOIN balancer_v3_pools p
        ON e.pool_address = p.pool_address
        AND toInt32OrNull(e.token_index) = p.token_index
    WHERE e.delta_amount_raw IS NOT NULL
      AND e.token_index IS NOT NULL
      AND e.pool_address IS NOT NULL
),

balancer_v3_deltas_swap AS (
    SELECT
        e.pool_address,
        e.block_timestamp,
        e.transaction_hash,
        e.log_index,
        'Balancer V3' AS protocol,
        e.token_address,
        e.delta_amount_raw
    FROM {{ ref('stg_pools__balancer_v3_events') }} e
    WHERE e.event_type = 'Swap'
      AND e.delta_amount_raw IS NOT NULL
      AND e.token_address IS NOT NULL
      AND e.pool_address IS NOT NULL
),

all_deltas AS (
    SELECT
        pool_address,
        toStartOfDay(block_timestamp) AS date,
        block_timestamp,
        protocol,
        token_address,
        delta_amount_raw
    FROM (
        SELECT * FROM uniswap_v3_deltas
        UNION ALL
        SELECT * FROM swapr_v3_deltas
        UNION ALL
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

daily_deltas AS (
    SELECT
        date,
        protocol,
        pool_address,
        token_address,
        sum(delta_amount_raw) AS daily_delta_raw
    FROM all_deltas
    GROUP BY date, protocol, pool_address, token_address
),


{% if start_month and end_month %}
prev_balances AS (
    SELECT
        protocol,
        pool_address,
        token_address,
        token_amount_raw AS balance_raw
    FROM {{ this }}
    WHERE date = (
        SELECT max(date)
        FROM {{ this }}
        WHERE date < toDate('{{ start_month }}')
    )
),
{% elif is_incremental() %}
prev_balances AS (
    SELECT
        protocol,
        pool_address,
        token_address,
        token_amount_raw AS balance_raw
    FROM {{ this }}
    WHERE date = (
        SELECT max(date)
        FROM {{ this }}
    )
),
{% endif %}

balances AS (
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
    FROM daily_deltas d
    {% if is_incremental() %}
    LEFT JOIN prev_balances p
        ON d.protocol = p.protocol
        AND d.pool_address = p.pool_address
        AND d.token_address = p.token_address
    {% endif %}
)

SELECT
    date,
    protocol,
    pool_address,
    token_address,
    balance_raw AS token_amount_raw,
    balance_raw / POWER(10, COALESCE(t.decimals, 18)) AS token_amount
FROM balances b
LEFT JOIN {{ ref('tokens_whitelist') }} t
    ON lower(t.address) = b.token_address
WHERE balance_raw != 0
ORDER BY date, protocol, pool_address, token_address
