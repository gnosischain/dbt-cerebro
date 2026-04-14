{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date, protocol, pool_address)',
        unique_key='(date, protocol, pool_address)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'pools', 'lps', 'intermediate']
    )
}}

{#- Model documentation in schema.yml -#}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}

WITH

uniswap_v3_lp_events AS (
    SELECT
        toDate(toStartOfDay(block_timestamp)) AS date,
        'Uniswap V3' AS protocol,
        concat('0x', replaceAll(lower(contract_address), '0x', '')) AS pool_address,
        event_name,
        lower(decoded_params['owner']) AS lp_address
    FROM {{ ref('contracts_UniswapV3_Pool_events') }}
    WHERE event_name IN ('Mint', 'Burn')
      AND decoded_params['owner'] IS NOT NULL
      AND block_timestamp < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(block_timestamp) >= toDate('{{ start_month }}')
        AND toStartOfMonth(block_timestamp) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('block_timestamp', 'date', 'true') }}
      {% endif %}
),

swapr_v3_lp_events AS (
    SELECT
        toDate(toStartOfDay(block_timestamp)) AS date,
        'Swapr V3' AS protocol,
        concat('0x', replaceAll(lower(contract_address), '0x', '')) AS pool_address,
        event_name,
        lower(decoded_params['owner']) AS lp_address
    FROM {{ ref('contracts_Swapr_v3_AlgebraPool_events') }}
    WHERE event_name IN ('Mint', 'Burn')
      AND decoded_params['owner'] IS NOT NULL
      AND block_timestamp < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(block_timestamp) >= toDate('{{ start_month }}')
        AND toStartOfMonth(block_timestamp) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('block_timestamp', 'date', 'true') }}
      {% endif %}
),

balancer_v3_lp_events_raw AS (
    SELECT
        block_timestamp,
        event_name AS raw_event_name,
        decoded_params['pool'] AS pool_param,
        decoded_params['liquidityProvider'] AS lp_param
    FROM {{ ref('contracts_BalancerV3_Vault_events') }}
    WHERE event_name IN ('LiquidityAdded', 'LiquidityRemoved')
      AND decoded_params['pool'] IS NOT NULL
      AND decoded_params['liquidityProvider'] IS NOT NULL
      AND block_timestamp < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(block_timestamp) >= toDate('{{ start_month }}')
        AND toStartOfMonth(block_timestamp) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('block_timestamp', 'date', 'true') }}
      {% endif %}
),

balancer_v3_lp_events AS (
    SELECT
        toDate(toStartOfDay(block_timestamp)) AS date,
        'Balancer V3' AS protocol,
        concat('0x', replaceAll(lower(pool_param), '0x', '')) AS pool_address,
        multiIf(
            raw_event_name = 'LiquidityAdded', 'Mint',
            raw_event_name = 'LiquidityRemoved', 'Burn',
            raw_event_name
        ) AS event_name,
        lower(lp_param) AS lp_address
    FROM balancer_v3_lp_events_raw
),

all_lp_events AS (
    SELECT * FROM uniswap_v3_lp_events
    UNION ALL
    SELECT * FROM swapr_v3_lp_events
    UNION ALL
    SELECT * FROM balancer_v3_lp_events
)

SELECT
    date,
    protocol,
    pool_address,
    countIf(event_name = 'Mint') AS mint_count,
    countIf(event_name = 'Burn') AS burn_count,
    uniqExactIf(lp_address, event_name = 'Mint') AS lps_minting_daily,
    uniqExactIf(lp_address, event_name = 'Burn') AS lps_burning_daily,
    groupBitmapState(cityHash64(lp_address)) AS lps_bitmap_state
FROM all_lp_events
WHERE lp_address IS NOT NULL
  AND lp_address != ''
GROUP BY date, protocol, pool_address
