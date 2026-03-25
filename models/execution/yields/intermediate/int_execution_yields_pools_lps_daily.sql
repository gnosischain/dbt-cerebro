{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date, protocol, pool_address)',
        unique_key='(date, protocol, pool_address)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['dev', 'execution', 'yields', 'pools', 'lps', 'intermediate']
    )
}}

{#-
  Daily LP provider activity from Mint + Burn events on Uniswap V3 and Swapr V3.
  Output: (date, protocol, pool_address, mint_count, burn_count,
           lps_minting_daily, lps_burning_daily, lps_bitmap_state)

  Notes:
  - `owner` in Mint/Burn events is the actual LP position owner address,
    even when the call goes through a router.
  - Bitmap tracks unique LP addresses via cityHash64 for rolling-window
    deduplication using groupBitmapMerge() downstream.
  - Contract event models already filter to whitelisted pool addresses.
-#}

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

balancer_v3_lp_events AS (
    SELECT
        toDate(toStartOfDay(block_timestamp)) AS date,
        'Balancer V3' AS protocol,
        concat('0x', replaceAll(lower(decoded_params['pool']), '0x', '')) AS pool_address,
        multiIf(
            event_name = 'LiquidityAdded', 'Mint',
            event_name = 'LiquidityRemoved', 'Burn',
            event_name
        ) AS event_name,
        lower(decoded_params['liquidityProvider']) AS lp_address
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
