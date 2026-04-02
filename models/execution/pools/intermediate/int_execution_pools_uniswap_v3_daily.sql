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

{#-
  Daily Uniswap V3 pool token balances with oracle price enrichment.

  Built from event deltas (Mint, Burn, Swap, Collect, Flash).
  Separates reserves (TVL-contributing) from unclaimed fees:
    * reserve_amount: Mint/Burn liquidity + swap deltas net of fees
    * fee_amount: swap fee income - collected fees + flash loan fees
  Fee rates: static from PoolCreated events (fee_tier_ppm).

  Includes oracle price (Dune), TVL in USD, and pool-implied token price.
-#}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}

WITH

pools AS (
    SELECT
        pool_address,
        replaceAll(pool_address, '0x', '') AS pool_address_no0x,
        token0_address,
        token1_address,
        fee_tier_ppm
    FROM {{ ref('stg_pools__v3_pool_registry') }}
    WHERE protocol = 'Uniswap V3'
      AND pool_address IN (
          SELECT lower(address)
          FROM {{ ref('contracts_whitelist') }}
          WHERE contract_type = 'UniswapV3Pool'
      )
),

daily_deltas AS (
    SELECT
        toDate(toStartOfDay(e.block_timestamp)) AS date,
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
        'Uniswap V3' AS protocol,
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
        pr.price_usd AS price_usd,
        (b.reserve_raw / POWER(10, if(tm.decimals > 0, tm.decimals, 18))) * pr.price_usd AS tvl_component_usd
    FROM balances b
    LEFT JOIN {{ ref('stg_pools__tokens_meta') }} tm
        ON tm.token_address = b.token_address
       AND b.date >= toDate(tm.date_start)
    ASOF LEFT JOIN (
        SELECT * FROM {{ ref('stg_pools__token_prices_daily') }} ORDER BY token, date
    ) pr
        ON pr.token = tm.token
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
