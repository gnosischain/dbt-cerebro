{{
    config(
        materialized='table',
        engine='ReplacingMergeTree()',
        order_by='(provider, pool_address, tick_lower_key, tick_upper_key)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'yields', 'user_portfolio', 'intermediate']
    )
}}

WITH

position_amounts AS (
    SELECT
        provider,
        pool_address,
        protocol,
        tick_lower,
        tick_upper,
        coalesce(tick_lower, toInt32(0))              AS tick_lower_key,
        coalesce(tick_upper, toInt32(0))              AS tick_upper_key,
        coalesce(sumIf(amount_usd, event_type = 'mint'), 0)    AS capital_in_usd,
        coalesce(sumIf(amount_usd, event_type = 'burn'), 0)    AS capital_out_usd,
        coalesce(sumIf(amount_usd, event_type = 'collect'), 0) AS fees_collected_usd,
        min(block_timestamp)                           AS entry_date,
        max(block_timestamp)                           AS last_action_date
    FROM {{ ref('int_execution_pools_dex_liquidity_events') }}
    GROUP BY provider, pool_address, protocol, tick_lower, tick_upper,
             tick_lower_key, tick_upper_key
),

liquidity_per_event AS (
    SELECT DISTINCT
        provider,
        pool_address,
        coalesce(tick_lower, toInt32(0)) AS tick_lower_key,
        coalesce(tick_upper, toInt32(0)) AS tick_upper_key,
        transaction_hash,
        log_index,
        liquidity_delta
    FROM {{ ref('int_execution_pools_dex_liquidity_events') }}
    WHERE event_type IN ('mint', 'burn')
      AND liquidity_delta IS NOT NULL
),

net_liquidity AS (
    SELECT
        provider,
        pool_address,
        tick_lower_key,
        tick_upper_key,
        sum(liquidity_delta) AS net_liquidity
    FROM liquidity_per_event
    GROUP BY provider, pool_address, tick_lower_key, tick_upper_key
),

balancer_active AS (
    SELECT
        provider,
        pool_address,
        max(has_positive_balance) AS has_active_tokens
    FROM (
        SELECT
            provider,
            pool_address,
            token_address,
            coalesce(sumIf(amount_raw, event_type = 'mint'), toUInt256(0))
              > coalesce(sumIf(amount_raw, event_type = 'burn'), toUInt256(0)) AS has_positive_balance
        FROM {{ ref('int_execution_pools_dex_liquidity_events') }}
        WHERE tick_lower IS NULL
          AND event_type IN ('mint', 'burn')
        GROUP BY provider, pool_address, token_address
    )
    GROUP BY provider, pool_address
),

current_ticks AS (
    SELECT pool_address, current_tick
    FROM {{ ref('stg_pools__v3_current_tick') }}
)

SELECT
    pa.provider                              AS provider,
    pa.pool_address                          AS pool_address,
    pa.protocol                              AS protocol,
    pa.tick_lower                            AS tick_lower,
    pa.tick_upper                            AS tick_upper,
    pa.tick_lower_key                        AS tick_lower_key,
    pa.tick_upper_key                        AS tick_upper_key,
    round(pa.capital_in_usd, 2)              AS capital_in_usd,
    round(pa.capital_out_usd, 2)             AS capital_out_usd,
    multiIf(
        pa.tick_lower IS NOT NULL, round(pa.fees_collected_usd, 2),
        coalesce(ba.has_active_tokens, 0) = 0 AND pa.capital_out_usd > pa.capital_in_usd,
            round(pa.capital_out_usd - pa.capital_in_usd, 2),
        0
    )                                        AS fees_collected_usd,
    coalesce(nl.net_liquidity, toInt256(0))  AS net_liquidity,
    multiIf(
        pa.tick_lower IS NOT NULL, nl.net_liquidity > toInt256(0),
        coalesce(ba.has_active_tokens, 0) = 1
    )                                        AS is_active,
    multiIf(
        pa.tick_lower IS NULL, true,
        ct.current_tick IS NULL, NULL,
        pa.tick_lower <= ct.current_tick AND ct.current_tick < pa.tick_upper
    )                                        AS is_in_range,
    ct.current_tick                          AS pool_current_tick,
    pa.entry_date                            AS entry_date,
    pa.last_action_date                      AS last_action_date
FROM position_amounts pa
LEFT JOIN net_liquidity nl
    ON  nl.provider       = pa.provider
    AND nl.pool_address   = pa.pool_address
    AND nl.tick_lower_key = pa.tick_lower_key
    AND nl.tick_upper_key = pa.tick_upper_key
LEFT JOIN balancer_active ba
    ON  ba.provider     = pa.provider
    AND ba.pool_address = pa.pool_address
    AND pa.tick_lower IS NULL
LEFT JOIN current_ticks ct
    ON ct.pool_address = pa.pool_address
