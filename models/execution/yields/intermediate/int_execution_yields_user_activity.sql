{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(block_timestamp, source, transaction_hash, log_index)',
        unique_key='(block_timestamp, source, transaction_hash, log_index)',
        partition_by='toStartOfMonth(block_timestamp)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'yields', 'user_portfolio', 'intermediate']
    )
}}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

WITH

lp_events AS (
    SELECT
        block_timestamp,
        transaction_hash,
        log_index,
        protocol,
        pool_address                                       AS position_address,
        provider                                           AS wallet_address,
        multiIf(
            event_type = 'mint',    'Add Liquidity',
            event_type = 'burn',    'Remove Liquidity',
            'Collect Fees'
        )                                                  AS action,
        token_symbol,
        token_address,
        amount,
        amount_usd,
        'lp'                                               AS source
    FROM {{ ref('int_execution_pools_dex_liquidity_events') }}
    WHERE provider IS NOT NULL
      AND provider != ''
      {% if not (start_month and end_month) %}
        {{ apply_monthly_incremental_filter('block_timestamp', 'block_timestamp', 'true') }}
      {% endif %}
),

pool_events_raw AS (
    SELECT 'Aave V3'   AS protocol, * FROM {{ ref('contracts_aaveV3_PoolInstance_events') }}
    UNION ALL
    SELECT 'SparkLend' AS protocol, * FROM {{ ref('contracts_spark_Pool_events') }}
),

lending_events AS (
    SELECT
        e.block_timestamp,
        e.transaction_hash,
        e.log_index,
        e.protocol                                         AS protocol,
        lower(e.decoded_params['reserve'])                 AS position_address,
        lower(
            multiIf(
                e.event_name = 'Supply',  e.decoded_params['onBehalfOf'],
                e.event_name = 'Withdraw', e.decoded_params['user'],
                e.event_name = 'Borrow',  e.decoded_params['onBehalfOf'],
                e.event_name = 'Repay',   e.decoded_params['user'],
                e.decoded_params['user']
            )
        )                                                  AS wallet_address,
        multiIf(
            e.event_name = 'Supply',   'Supply',
            e.event_name = 'Withdraw', 'Withdraw',
            e.event_name = 'Borrow',   'Borrow',
            'Repay'
        )                                                  AS action,
        rm.reserve_symbol                                  AS token_symbol,
        lower(e.decoded_params['reserve'])                 AS token_address,
        toFloat64(toUInt256OrNull(e.decoded_params['amount']))
            / power(10, rm.decimals)                       AS amount,
        toFloat64(toUInt256OrNull(e.decoded_params['amount']))
            / power(10, rm.decimals)
            * coalesce(pr.price, 0)                        AS amount_usd,
        'lending'                                          AS source
    FROM pool_events_raw e
    INNER JOIN {{ ref('lending_market_mapping') }} rm
        ON  rm.protocol             = e.protocol
       AND lower(rm.reserve_address) = lower(e.decoded_params['reserve'])
    LEFT JOIN {{ ref('int_execution_token_prices_daily') }} pr
        ON pr.symbol = rm.reserve_symbol
       AND pr.date   = toDate(e.block_timestamp)
    WHERE e.event_name IN ('Supply', 'Withdraw', 'Borrow', 'Repay')
      AND e.decoded_params['reserve'] IS NOT NULL
      AND e.decoded_params['amount'] IS NOT NULL
      AND e.block_timestamp < today()
      {% if not (start_month and end_month) %}
        {{ apply_monthly_incremental_filter('e.block_timestamp', 'block_timestamp', 'true') }}
      {% endif %}
)

SELECT
    block_timestamp,
    transaction_hash,
    log_index,
    protocol,
    position_address,
    wallet_address,
    action,
    token_symbol,
    token_address,
    round(amount, 6)      AS amount,
    round(amount_usd, 2)  AS amount_usd,
    source
FROM lp_events

UNION ALL

SELECT
    block_timestamp,
    transaction_hash,
    log_index,
    protocol,
    position_address,
    wallet_address,
    action,
    token_symbol,
    token_address,
    round(amount, 6)      AS amount,
    round(amount_usd, 2)  AS amount_usd,
    source
FROM lending_events
