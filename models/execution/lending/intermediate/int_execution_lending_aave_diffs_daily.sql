{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        on_schema_change='sync_all_columns',
        engine='ReplacingMergeTree()',
        order_by='(date, protocol, reserve_address, user_address)',
        unique_key='(date, protocol, reserve_address, user_address)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['production','execution','lending','aave','spark','diffs']
    )
}}

-- NOTE: diff_scaled is Int256 for exact scaled-balance arithmetic (no Float64 precision loss
-- on amounts above 2^53 wei). When deploying this model for the first time after the
-- Float64 -> Int256 migration, a --full-refresh is required so the column type is recreated.

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}

WITH

reserve_map AS (
    SELECT
        protocol,
        lower(supply_token_address) AS atoken_address,
        lower(reserve_address)      AS reserve_address,
        reserve_symbol,
        decimals
    FROM {{ ref('lending_market_mapping') }}
),

pool_events_raw AS (
    SELECT 'Aave V3'   AS protocol, * FROM {{ ref('contracts_aaveV3_PoolInstance_events') }}
    UNION ALL
    SELECT 'SparkLend' AS protocol, * FROM {{ ref('contracts_spark_Pool_events') }}
),

atoken_events_raw AS (
    SELECT 'Aave V3'   AS protocol, * FROM {{ ref('contracts_aaveV3_AToken_events') }}
    UNION ALL
    SELECT 'SparkLend' AS protocol, * FROM {{ ref('contracts_spark_AToken_events') }}
),

-- ReserveDataUpdated events carry the liquidityIndex snapshot applied immediately
-- before each user action in the same (tx, reserve). We do NOT rank them; instead we
-- ASOF-join each pool action below to the RDU with the largest log_index < the action's
-- log_index. This handles Spark correctly, where FlashLoan RDUs interleave with user
-- action RDUs and rank-based pairing would misalign (e.g. pair the user's Supply with
-- the FlashLoan's RDU).
reserve_index_by_tx AS (
    SELECT
        e.protocol,
        e.transaction_hash,
        lower(e.decoded_params['reserve']) AS reserve_address,
        e.log_index,
        toUInt256OrZero(e.decoded_params['liquidityIndex']) AS liquidity_index
    FROM pool_events_raw e
    INNER JOIN reserve_map rm
        ON rm.protocol        = e.protocol
       AND rm.reserve_address = lower(e.decoded_params['reserve'])
    WHERE e.event_name = 'ReserveDataUpdated'
      AND e.block_timestamp < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(e.block_timestamp) >= toDate('{{ start_month }}')
        AND toStartOfMonth(e.block_timestamp) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('e.block_timestamp', 'date', 'true') }}
      {% endif %}
),

pool_events AS (
    SELECT
        protocol,
        toDate(block_timestamp) AS date,
        transaction_hash,
        log_index,
        lower(decoded_params['reserve']) AS reserve_address,
        lower(decoded_params['onBehalfOf']) AS user_address,
        'Supply' AS action,
        toUInt256OrZero(decoded_params['amount']) AS amount
    FROM pool_events_raw
    WHERE event_name = 'Supply'
      AND decoded_params['reserve'] IS NOT NULL
      AND decoded_params['amount'] IS NOT NULL
      AND block_timestamp < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(block_timestamp) >= toDate('{{ start_month }}')
        AND toStartOfMonth(block_timestamp) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('block_timestamp', 'date', 'true') }}
      {% endif %}

    UNION ALL

    SELECT
        protocol,
        toDate(block_timestamp) AS date,
        transaction_hash,
        log_index,
        lower(decoded_params['reserve']) AS reserve_address,
        lower(decoded_params['user']) AS user_address,
        'Withdraw' AS action,
        toUInt256OrZero(decoded_params['amount']) AS amount
    FROM pool_events_raw
    WHERE event_name = 'Withdraw'
      AND decoded_params['reserve'] IS NOT NULL
      AND decoded_params['amount'] IS NOT NULL
      AND block_timestamp < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(block_timestamp) >= toDate('{{ start_month }}')
        AND toStartOfMonth(block_timestamp) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('block_timestamp', 'date', 'true') }}
      {% endif %}

    UNION ALL

    SELECT
        protocol,
        toDate(block_timestamp) AS date,
        transaction_hash,
        log_index,
        lower(decoded_params['reserve']) AS reserve_address,
        lower(decoded_params['repayer']) AS user_address,
        'RepayWithATokens' AS action,
        toUInt256OrZero(decoded_params['amount']) AS amount
    FROM pool_events_raw
    WHERE event_name = 'Repay'
      AND decoded_params['useATokens'] = '1'
      AND decoded_params['reserve'] IS NOT NULL
      AND decoded_params['amount'] IS NOT NULL
      AND block_timestamp < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(block_timestamp) >= toDate('{{ start_month }}')
        AND toStartOfMonth(block_timestamp) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('block_timestamp', 'date', 'true') }}
      {% endif %}

    UNION ALL

    SELECT
        protocol,
        toDate(block_timestamp) AS date,
        transaction_hash,
        log_index,
        lower(decoded_params['collateralAsset']) AS reserve_address,
        lower(decoded_params['user']) AS user_address,
        'LiquidationWithdraw' AS action,
        toUInt256OrZero(decoded_params['liquidatedCollateralAmount']) AS amount
    FROM pool_events_raw
    WHERE event_name = 'LiquidationCall'
      AND decoded_params['receiveAToken'] = '0'
      AND decoded_params['collateralAsset'] IS NOT NULL
      AND decoded_params['liquidatedCollateralAmount'] IS NOT NULL
      AND block_timestamp < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(block_timestamp) >= toDate('{{ start_month }}')
        AND toStartOfMonth(block_timestamp) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('block_timestamp', 'date', 'true') }}
      {% endif %}
),

-- Filter to in-scope reserves.
pool_events_scoped AS (
    SELECT
        pe.protocol,
        pe.date,
        pe.transaction_hash,
        pe.log_index,
        pe.reserve_address,
        pe.user_address,
        pe.action,
        pe.amount
    FROM pool_events pe
    INNER JOIN reserve_map rm
      ON rm.protocol        = pe.protocol
     AND rm.reserve_address = pe.reserve_address
),

pool_deltas AS (
    SELECT
        pe.protocol        AS protocol,
        pe.date            AS date,
        pe.user_address    AS user_address,
        pe.reserve_address AS reserve_address,
        CASE
            WHEN pe.action = 'Supply' THEN
                toInt256(
                    intDiv(
                        pe.amount * toUInt256OrZero('1000000000000000000000000000'),
                        ri.liquidity_index
                    )
                )
            ELSE
                -toInt256(
                    intDiv(
                        pe.amount * toUInt256OrZero('1000000000000000000000000000')
                            + ri.liquidity_index - toUInt256OrZero('1'),
                        ri.liquidity_index
                    )
                )
        END AS scaled_delta
    FROM pool_events_scoped pe
    ASOF INNER JOIN reserve_index_by_tx ri
        ON ri.protocol         = pe.protocol
       AND ri.transaction_hash = pe.transaction_hash
       AND ri.reserve_address  = pe.reserve_address
       AND ri.log_index        <  pe.log_index
    WHERE ri.liquidity_index > toUInt256OrZero('0')
),

-- aToken BalanceTransfer values are already in scaled units.
transfer_deltas AS (
    SELECT
        t.protocol,
        toDate(t.block_timestamp) AS date,
        lower(t.decoded_params['from']) AS user_address,
        rm.reserve_address AS reserve_address,
        -toInt256(toUInt256OrZero(t.decoded_params['value'])) AS scaled_delta
    FROM atoken_events_raw t
    INNER JOIN reserve_map rm
        ON rm.protocol       = t.protocol
       AND rm.atoken_address = lower(t.contract_address)
    WHERE t.event_name = 'BalanceTransfer'
      AND t.decoded_params['from'] != '0x0000000000000000000000000000000000000000'
      AND t.decoded_params['to']   != '0x0000000000000000000000000000000000000000'
      AND t.block_timestamp < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(t.block_timestamp) >= toDate('{{ start_month }}')
        AND toStartOfMonth(t.block_timestamp) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('t.block_timestamp', 'date', 'true') }}
      {% endif %}

    UNION ALL

    SELECT
        t.protocol,
        toDate(t.block_timestamp) AS date,
        lower(t.decoded_params['to']) AS user_address,
        rm.reserve_address AS reserve_address,
        toInt256(toUInt256OrZero(t.decoded_params['value'])) AS scaled_delta
    FROM atoken_events_raw t
    INNER JOIN reserve_map rm
        ON rm.protocol       = t.protocol
       AND rm.atoken_address = lower(t.contract_address)
    WHERE t.event_name = 'BalanceTransfer'
      AND t.decoded_params['from'] != '0x0000000000000000000000000000000000000000'
      AND t.decoded_params['to']   != '0x0000000000000000000000000000000000000000'
      AND t.block_timestamp < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(t.block_timestamp) >= toDate('{{ start_month }}')
        AND toStartOfMonth(t.block_timestamp) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('t.block_timestamp', 'date', 'true') }}
      {% endif %}
),

all_deltas AS (
    SELECT protocol, date, user_address, reserve_address, scaled_delta
    FROM pool_deltas
    UNION ALL
    SELECT protocol, date, user_address, reserve_address, scaled_delta
    FROM transfer_deltas
),

agg AS (
    SELECT
        date,
        protocol,
        user_address,
        reserve_address,
        sum(scaled_delta) AS diff_scaled
    FROM all_deltas
    GROUP BY date, protocol, user_address, reserve_address
)

SELECT
    date,
    protocol,
    user_address,
    reserve_address,
    diff_scaled
FROM agg
WHERE diff_scaled != toInt256(0)
