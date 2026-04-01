{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date, reserve_address, user_address)',
        unique_key='(date, reserve_address, user_address)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['production','execution','yields','aave','diffs']
    )
}}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}

WITH

reserve_map AS (
    SELECT
        lower(atoken_address)  AS atoken_address,
        lower(reserve_address) AS reserve_address,
        reserve_symbol,
        decimals
    FROM {{ ref('atoken_reserve_mapping') }}
),

reserve_index_by_tx AS (
    SELECT
        transaction_hash,
        lower(decoded_params['reserve']) AS reserve_address,
        any(toFloat64(toUInt256OrNull(decoded_params['liquidityIndex']))) AS liquidity_index
    FROM {{ ref('contracts_aaveV3_PoolInstance_events') }}
    WHERE event_name = 'ReserveDataUpdated'
      AND lower(decoded_params['reserve']) IN (SELECT reserve_address FROM reserve_map)
      AND block_timestamp < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(block_timestamp) >= toDate('{{ start_month }}')
        AND toStartOfMonth(block_timestamp) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('block_timestamp', 'date', 'true') }}
      {% endif %}
    GROUP BY transaction_hash, lower(decoded_params['reserve'])
),

pool_events AS (
    SELECT
        toDate(block_timestamp) AS date,
        transaction_hash,
        lower(decoded_params['reserve']) AS reserve_address,
        lower(decoded_params['onBehalfOf']) AS user_address,
        'Supply' AS action,
        toFloat64(toUInt256OrNull(decoded_params['amount'])) AS amount
    FROM {{ ref('contracts_aaveV3_PoolInstance_events') }}
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
        toDate(block_timestamp) AS date,
        transaction_hash,
        lower(decoded_params['reserve']) AS reserve_address,
        lower(decoded_params['user']) AS user_address,
        'Withdraw' AS action,
        toFloat64(toUInt256OrNull(decoded_params['amount'])) AS amount
    FROM {{ ref('contracts_aaveV3_PoolInstance_events') }}
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
        toDate(block_timestamp) AS date,
        transaction_hash,
        lower(decoded_params['reserve']) AS reserve_address,
        lower(decoded_params['repayer']) AS user_address,
        'RepayWithATokens' AS action,
        toFloat64(toUInt256OrNull(decoded_params['amount'])) AS amount
    FROM {{ ref('contracts_aaveV3_PoolInstance_events') }}
    WHERE event_name = 'Repay'
      AND decoded_params['useATokens'] = 'true'
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
        toDate(block_timestamp) AS date,
        transaction_hash,
        lower(decoded_params['collateralAsset']) AS reserve_address,
        lower(decoded_params['user']) AS user_address,
        'LiquidationWithdraw' AS action,
        toFloat64(toUInt256OrNull(decoded_params['liquidatedCollateralAmount'])) AS amount
    FROM {{ ref('contracts_aaveV3_PoolInstance_events') }}
    WHERE event_name = 'LiquidationCall'
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

pool_events_filtered AS (
    SELECT
        pe.date AS date,
        pe.transaction_hash AS transaction_hash,
        pe.reserve_address AS reserve_address,
        pe.user_address AS user_address,
        pe.action AS action,
        pe.amount AS amount
    FROM pool_events pe
    INNER JOIN reserve_map rm ON rm.reserve_address = pe.reserve_address
),

pool_deltas AS (
    SELECT
        pe.date AS date,
        pe.user_address AS user_address,
        pe.reserve_address AS reserve_address,
        CASE
            WHEN pe.action = 'Supply'
                THEN pe.amount * 1e27 / ri.liquidity_index
            ELSE
                -(pe.amount * 1e27 / ri.liquidity_index)
        END AS scaled_delta
    FROM pool_events_filtered pe
    INNER JOIN reserve_index_by_tx ri
        ON ri.transaction_hash = pe.transaction_hash
       AND ri.reserve_address = pe.reserve_address
    WHERE ri.liquidity_index IS NOT NULL
      AND ri.liquidity_index > 0
),

transfer_deltas AS (
    SELECT
        toDate(block_timestamp) AS date,
        lower(decoded_params['from']) AS user_address,
        rm.reserve_address AS reserve_address,
        -toFloat64(toUInt256OrNull(decoded_params['value'])) AS scaled_delta
    FROM {{ ref('contracts_aaveV3_AToken_events') }} t
    INNER JOIN reserve_map rm
        ON rm.atoken_address = lower(t.contract_address)
    WHERE t.event_name = 'BalanceTransfer'
      AND decoded_params['from'] != '0x0000000000000000000000000000000000000000'
      AND decoded_params['to']   != '0x0000000000000000000000000000000000000000'
      AND block_timestamp < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(block_timestamp) >= toDate('{{ start_month }}')
        AND toStartOfMonth(block_timestamp) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('block_timestamp', 'date', 'true') }}
      {% endif %}

    UNION ALL

    SELECT
        toDate(block_timestamp) AS date,
        lower(decoded_params['to']) AS user_address,
        rm.reserve_address AS reserve_address,
        toFloat64(toUInt256OrNull(decoded_params['value'])) AS scaled_delta
    FROM {{ ref('contracts_aaveV3_AToken_events') }} t
    INNER JOIN reserve_map rm
        ON rm.atoken_address = lower(t.contract_address)
    WHERE t.event_name = 'BalanceTransfer'
      AND decoded_params['from'] != '0x0000000000000000000000000000000000000000'
      AND decoded_params['to']   != '0x0000000000000000000000000000000000000000'
      AND block_timestamp < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(block_timestamp) >= toDate('{{ start_month }}')
        AND toStartOfMonth(block_timestamp) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('block_timestamp', 'date', 'true') }}
      {% endif %}
),

all_deltas AS (
    SELECT date, user_address, reserve_address, scaled_delta
    FROM pool_deltas
    UNION ALL
    SELECT date, user_address, reserve_address, scaled_delta
    FROM transfer_deltas
),

agg AS (
    SELECT
        date,
        user_address,
        reserve_address,
        sum(scaled_delta) AS diff_scaled
    FROM all_deltas
    GROUP BY date, user_address, reserve_address
)

SELECT
    date,
    user_address,
    reserve_address,
    diff_scaled
FROM agg
WHERE diff_scaled != 0
