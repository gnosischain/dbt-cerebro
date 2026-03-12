{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date, reserve_address, user_address)',
        unique_key='(date, reserve_address, user_address)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['dev','execution','yields','aave','user_balances']
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
        toStartOfDay(block_timestamp) AS date,
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
        toStartOfDay(block_timestamp) AS date,
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
        toStartOfDay(block_timestamp) AS date,
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
        toStartOfDay(block_timestamp) AS date,
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

pool_scaled_deltas AS (
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

transfer_scaled_deltas AS (
    SELECT
        toStartOfDay(block_timestamp) AS date,
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
        toStartOfDay(block_timestamp) AS date,
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

all_scaled_deltas AS (
    SELECT date, user_address, reserve_address, scaled_delta
    FROM pool_scaled_deltas
    UNION ALL
    SELECT date, user_address, reserve_address, scaled_delta
    FROM transfer_scaled_deltas
),

daily_scaled_diff AS (
    SELECT
        date,
        user_address,
        reserve_address,
        sum(scaled_delta) AS diff_scaled
    FROM all_scaled_deltas
    GROUP BY date, user_address, reserve_address
    HAVING diff_scaled != 0
),

daily_index AS (
    SELECT
        toStartOfDay(block_timestamp) AS date,
        lower(decoded_params['reserve']) AS reserve_address,
        argMax(
            toFloat64(toUInt256OrNull(decoded_params['liquidityIndex'])),
            block_timestamp
        ) AS liquidity_index_eod
    FROM {{ ref('contracts_aaveV3_PoolInstance_events') }}
    WHERE event_name = 'ReserveDataUpdated'
      AND decoded_params['liquidityIndex'] IS NOT NULL
      AND block_timestamp < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(block_timestamp) >= toDate('{{ start_month }}')
        AND toStartOfMonth(block_timestamp) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('block_timestamp', 'date', 'true') }}
      {% endif %}
    GROUP BY date, reserve_address
),

overall_max_date AS (
    SELECT
        least(
            {% if end_month %}
                toLastDayOfMonth(toDate('{{ end_month }}')),
            {% else %}
                today(),
            {% endif %}
            yesterday()
        ) AS max_date
),

{% if is_incremental() %}
prev_balances AS (
    SELECT
        user_address,
        reserve_address,
        scaled_balance
    FROM {{ this }}
    WHERE date = (SELECT max(date) FROM {{ this }})
),
{% endif %}

calendar AS (
    SELECT
        user_address AS user_address,
        reserve_address AS reserve_address,
        addDays(min_date, offset) AS date
    FROM (
        SELECT
            user_address,
            reserve_address,
            min(date) AS min_date,
            dateDiff('day', min(date), (SELECT max_date FROM overall_max_date)) AS num_days
        FROM (
            SELECT user_address, reserve_address, date
            FROM daily_scaled_diff
            {% if is_incremental() %}
            UNION ALL
            SELECT user_address, reserve_address,
                   (SELECT max(date) + 1 FROM {{ this }}) AS date
            FROM prev_balances
            {% endif %}
        )
        GROUP BY user_address, reserve_address
    )
    ARRAY JOIN range(toUInt64(num_days + 1)) AS offset
),

daily_balances AS (
    SELECT
        c.date AS date,
        c.user_address AS user_address,
        c.reserve_address AS reserve_address,
        sum(coalesce(d.diff_scaled, 0)) OVER (
            PARTITION BY c.user_address, c.reserve_address
            ORDER BY c.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
        {% if is_incremental() %}
            + coalesce(p.scaled_balance, 0)
        {% endif %}
        AS scaled_balance
    FROM calendar c
    LEFT JOIN daily_scaled_diff d
        ON d.user_address = c.user_address
       AND d.reserve_address = c.reserve_address
       AND d.date = c.date
    {% if is_incremental() %}
    LEFT JOIN prev_balances p
        ON p.user_address = c.user_address
       AND p.reserve_address = c.reserve_address
    {% endif %}
),

balances_with_underlying AS (
    SELECT
        b.date AS date,
        b.user_address AS user_address,
        b.reserve_address AS reserve_address,
        rm.reserve_symbol AS symbol,
        rm.decimals AS decimals,
        b.scaled_balance AS scaled_balance,
        CASE
            WHEN b.scaled_balance <= 0 THEN 0
            ELSE (b.scaled_balance * i.liquidity_index_eod) / 1e27
        END AS balance_raw
    FROM daily_balances b
    INNER JOIN reserve_map rm
        ON rm.reserve_address = b.reserve_address
    LEFT JOIN daily_index i
        ON i.date = b.date
       AND i.reserve_address = b.reserve_address
    WHERE b.scaled_balance != 0
)

SELECT
    b.date AS date,
    b.reserve_address AS reserve_address,
    b.symbol AS symbol,
    b.user_address AS user_address,
    b.scaled_balance AS scaled_balance,
    b.balance_raw AS balance_raw,
    b.balance_raw / power(10, b.decimals) AS balance,
    (b.balance_raw / power(10, b.decimals)) * coalesce(p.price, 0) AS balance_usd
FROM balances_with_underlying b
LEFT JOIN {{ ref('int_execution_token_prices_daily') }} p
    ON p.date = b.date
   AND upper(p.symbol) = upper(b.symbol)
