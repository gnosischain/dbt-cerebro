{{
    config(
        materialized='incremental',
        incremental_strategy=('append' if var('start_month', none) else 'delete+insert'),
        engine='ReplacingMergeTree()',
        order_by='(date, reserve_address, user_address)',
        unique_key='(date, reserve_address, user_address)',
        partition_by='toStartOfMonth(date)',
        pre_hook=["SET join_use_nulls = 0"],
        settings={'allow_nullable_key': 1},
        tags=['production','execution','lending','aave','user_balances']
    )
}}

-- depends_on: {{ ref('int_execution_lending_aave_diffs_daily') }}

{% set start_month     = var('start_month', none) %}
{% set end_month       = var('end_month', none) %}
{% set reserve_symbol  = var('reserve_symbol', none) %}

WITH

reserve_map AS (
    SELECT
        lower(reserve_address) AS reserve_address,
        reserve_symbol,
        decimals
    FROM {{ ref('atoken_reserve_mapping') }}
    WHERE 1=1
      {{ symbol_filter('reserve_symbol', reserve_symbol, 'include') }}
),

deltas AS (
    SELECT
        d.date AS date,
        d.user_address AS user_address,
        d.reserve_address AS reserve_address,
        d.diff_scaled AS diff_scaled
    FROM {{ ref('int_execution_lending_aave_diffs_daily') }} d
    INNER JOIN reserve_map rm ON rm.reserve_address = d.reserve_address
    WHERE d.date < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(d.date) >= toDate('{{ start_month }}')
        AND toStartOfMonth(d.date) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('d.date', 'date', 'true') }}
      {% endif %}
),

daily_index AS (
    SELECT
        toDate(block_timestamp) AS date,
        lower(decoded_params['reserve']) AS reserve_address,
        argMax(
            toFloat64(toUInt256OrNull(decoded_params['liquidityIndex'])),
            block_timestamp
        ) AS liquidity_index_eod
    FROM {{ ref('contracts_aaveV3_PoolInstance_events') }}
    WHERE event_name = 'ReserveDataUpdated'
      AND decoded_params['liquidityIndex'] IS NOT NULL
      AND lower(decoded_params['reserve']) IN (SELECT reserve_address FROM reserve_map)
      AND block_timestamp < today()
      {% if end_month %}
        AND toDate(block_timestamp) <= toLastDayOfMonth(toDate('{{ end_month }}'))
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
current_partition AS (
    SELECT
        max(date) AS max_date
    FROM {{ this }}
    WHERE date < yesterday()
      AND reserve_address IN (SELECT reserve_address FROM reserve_map)
),

prev_balances AS (
    SELECT
        t1.user_address AS user_address,
        t1.reserve_address AS reserve_address,
        t1.scaled_balance AS scaled_balance
    FROM {{ this }} t1
    CROSS JOIN current_partition t2
    WHERE t1.date = t2.max_date
      AND t1.reserve_address IN (SELECT reserve_address FROM reserve_map)
),

-- Seed previous balances as starting events, then append new deltas
seeded_events AS (
    SELECT
        addDays(cp.max_date, 1) AS date,
        p.user_address AS user_address,
        p.reserve_address AS reserve_address,
        p.scaled_balance AS diff_scaled
    FROM prev_balances p
    CROSS JOIN current_partition cp
    UNION ALL
    SELECT
        d.date AS date,
        d.user_address AS user_address,
        d.reserve_address AS reserve_address,
        d.diff_scaled AS diff_scaled
    FROM deltas d
),

{% endif %}

-- Compute cumulative balance at each event point only (sparse)
cumulative_at_events AS (
    SELECT
        date,
        user_address,
        reserve_address,
        sum(diff_scaled) OVER (
            PARTITION BY user_address, reserve_address
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS scaled_balance
    FROM {% if is_incremental() %}seeded_events{% else %}deltas{% endif %}
),

-- Determine how far to forward-fill each event
with_next_event AS (
    SELECT
        date,
        user_address,
        reserve_address,
        scaled_balance,
        leadInFrame(date, 1, toDate('2099-01-01')) OVER (
            PARTITION BY user_address, reserve_address
            ORDER BY date
            ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        ) AS next_event_date
    FROM cumulative_at_events
),

-- Expand sparse events to daily rows via ARRAY JOIN
daily_balances AS (
    SELECT
        addDays(w.date, number) AS date,
        w.user_address AS user_address,
        w.reserve_address AS reserve_address,
        w.scaled_balance AS scaled_balance
    FROM with_next_event w
    CROSS JOIN overall_max_date omd
    ARRAY JOIN range(toUInt64(greatest(
        dateDiff('day', w.date, least(w.next_event_date, addDays(omd.max_date, 1))),
        0
    ))) AS number
),

balances_with_index AS (
    SELECT
        b.date AS date,
        b.user_address AS user_address,
        b.reserve_address AS reserve_address,
        b.scaled_balance AS scaled_balance,
        i.liquidity_index_eod AS liquidity_index_eod
    FROM daily_balances b
    ASOF LEFT JOIN daily_index i
        ON i.reserve_address = b.reserve_address
        AND b.date >= i.date
    WHERE b.scaled_balance != 0
),

balances_with_underlying AS (
    SELECT
        bi.date AS date,
        bi.user_address AS user_address,
        bi.reserve_address AS reserve_address,
        rm.reserve_symbol AS symbol,
        rm.decimals AS decimals,
        bi.scaled_balance AS scaled_balance,
        CASE
            WHEN bi.scaled_balance <= 0 THEN 0
            ELSE (bi.scaled_balance * bi.liquidity_index_eod) / 1e27
        END AS balance_raw
    FROM balances_with_index bi
    INNER JOIN reserve_map rm
        ON rm.reserve_address = bi.reserve_address
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
   AND p.symbol = b.symbol
