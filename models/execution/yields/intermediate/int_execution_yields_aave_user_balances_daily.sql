{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date, reserve_address, user_address)',
        unique_key='(date, reserve_address, user_address)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['dev','execution','yields','aave','user_balances'],
        incremental_predicates=(
            [
                "toStartOfMonth(date) >= toDate('" ~ var('start_month') ~ "')",
                "toStartOfMonth(date) <= toDate('" ~ var('end_month') ~ "')"
            ]
            if var('start_month', none) and var('end_month', none)
            else []
        )
    )
}}

-- depends_on: {{ ref('int_execution_yields_aave_diffs_daily') }}

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
    FROM {{ ref('int_execution_yields_aave_diffs_daily') }} d
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

keys AS (
    SELECT DISTINCT
        user_address,
        reserve_address
    FROM (
        SELECT user_address, reserve_address
        FROM prev_balances
        UNION ALL
        SELECT user_address, reserve_address
        FROM deltas
    )
),

calendar AS (
    SELECT
        k.user_address AS user_address,
        k.reserve_address AS reserve_address,
        addDays(cp.max_date + 1, offset) AS date
    FROM keys k
    CROSS JOIN current_partition cp
    CROSS JOIN overall_max_date o
    ARRAY JOIN range(
        dateDiff('day', cp.max_date, o.max_date)
    ) AS offset
),

{% else %}

calendar AS (
    SELECT
        user_address,
        reserve_address,
        addDays(min_date, offset) AS date
    FROM (
        SELECT
            d.user_address AS user_address,
            d.reserve_address AS reserve_address,
            min(d.date) AS min_date,
            dateDiff('day', min(d.date), any(o.max_date)) AS num_days
        FROM deltas d
        CROSS JOIN overall_max_date o
        GROUP BY d.user_address, d.reserve_address
    )
    ARRAY JOIN range(toUInt64(num_days + 1)) AS offset
),

{% endif %}

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
    LEFT JOIN deltas d
        ON d.user_address = c.user_address
       AND d.reserve_address = c.reserve_address
       AND d.date = c.date
    {% if is_incremental() %}
    LEFT JOIN prev_balances p
        ON p.user_address = c.user_address
       AND p.reserve_address = c.reserve_address
    {% endif %}
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
