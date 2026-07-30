{#
  Incremental carry-forward for per-user aToken scaled balances.

  CRITICAL — seed must be a CONSTANT add, never a synthetic delta in the event
  stream. The previous densify+UNION ALL seed pattern double-counted whenever
  append left unmerged ReplacingMergeTree duplicates on the seed day
  (exact 2x spikes on 2026-06-19 and 2026-06-21 across all reserves).

  Pattern matches int_execution_tokens_balances_native_daily:
    calendar(days after watermark) LEFT JOIN diffs
    scaled = sum(diffs) OVER (calendar) + coalesce(prev_scaled, 0)

  Windowed batches (start_month) normally append — the batch runner does
  non-overlapping months. Pass reprocess_overwrite=true to re-run an
  OVERLAPPING window safely (delete+insert), seeding from the last good day
  BEFORE the window:
    dbt run -s int_execution_lending_aave_user_balances_daily \
      --vars 'start_month: 2026-06-01, end_month: 2026-06-01, reprocess_overwrite: true'
#}
{{
    config(
        materialized='incremental',
        incremental_strategy=('delete+insert' if var('reprocess_overwrite', false) else ('append' if (var('start_month', none) or var('incremental_end_date', none)) else 'delete+insert')),
        on_schema_change='sync_all_columns',
        engine='ReplacingMergeTree()',
        order_by='(date, protocol, reserve_address, user_address)',
        unique_key='(date, protocol, reserve_address, user_address)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['production','execution','lending','aave','spark','user_balances','refill_append'],
        pre_hook=["SET join_use_nulls = 0"],
        post_hook=["SET join_use_nulls = 0"]
    )
}}
-- depends_on: {{ ref('int_execution_lending_aave_diffs_daily') }}
-- NOTE: scaled_balance and balance_raw are UInt256/Int256 for exact aToken math
-- (mirrors Aave's on-chain WadRayMath). Run with --full-refresh when migrating from
-- the previous Float64 schema so the column types are recreated.

{% set start_month     = var('start_month', none) %}
{% set end_month       = var('end_month', none) %}
{% set reserve_symbol  = var('reserve_symbol', none) %}
{% set incr_end        = mb_var('incremental_end_date', none) %}

WITH

reserve_map AS (
    SELECT
        protocol,
        lower(reserve_address) AS reserve_address,
        reserve_symbol,
        decimals
    FROM {{ ref('lending_market_mapping') }}
    WHERE 1=1
      {{ symbol_filter('reserve_symbol', reserve_symbol, 'include') }}
),

deltas AS (
    SELECT
        d.date            AS date,
        d.protocol        AS protocol,
        d.user_address    AS user_address,
        d.reserve_address AS reserve_address,
        d.diff_scaled     AS diff_scaled
    FROM {{ ref('int_execution_lending_aave_diffs_daily') }} d
    INNER JOIN reserve_map rm
      ON  rm.protocol        = d.protocol
     AND  rm.reserve_address = d.reserve_address
    WHERE d.date < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(d.date) >= toDate('{{ start_month }}')
        AND toStartOfMonth(d.date) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('d.date', 'date', 'true') }}
      {% endif %}
),

pool_events_raw AS (
    SELECT 'Aave V3'   AS protocol, * FROM {{ ref('contracts_aaveV3_PoolInstance_events') }}
    UNION ALL
    SELECT 'SparkLend' AS protocol, * FROM {{ ref('contracts_spark_Pool_events') }}
),

daily_index AS (
    SELECT
        protocol,
        toDate(block_timestamp) AS date,
        lower(decoded_params['reserve']) AS reserve_address,
        argMax(
            toUInt256OrZero(decoded_params['liquidityIndex']),
            (block_timestamp, log_index)
        ) AS liquidity_index_eod
    FROM pool_events_raw
    WHERE event_name = 'ReserveDataUpdated'
      AND decoded_params['liquidityIndex'] IS NOT NULL
      AND (protocol, lower(decoded_params['reserve'])) IN (
            SELECT protocol, reserve_address FROM reserve_map
      )
      AND block_timestamp < today()
      {% if end_month %}
        AND toDate(block_timestamp) <= toLastDayOfMonth(toDate('{{ end_month }}'))
      {% elif incr_end is not none %}
        AND toDate(block_timestamp) <= toDate('{{ incr_end }}')
      {% endif %}
    GROUP BY protocol, date, reserve_address
),

overall_max_date AS (
    SELECT
        least(
            {% if end_month %}
                toLastDayOfMonth(toDate('{{ end_month }}')),
            {% elif incr_end is not none %}
                toDate('{{ incr_end }}'),
            {% else %}
                yesterday(),
            {% endif %}
            yesterday()
        ) AS max_date
),

{% if start_month and end_month %}
-- Reprocess/backfill a bounded window: seed from the last GOOD day BEFORE the
-- window (not from max(date), which may sit inside a poisoned tail).
prev_balances AS (
    SELECT
        protocol,
        user_address,
        reserve_address,
        any(scaled_balance) AS scaled_balance
    FROM {{ this }}
    WHERE date = (
        SELECT max(date)
        FROM {{ this }}
        WHERE date < toDate('{{ start_month }}')
          AND (protocol, reserve_address) IN (SELECT protocol, reserve_address FROM reserve_map)
    )
      AND (protocol, reserve_address) IN (SELECT protocol, reserve_address FROM reserve_map)
    GROUP BY protocol, user_address, reserve_address
),

seed_date AS (
    SELECT max(date) AS max_date
    FROM {{ this }}
    WHERE date < toDate('{{ start_month }}')
      AND (protocol, reserve_address) IN (SELECT protocol, reserve_address FROM reserve_map)
),

{% elif is_incremental() %}
-- Append path (start_month / incremental_end_date): strict max(date) watermark
-- so the calendar only emits dates that are not already in the table.
-- Daily delete+insert path: lag one day (date < yesterday) so yesterday can be
-- recomputed when late diffs arrive.
current_partition AS (
    SELECT
        max(date) AS max_date
    FROM {{ this }}
    WHERE (protocol, reserve_address) IN (SELECT protocol, reserve_address FROM reserve_map)
      {% if incr_end is none %}
      AND date < yesterday()
      {% endif %}
),

prev_balances AS (
    -- Dedup the seed day with partition-pruned GROUP BY any() instead of FINAL.
    -- Unmerged ReplacingMergeTree parts on the seed day were what turned one
    -- carry-forward into two seeds and permanently doubled every holder's balance.
    SELECT
        protocol,
        user_address,
        reserve_address,
        any(scaled_balance) AS scaled_balance
    FROM {{ this }}
    WHERE date = (SELECT max_date FROM current_partition)
      AND (protocol, reserve_address) IN (SELECT protocol, reserve_address FROM reserve_map)
    GROUP BY protocol, user_address, reserve_address
),

seed_date AS (
    SELECT max_date FROM current_partition
),

{% endif %}

{% if is_incremental() %}
keys AS (
    SELECT DISTINCT
        protocol,
        user_address,
        reserve_address
    FROM (
        SELECT protocol, user_address, reserve_address FROM prev_balances
        UNION ALL
        SELECT protocol, user_address, reserve_address FROM deltas
    )
),

calendar AS (
    SELECT
        k.protocol        AS protocol,
        k.user_address    AS user_address,
        k.reserve_address AS reserve_address,
        addDays(sd.max_date, offset + 1) AS date
    FROM keys k
    CROSS JOIN seed_date sd
    CROSS JOIN overall_max_date omd
    ARRAY JOIN range(toUInt64(greatest(
        dateDiff('day', sd.max_date, omd.max_date),
        0
    ))) AS offset
),

{% else %}

calendar AS (
    SELECT
        protocol,
        user_address,
        reserve_address,
        addDays(min_date, offset) AS date
    FROM (
        SELECT
            d.protocol        AS protocol,
            d.user_address    AS user_address,
            d.reserve_address AS reserve_address,
            min(d.date)       AS min_date,
            dateDiff('day', min(d.date), any(omd.max_date)) AS num_days
        FROM deltas d
        CROSS JOIN overall_max_date omd
        GROUP BY d.protocol, d.user_address, d.reserve_address
    )
    ARRAY JOIN range(toUInt64(num_days + 1)) AS offset
),

{% endif %}

daily_balances AS (
    SELECT
        c.date            AS date,
        c.protocol        AS protocol,
        c.user_address    AS user_address,
        c.reserve_address AS reserve_address,
        sum(coalesce(d.diff_scaled, toInt256(0))) OVER (
            PARTITION BY c.protocol, c.user_address, c.reserve_address
            ORDER BY c.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
        {% if is_incremental() %}
            + coalesce(p.scaled_balance, toInt256(0))
        {% endif %}
        AS scaled_balance
    FROM calendar c
    LEFT JOIN deltas d
      ON  d.date            = c.date
     AND  d.protocol        = c.protocol
     AND  d.user_address    = c.user_address
     AND  d.reserve_address = c.reserve_address
    {% if is_incremental() %}
    LEFT JOIN prev_balances p
      ON  p.protocol        = c.protocol
     AND  p.user_address    = c.user_address
     AND  p.reserve_address = c.reserve_address
    {% endif %}
),

balances_with_index AS (
    SELECT
        b.date                AS date,
        b.protocol            AS protocol,
        b.user_address        AS user_address,
        b.reserve_address     AS reserve_address,
        b.scaled_balance      AS scaled_balance,
        i.liquidity_index_eod AS liquidity_index_eod
    FROM daily_balances b
    ASOF LEFT JOIN daily_index i
        ON  i.protocol        = b.protocol
        AND i.reserve_address = b.reserve_address
        AND b.date >= i.date
    -- Sparse-table rule: drop zero balances, but on incremental runs still emit
    -- a zero row for keys that had activity in the window so delete+insert can
    -- overwrite a stale positive balance after a full withdraw.
    WHERE b.scaled_balance != toInt256(0)
    {% if is_incremental() %}
       OR (b.protocol, b.user_address, b.reserve_address) IN (
            SELECT DISTINCT protocol, user_address, reserve_address FROM deltas
          )
    {% endif %}
),

balances_with_underlying AS (
    SELECT
        bi.date            AS date,
        bi.protocol        AS protocol,
        bi.user_address    AS user_address,
        bi.reserve_address AS reserve_address,
        rm.reserve_symbol  AS symbol,
        rm.decimals        AS decimals,
        bi.scaled_balance  AS scaled_balance,
        CASE
            WHEN bi.scaled_balance <= toInt256(0) THEN toUInt256OrZero('0')
            ELSE intDiv(
                toUInt256(bi.scaled_balance) * bi.liquidity_index_eod,
                toUInt256OrZero('1000000000000000000000000000')
            )
        END AS balance_raw
    FROM balances_with_index bi
    INNER JOIN reserve_map rm
        ON  rm.protocol        = bi.protocol
        AND rm.reserve_address = bi.reserve_address
)

SELECT
    b.date            AS date,
    b.protocol        AS protocol,
    b.reserve_address AS reserve_address,
    b.symbol          AS symbol,
    b.user_address    AS user_address,
    b.scaled_balance  AS scaled_balance,
    b.balance_raw     AS balance_raw,
    toFloat64(b.balance_raw) / power(10, b.decimals) AS balance,
    (toFloat64(b.balance_raw) / power(10, b.decimals)) * coalesce(p.price, 0) AS balance_usd
FROM balances_with_underlying b
LEFT JOIN {{ ref('int_execution_token_prices_daily') }} p
    ON p.date = b.date
   AND p.symbol = b.symbol
WHERE b.date < today()
  {% if incr_end is not none %}
  AND b.date <= toDate('{{ incr_end }}')
  {% endif %}
