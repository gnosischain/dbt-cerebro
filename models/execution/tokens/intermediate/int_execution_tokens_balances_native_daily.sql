{#
  Windowed batches (start_month) normally append -- the batch runner does
  non-overlapping months. Pass reprocess_overwrite=true to re-run an
  OVERLAPPING window safely: delete+insert atomically replaces the touched
  month-partitions (no duplicate rows, no OPTIMIZE/FINAL). Paired with the
  start_month-aware seed below (reseed from the last day BEFORE the window),
  this makes a corrupted month self-healing:
    dbt run -s int_execution_tokens_balances_native_daily \
      --vars 'start_month: 2026-07-01, end_month: 2026-07-01, reprocess_overwrite: true'
#}
{{
  config(
    materialized='incremental',
    incremental_strategy=('delete+insert' if var('reprocess_overwrite', false) else ('append' if var('start_month', none) else 'delete+insert')),
    engine='ReplacingMergeTree()',
    order_by='(date, token_address, address)',
    partition_by='toStartOfMonth(date)',
    unique_key='(date, token_address, address)',
    settings={ 'allow_nullable_key': 1 },
    pre_hook=[
      "SET max_memory_usage = 8000000000",
      "SET max_bytes_before_external_group_by = 2000000000",
      "SET max_bytes_before_external_sort = 2000000000",
      "SET join_algorithm = 'grace_hash'"
    ],
    post_hook=[
      "SET max_memory_usage = 0",
      "SET max_bytes_before_external_group_by = 0",
      "SET max_bytes_before_external_sort = 0",
      "SET join_algorithm = 'default'"
    ],
    tags=['production','execution','tokens','balances_daily']
  )
}}

-- depends_on: {{ ref('int_execution_tokens_address_diffs_daily') }}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}
{% set symbol = var('symbol', none) %}
{% set symbol_exclude = var('symbol_exclude', none) %}

{% set symbol_sql %}
  {{ symbol_filter('symbol', symbol, 'include') }}
  {{ symbol_filter('symbol', symbol_exclude, 'exclude') }}
{% endset %}

WITH deltas AS (
    SELECT
        date,
        token_address,
        symbol,
        token_class,
        address,
        net_delta_raw
    FROM {{ ref('int_execution_tokens_address_diffs_daily') }}
    WHERE date < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(date) >= toDate('{{ start_month }}')
        AND toStartOfMonth(date) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('date', 'date', 'true', lookback_days=2, filters_sql=symbol_sql) }}
      {% endif %}
      {{ symbol_filter('symbol', symbol, 'include') }}
      {{ symbol_filter('symbol', symbol_exclude, 'exclude') }}

),

overall_max_date AS (
    SELECT
        least(
            {% if end_month %}
                toLastDayOfMonth(toDate('{{ end_month }}')),
            {% else %}
                today(),
            {% endif %}
            yesterday(),
            (
                SELECT max(toDate(date))
                FROM {{ ref('int_execution_tokens_address_diffs_daily') }}
                {% if end_month %}
                WHERE toStartOfMonth(date) <= toDate('{{ end_month }}')
                {% endif %}
            )
        ) AS max_date
),

{% if start_month and end_month %}
-- Reprocess/backfill a bounded window: seed the carry-forward from the last
-- GOOD day BEFORE the window (date < start_month), NOT from the current
-- max(date) -- which may itself sit inside the window being repaired. This is
-- what lets an overlapping re-run (reprocess_overwrite=true) rebuild the window
-- from a correct base instead of a poisoned tail.
prev_balances AS (
    -- Dedup the seed day with a partition-pruned GROUP BY any() instead of a
    -- full-table FINAL. FINAL over this 395M-row table forces a whole-table
    -- merge-sort (MergeSortingTransform) and OOMs; the seed day is a single,
    -- already-clean past partition, so GROUP BY on that one day is correct and cheap.
    SELECT
        token_address,
        symbol,
        token_class,
        address,
        any(balance_raw) AS balance_raw
    FROM {{ this }}
    WHERE date = (
        SELECT max(date)
        FROM {{ this }}
        WHERE date < toDate('{{ start_month }}')
          {{ symbol_filter('symbol', symbol, 'include') }}
          {{ symbol_filter('symbol', symbol_exclude, 'exclude') }}
    )
    {{ symbol_filter('symbol', symbol, 'include') }}
    {{ symbol_filter('symbol', symbol_exclude, 'exclude') }}
    GROUP BY token_address, symbol, token_class, address
),

{% elif is_incremental() %}
current_partition AS (
    SELECT
        max(toStartOfMonth(date)) AS month
        ,max(date)  AS max_date
    FROM {{ this }}
    WHERE date < yesterday()
      {{ symbol_filter('symbol', symbol, 'include') }}
      {{ symbol_filter('symbol', symbol_exclude, 'exclude') }}
),
prev_balances AS (
    SELECT
        t1.token_address,
        t1.symbol,
        t1.token_class,
        t1.address,
        t1.balance_raw
    FROM {{ this }} t1
    CROSS JOIN current_partition t2
    WHERE
        t1.date = t2.max_date
        {{ symbol_filter('t1.symbol', symbol, 'include') }}
        {{ symbol_filter('t1.symbol', symbol_exclude, 'exclude') }}
),
{% endif %}

{% if is_incremental() %}
keys AS (
    SELECT DISTINCT
        token_address,
        symbol,
        token_class,
        address
    FROM (
        SELECT
            token_address,
            symbol,
            token_class,
            address
        FROM prev_balances

        UNION ALL

        SELECT
            token_address,
            symbol,
            token_class,
            address
        FROM deltas
    )
),

calendar AS (
    SELECT
        k.token_address,
        k.symbol,
        k.token_class,
        k.address,
        {% if start_month and end_month %}
        addDays(
            (SELECT max(date) FROM {{ this }} WHERE date < toDate('{{ start_month }}')),
            offset + 1
        ) AS date
        {% else %}
        addDays(cp.max_date + 1, offset) AS date
        {% endif %}
    FROM keys k
    {% if not (start_month and end_month) %}
    CROSS JOIN current_partition cp
    {% endif %}
    CROSS JOIN overall_max_date o
    ARRAY JOIN range(
        {% if start_month and end_month %}
        toUInt32(dateDiff('day',
            (SELECT max(date) FROM {{ this }} WHERE date < toDate('{{ start_month }}')),
            o.max_date
        ))
        {% else %}
        dateDiff('day', cp.max_date, o.max_date)
        {% endif %}
    ) AS offset
),

{% else %}

calendar AS (
    SELECT
        token_address,
        symbol,
        token_class,
        address,
        addDays(min_date, offset) AS date
    FROM
    (
        SELECT
            d.token_address,
            d.symbol,
            d.token_class,
            d.address,
            min(d.date) AS min_date,
            dateDiff('day', min(d.date), any(o.max_date)) AS num_days
        FROM deltas d
        CROSS JOIN overall_max_date o
        GROUP BY
            d.token_address,
            d.symbol,
            d.token_class,
            d.address
    )
    ARRAY JOIN range(num_days + 1) AS offset
),


{% endif %}


balances AS (
    SELECT
        c.date AS date,
        c.token_address AS token_address,
        c.symbol AS symbol,
        c.token_class AS token_class,
        c.address AS address,

        sum(COALESCE(d.net_delta_raw,toInt256(0))) OVER (
            PARTITION BY c.token_address, c.address
            ORDER BY c.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
        {% if is_incremental() %}
            + coalesce(p.balance_raw, toInt256(0))
        {% endif %}
        AS balance_raw
    FROM calendar c
    LEFT JOIN deltas d
      ON d.token_address = c.token_address
     AND d.address       = c.address
     AND d.date          = c.date
    {% if is_incremental() %}
    LEFT JOIN prev_balances p
      ON p.token_address = c.token_address
     AND p.address       = c.address
    {% endif %}
),

final AS (
    SELECT
        b.date AS date,
        b.token_address AS token_address,
        b.symbol AS symbol,
        b.token_class AS token_class,
        b.address AS address,
        b.balance_raw AS balance_raw,
        b.balance_raw/POWER(10, t.decimals) AS balance
    FROM balances b
    INNER JOIN {{ ref('tokens_whitelist') }} t
      ON lower(t.address) = b.token_address
     AND b.date >= toDate(t.date_start)
     AND (t.date_end IS NULL OR b.date < toDate(t.date_end))
    -- Sparse-table tombstone rule: zero-balance rows are normally dropped to
    -- keep the table small, but on INCREMENTAL runs a key whose corrected
    -- balance is 0 must still emit its zero row — delete+insert derives the
    -- delete-set from the new rows, so "no row" can never overwrite a stale
    -- one. Spend-to-zero addresses otherwise keep their previous (possibly
    -- negative) balance forever (see docs/lessons/sparse-zero-row-stale-survival.md).
    -- Scoped to keys with activity in the window, so the bloat is bounded to
    -- the day's spent-to-zero addresses; they drop out on later days.
    WHERE b.balance_raw != 0
    {% if is_incremental() %}
       OR (b.token_address, b.address) IN (
            SELECT DISTINCT token_address, address FROM deltas
          )
    {% endif %}
)

SELECT
    date,
    token_address,
    symbol,
    token_class,
    address,
    balance_raw,
    balance
FROM final
