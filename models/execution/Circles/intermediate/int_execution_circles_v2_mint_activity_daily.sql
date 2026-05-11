{{
  config(
    materialized='incremental',
    incremental_strategy=('append' if var('start_month', none) else 'delete+insert'),
    engine='ReplacingMergeTree()',
    order_by='(date, avatar)',
    unique_key='(avatar, date)',
    partition_by='toStartOfMonth(date)',
    settings={'allow_nullable_key': 1},
    tags=['production','execution','circles_v2','mint_activity_daily'],
    pre_hook=["SET join_algorithm = 'grace_hash'"],
    post_hook=["SET join_algorithm = 'default'"]
  )
}}

-- Densified per-(avatar, date) personal-mint activity, with 14-day rolling
-- window aggregates used by the Active Minters KPIs and cohort distribution.
--
-- Replaces Dune's gnosis_circlesv2_mint_activity_v (query_6317871).
--
-- Source: hub_transfers where from_address = 0x00..00 and to_address is the
-- avatar (matches int_execution_circles_v2_hub_transfers semantics; see
-- api_execution_circles_v2_avatar_mint_activity_daily for the daily-only
-- variant).
--
-- Window semantics (matches Dune):
--   mint_14dw      = SUM(mint_amount) over 14 trailing rows (incl. current)
--   mint_days_14dw = COUNT(*)        over 14 trailing rows (incl. current)
--
-- Incremental strategy:
--   * 30-day lookback on the event stream so trailing windows for the
--     emitted 14-day output region are computed from complete history
--     (window needs date - 13; we pull date - 27 to be safe).
--   * affected_avatars restricts the densification cross-join to avatars
--     that minted in the lookback window, avoiding a full-history rebuild.
--   * delete+insert keyed on (avatar, date) so other avatars' history is
--     untouched.
--   * var('start_month'/'end_month') give the manual-backfill path that
--     bypasses the lookback (matches int_execution_gnosis_app_user_activity_daily).

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

WITH
{% if start_month and end_month %}
mint_events AS (
    SELECT
        to_address                                   AS avatar,
        toDate(block_timestamp)                      AS date,
        sum(toFloat64(amount_raw)) / pow(10, 18)     AS mint_amount
    FROM {{ ref('int_execution_circles_v2_hub_transfers') }}
    WHERE from_address = '0x0000000000000000000000000000000000000000'
      AND to_address  != '0x0000000000000000000000000000000000000000'
      AND toStartOfMonth(block_timestamp) >= toDate('{{ start_month }}')
      AND toStartOfMonth(block_timestamp) <= toDate('{{ end_month }}')
    GROUP BY to_address, toDate(block_timestamp)
),
{% else %}
mint_events AS (
    SELECT
        to_address                                   AS avatar,
        toDate(block_timestamp)                      AS date,
        sum(toFloat64(amount_raw)) / pow(10, 18)     AS mint_amount
    FROM {{ ref('int_execution_circles_v2_hub_transfers') }}
    WHERE from_address = '0x0000000000000000000000000000000000000000'
      AND to_address  != '0x0000000000000000000000000000000000000000'
      {{ apply_monthly_incremental_filter(
            source_field='block_timestamp',
            destination_field='date',
            add_and=True,
            lookback_days=27,
            lookback_res='day') }}
    GROUP BY to_address, toDate(block_timestamp)
),
{% endif %}

affected_avatars AS (
    SELECT DISTINCT avatar
    FROM mint_events
),

-- Per-avatar earliest historical mint, pre-aggregated to avoid the
-- correlated-subquery pattern (unsupported on ClickHouse without
-- allow_experimental_correlated_subqueries). Source-of-truth for
-- first-time / full-refresh runs.
mint_events_min_per_avatar AS (
    SELECT
        to_address                                  AS avatar,
        min(toDate(block_timestamp))                AS min_date
    FROM {{ ref('int_execution_circles_v2_hub_transfers') }}
    WHERE from_address = '0x0000000000000000000000000000000000000000'
      AND to_address  != '0x0000000000000000000000000000000000000000'
      AND to_address IN (SELECT avatar FROM affected_avatars)
    GROUP BY to_address
),

{% if is_incremental() and not (start_month and end_month) %}
-- On incremental runs the persisted table already covers each avatar's
-- pre-lookback history; use its earliest stored date as the lower bound
-- so we never re-densify dates that already exist in the table.
existing_min_per_avatar AS (
    SELECT
        avatar,
        min(toDate(date))                           AS min_date
    FROM {{ this }}
    WHERE avatar IN (SELECT avatar FROM affected_avatars)
    GROUP BY avatar
),
{% endif %}

avatar_first_mint AS (
    -- Earliest mint per affected avatar, used as the calendar lower bound
    -- so we never emit pre-history rows.
    SELECT
        a.avatar,
        {% if is_incremental() and not (start_month and end_month) %}
        coalesce(e.min_date, m.min_date)            AS min_date
        {% else %}
        m.min_date                                  AS min_date
        {% endif %}
    FROM affected_avatars a
    LEFT JOIN mint_events_min_per_avatar m ON m.avatar = a.avatar
    {% if is_incremental() and not (start_month and end_month) %}
    LEFT JOIN existing_min_per_avatar  e ON e.avatar = a.avatar
    {% endif %}
),

calendar_bounds AS (
    SELECT
        avatar,
        {% if start_month and end_month %}
            greatest(min_date, toDate('{{ start_month }}')) AS lo,
            least(today(), toLastDayOfMonth(toDate('{{ end_month }}'))) AS hi
        {% else %}
            -- Incremental: only emit rows from the last 14 days the rolling
            -- window can fully cover. Pull 13 extra preceding rows so the
            -- window function sees the complete prefix.
            greatest(min_date, addDays(today(), -27)) AS lo,
            today() AS hi
        {% endif %}
    FROM avatar_first_mint
),

calendar AS (
    SELECT
        b.avatar,
        addDays(b.lo, n) AS date
    FROM calendar_bounds b
    ARRAY JOIN range(toUInt32(dateDiff('day', b.lo, b.hi) + 1)) AS n
),

dense AS (
    SELECT
        c.avatar,
        c.date,
        coalesce(m.mint_amount, 0) AS mint_amount
    FROM calendar c
    LEFT JOIN mint_events m
        ON m.avatar = c.avatar
       AND m.date   = c.date
),

windowed AS (
    SELECT
        avatar,
        date,
        mint_amount,
        sum(mint_amount) OVER (
            PARTITION BY avatar
            ORDER BY date
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS mint_14dw,
        count() OVER (
            PARTITION BY avatar
            ORDER BY date
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS mint_days_14dw
    FROM dense
)

SELECT
    avatar,
    date,
    mint_amount,
    mint_14dw,
    toUInt8(mint_days_14dw) AS mint_days_14dw
FROM windowed
{% if is_incremental() and not (start_month and end_month) %}
-- Drop the 13-day prefix we pulled in only to seed the window; those
-- (avatar, date) rows already exist in the table and would be unchanged.
WHERE date >= addDays(today(), -14)
{% endif %}
