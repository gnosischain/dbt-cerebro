{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

{{
  config(
    materialized='incremental',
    incremental_strategy=('append' if start_month else 'delete+insert'),
    engine='ReplacingMergeTree()',
    order_by='(date, address, activity_kind)',
    unique_key='(date, address, activity_kind)',
    partition_by='toStartOfMonth(date)',
    settings={'allow_nullable_key': 1},
    pre_hook=["SET join_algorithm = 'grace_hash'"],
    tags=['production','execution','gnosis_app','activity_daily']
  )
}}

{# Description in schema.yml — see int_execution_gnosis_app_user_activity_daily #}

{#
  activity_kind enum, one row per (date, address, activity_kind):
    onboard                  — first-ever heuristic hit per address (from
                               int_execution_gnosis_app_users_current).
                               One row per user; anchors cohort_month.
    circles_register_human,
    circles_invite_human,
    circles_trust,
    circles_profile_update,
    circles_metri_fee,
    safe_invitation_module   — raw heuristic events from
                               int_execution_gnosis_app_user_events.
    swap_signed              — PreSignature events from
                               int_execution_gnosis_app_swaps (all rows).
    swap_filled              — subset with was_filled = true.
    topup                    — GA→GP TopUps.
    marketplace_buy          — GA marketplace purchases.

  Same address may appear multiple times per day with different kinds.
  amount_usd is populated only for swap_filled, topup, marketplace_buy
  (latter TBD until CRC pricing is wired).
#}

WITH onboard_rows AS (
    -- One onboarding row per user on their first-seen date. Needed so the
    -- "new user" cohort_month can be computed without looking at the event
    -- log directly.
    SELECT
        toDate(first_seen_at) AS date,
        address               AS address,
        'onboard'             AS activity_kind,
        1                     AS n_events,
        CAST(NULL AS Nullable(Float64)) AS amount_usd
    FROM {{ ref('int_execution_gnosis_app_users_current') }}
    WHERE first_seen_at IS NOT NULL
    {% if start_month and end_month %}
      AND toStartOfMonth(first_seen_at) >= toDate('{{ start_month }}')
      AND toStartOfMonth(first_seen_at) <= toDate('{{ end_month }}')
    {% else %}
      {{ apply_monthly_incremental_filter('first_seen_at', 'date', add_and=True) }}
    {% endif %}
),

heuristic_rows AS (
    SELECT
        toDate(block_timestamp)  AS date,
        address                  AS address,
        heuristic_kind           AS activity_kind,
        count(*)                 AS n_events,
        CAST(NULL AS Nullable(Float64)) AS amount_usd
    FROM {{ ref('int_execution_gnosis_app_user_events') }}
    WHERE block_timestamp IS NOT NULL
    {% if start_month and end_month %}
      AND toStartOfMonth(block_timestamp) >= toDate('{{ start_month }}')
      AND toStartOfMonth(block_timestamp) <= toDate('{{ end_month }}')
    {% else %}
      {{ apply_monthly_incremental_filter('block_timestamp', 'date', add_and=True) }}
    {% endif %}
    GROUP BY toDate(block_timestamp), address, heuristic_kind
),

swap_signed_rows AS (
    SELECT
        toDate(block_timestamp)  AS date,
        taker                    AS address,
        'swap_signed'            AS activity_kind,
        count(*)                 AS n_events,
        CAST(NULL AS Nullable(Float64)) AS amount_usd
    FROM {{ ref('int_execution_gnosis_app_swaps') }}
    {% if start_month and end_month %}
    WHERE toStartOfMonth(block_timestamp) >= toDate('{{ start_month }}')
      AND toStartOfMonth(block_timestamp) <= toDate('{{ end_month }}')
    {% else %}
    WHERE 1=1 {{ apply_monthly_incremental_filter('block_timestamp', 'date', add_and=True) }}
    {% endif %}
    GROUP BY toDate(block_timestamp), taker
),

swap_filled_rows AS (
    -- first_fill_at is Nullable(DateTime) after the join_use_nulls=1 fix in
    -- int_execution_gnosis_app_swaps; cast to non-nullable so the date column
    -- stays non-nullable through the UNION ALL (downstream fct_ models order
    -- by date without allow_nullable_key).
    SELECT
        toDate(assumeNotNull(first_fill_at))     AS date,
        taker                                    AS address,
        'swap_filled'                            AS activity_kind,
        count(*)                                 AS n_events,
        sum(toFloat64OrNull(toString(amount_usd))) AS amount_usd
    FROM {{ ref('int_execution_gnosis_app_swaps') }}
    WHERE was_filled = 1
      AND first_fill_at IS NOT NULL
    {% if start_month and end_month %}
      AND toStartOfMonth(first_fill_at) >= toDate('{{ start_month }}')
      AND toStartOfMonth(first_fill_at) <= toDate('{{ end_month }}')
    {% else %}
      {{ apply_monthly_incremental_filter('first_fill_at', 'date', add_and=True) }}
    {% endif %}
    GROUP BY toDate(assumeNotNull(first_fill_at)), taker
),

topup_rows AS (
    SELECT
        toDate(block_timestamp)  AS date,
        ga_user                  AS address,
        'topup'                  AS activity_kind,
        count(*)                 AS n_events,
        sum(toFloat64OrNull(toString(amount_usd))) AS amount_usd
    FROM {{ ref('int_execution_gnosis_app_gpay_topups') }}
    {% if start_month and end_month %}
    WHERE toStartOfMonth(block_timestamp) >= toDate('{{ start_month }}')
      AND toStartOfMonth(block_timestamp) <= toDate('{{ end_month }}')
    {% else %}
    WHERE 1=1 {{ apply_monthly_incremental_filter('block_timestamp', 'date', add_and=True) }}
    {% endif %}
    GROUP BY toDate(block_timestamp), ga_user
),

marketplace_rows AS (
    SELECT
        toDate(block_timestamp)  AS date,
        payer                    AS address,
        'marketplace_buy'        AS activity_kind,
        count(*)                 AS n_events,
        CAST(NULL AS Nullable(Float64)) AS amount_usd
    FROM {{ ref('int_execution_gnosis_app_marketplace_payments') }}
    {% if start_month and end_month %}
    WHERE toStartOfMonth(block_timestamp) >= toDate('{{ start_month }}')
      AND toStartOfMonth(block_timestamp) <= toDate('{{ end_month }}')
    {% else %}
    WHERE 1=1 {{ apply_monthly_incremental_filter('block_timestamp', 'date', add_and=True) }}
    {% endif %}
    GROUP BY toDate(block_timestamp), payer
)

SELECT * FROM onboard_rows
UNION ALL SELECT * FROM heuristic_rows
UNION ALL SELECT * FROM swap_signed_rows
UNION ALL SELECT * FROM swap_filled_rows
UNION ALL SELECT * FROM topup_rows
UNION ALL SELECT * FROM marketplace_rows
