{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

{{
  config(
    materialized='incremental',
    incremental_strategy=('append' if start_month else 'delete+insert'),
    engine='ReplacingMergeTree()',
    order_by='(project_id, event_name, event_time, insert_id)',
    partition_by='toStartOfMonth(event_date)',
    unique_key='(project_id, event_name, event_time, insert_id)',
    tags=['production', 'staging', 'mixpanel_ga']
  )
}}

-- raw CTE extracts all JSON properties and computes boolean flags.
-- event_category is computed in the outer SELECT so it can reference
-- is_autocapture (a derived column) without a self-join.
WITH raw AS (
    SELECT
        -- ── time ──────────────────────────────────────────────────────────
        event_time,
        toDate(event_time)          AS event_date,
        toHour(event_time)          AS hour_of_day,
        toDayOfWeek(event_time)     AS day_of_week,

        -- ── event identity ───────────────────────────────────────────────
        event_name,
        project_id,
        insert_id,

        -- ── privacy-safe user / device ───────────────────────────────────
        -- Keyed pseudonym (salted) so the hash cannot be reversed via
        -- rainbow tables against the public on-chain address space.
        -- See macros/pseudonymize_address.sql — the same macro must be
        -- used on on-chain addresses for any cross-domain join.
        {{ pseudonymize_address('distinct_id') }}                                AS user_id_hash,
        {{ pseudonymize_address("JSONExtractString(properties, '$device_id')") }} AS device_id_hash,

        -- ── user identity type ───────────────────────────────────────────
        -- distinct_id starting with '$device:' means anonymous (not wallet-identified)
        if(NOT startsWith(distinct_id, '$device:'), 1, 0)                  AS is_identified,

        -- ── page / domain ────────────────────────────────────────────────
        COALESCE(
            nullIf(JSONExtractString(properties, 'current_url_path'), ''),
            replaceRegexpOne(
                JSONExtractString(properties, '$current_url'),
                '^https?://[^/]+(/[^?#]*)?.*',
                '\\1'
            )
        )                                                                   AS page_path,

        COALESCE(
            nullIf(JSONExtractString(properties, 'current_domain'), ''),
            replaceRegexpOne(
                JSONExtractString(properties, '$current_url'),
                '^https?://([^/]+).*',
                '\\1'
            )
        )                                                                   AS current_domain,

        JSONExtractString(properties, 'current_page_title')                 AS page_title,

        -- ── production flag ──────────────────────────────────────────────
        -- app.gnosis.io = ~89% of traffic; rest is deploy previews / localhost
        if(
            COALESCE(
                nullIf(JSONExtractString(properties, 'current_domain'), ''),
                replaceRegexpOne(
                    JSONExtractString(properties, '$current_url'),
                    '^https?://([^/]+).*',
                    '\\1'
                )
            ) = 'app.gnosis.io', 1, 0
        )                                                                   AS is_production,

        -- ── traffic sources ──────────────────────────────────────────────
        multiIf(
            JSONExtractString(properties, '$referring_domain') IN ('', '$direct'), 'direct',
            JSONExtractString(properties, '$referring_domain')
        )                                                                   AS referrer_domain,

        multiIf(
            JSONExtractString(properties, '$initial_referring_domain') IN ('', '$direct'), 'direct',
            JSONExtractString(properties, '$initial_referring_domain')
        )                                                                   AS initial_referrer_domain,

        -- ── geography (no city – privacy) ────────────────────────────────
        JSONExtractString(properties, 'mp_country_code')                    AS country_code,
        JSONExtractString(properties, '$region')                            AS region,

        -- ── technology ───────────────────────────────────────────────────
        JSONExtractString(properties, '$browser')                           AS browser,
        JSONExtractString(properties, '$browser_version')                   AS browser_version,
        JSONExtractString(properties, '$os')                                AS os,
        multiIf(
            JSONExtractString(properties, '$device') != '', JSONExtractString(properties, '$device'),
            'Desktop'
        )                                                                   AS device_type,
        toUInt16OrZero(JSONExtractString(properties, '$screen_width'))      AS screen_width,
        toUInt16OrZero(JSONExtractString(properties, '$screen_height'))     AS screen_height,

        -- ── SDK metadata ─────────────────────────────────────────────────
        JSONExtractString(properties, '$lib_version')                       AS lib_version,
        JSONExtractString(properties, 'mp_lib')                             AS mp_lib,

        -- ── flags ────────────────────────────────────────────────────────
        if(
            JSONExtractString(properties, '$mp_autocapture') IN ('true', '1'),
            1, 0
        )                                                                   AS is_autocapture,

        -- ── custom properties ────────────────────────────────────────────
        JSONExtractString(properties, 'bottomSheet')                        AS bottom_sheet,

        -- ── custom event properties (feature-specific) ───────────────────
        JSONExtractString(properties, 'amount')                             AS event_amount,
        JSONExtractString(properties, 'sku')                                AS event_sku,
        JSONExtractString(properties, 'seller')                             AS event_seller,
        JSONExtractString(properties, 'value')                              AS event_value,
        JSONExtractString(properties, 'assetId')                            AS event_asset_id

    FROM {{ source('mixpanel_ga', 'mixpanel_raw_events') }}
    WHERE toDate(event_time) < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(toDate(event_time)) >= toDate('{{ start_month }}')
        AND toStartOfMonth(toDate(event_time)) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('event_time', 'event_date', true) }}
      {% endif %}
)

SELECT
    event_time,
    event_date,
    hour_of_day,
    day_of_week,
    event_name,
    project_id,
    insert_id,
    user_id_hash,
    device_id_hash,
    is_identified,
    page_path,
    current_domain,
    page_title,
    is_production,
    referrer_domain,
    initial_referrer_domain,
    country_code,
    region,
    browser,
    browser_version,
    os,
    device_type,
    screen_width,
    screen_height,
    lib_version,
    mp_lib,
    is_autocapture,
    bottom_sheet,
    event_amount,
    event_sku,
    event_seller,
    event_value,
    event_asset_id,

    -- event_category is computed here (outer SELECT) so it can reference
    -- is_autocapture without a CTE self-reference.
    -- To change classifications, edit macros/mixpanel_ga_event_category.sql.
    {{ mixpanel_ga_event_category() }}                                      AS event_category

FROM raw
