




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
        
    sipHash64(concat(unhex('00'), lower(distinct_id)))
                                AS user_id_hash,
        
    sipHash64(concat(unhex('00'), lower(JSONExtractString(properties, '$device_id'))))
 AS device_id_hash,

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

    FROM `mixpanel_ga`.`mixpanel_raw_events`
    WHERE toDate(event_time) < today()
      
        
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(event_time)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.event_date)), -0))
        FROM `dbt`.`stg_mixpanel_ga__events` AS x1
        WHERE 1=1 
      )
      AND toDate(event_time) >= (
        SELECT
          
            addDays(max(toDate(x2.event_date)), -0)
          

        FROM `dbt`.`stg_mixpanel_ga__events` AS x2
        WHERE 1=1 
      )
    
  

      
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
    
multiIf(
    event_name = '$mp_web_page_view',                                            'pageview',
    event_name IN ('Open Modal', 'Open Bottom Sheet'),                           'modal',
    event_name = 'Login with Passkey',                                           'login',
    event_name IN (
        'Success - Circles mint',
        'Marketplace Purchase',
        'Request QR Scanner',
        'Close QR Scanner',
        'QR Scan',
        'QR Scan Passkey Validation URL',
        'QR Scan Transaction URL'
    ),                                                                                 'feature',
    event_name IN ('back', 'Back', 'Close'),                                     'navigation',
    startsWith(event_name, '$'),                                                 'system',
    is_autocapture = 1,                                                          'action',
    'other'
)
                                      AS event_category

FROM raw