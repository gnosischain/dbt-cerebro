{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(user_id_hash)',
    tags=['production', 'mixpanel_ga', 'internal_only', 'privacy:tier_internal'],
    meta={'expose_to_mcp': False, 'privacy_tier': 'internal'}
  )
}}

-- INTERNAL ONLY — canonical per-Gnosis-App-account attribute dimension built from
-- the Mixpanel profile snapshot. One row per user_id_hash. The enrichment
-- dimension for segmenting acquisition/campaign metrics by product adoption and
-- geo. JSON is parsed per row in the `parsed` CTE, then each column is picked
-- from the newest snapshot per distinct_id via argMax(col, synced_at) (parsing
-- before aggregation avoids ClickHouse ILLEGAL_AGGREGATION).
--
-- Profile properties are CURRENT-STATE (no history): the boolean flags say "is
-- currently a backer / has IBAN / installed PWA", not when. Carries user_id_hash
-- + pay_safe_pseudonym (hashes) — never exposed to cerebro-api or MCP. `username`
-- is intentionally not materialized.

WITH parsed AS (
    SELECT
        distinct_id,
        synced_at AS ver,
        if(
            startsWith(lower(JSONExtractString(properties, 'pay')), '0x'),
            {{ pseudonymize_address("JSONExtractString(properties, 'pay')") }},
            0
        )                                                                             AS pay_safe_pseudonym,
        nullIf(splitByChar('?', JSONExtractString(properties, 'initial_utm_campaign'))[1], '') AS first_touch_campaign,
        nullIf(JSONExtractString(properties, 'initial_utm_source'), '')                AS first_touch_source,
        nullIf(JSONExtractString(properties, 'initial_utm_medium'), '')                AS first_touch_medium,
        nullIf(JSONExtractString(properties, 'initial_utm_content'), '')               AS first_touch_content,
        nullIf(JSONExtractString(properties, 'initial_utm_id'), '')                    AS first_touch_utm_id,
        nullIf(JSONExtractString(properties, 'initial_utm_source_platform'), '')       AS first_touch_source_platform,
        nullIf(JSONExtractString(properties, 'initial_utm_creative_format'), '')       AS first_touch_creative_format,
        nullIf(JSONExtractString(properties, 'initial_utm_marketing_tactic'), '')      AS first_touch_marketing_tactic,
        JSONExtractString(properties, '$country_code')                                 AS country_code,
        JSONExtractString(properties, '$region')                                       AS region,
        JSONExtractBool(properties, 'pwa')                                             AS is_pwa,
        JSONExtractBool(properties, 'iban')                                            AS has_iban,
        JSONExtractBool(properties, 'gNFT')                                            AS has_gnft,
        JSONExtractBool(properties, 'backer')                                          AS is_backer,
        JSONExtractBool(properties, 'referral')                                        AS joined_via_referral
    FROM {{ source('mixpanel_ga', 'mixpanel_raw_profiles') }}
)

SELECT
    {{ pseudonymize_address('distinct_id') }}              AS user_id_hash,
    argMax(pay_safe_pseudonym, ver)                       AS pay_safe_pseudonym,
    argMax(first_touch_campaign, ver)                     AS first_touch_campaign,
    argMax(first_touch_source, ver)                       AS first_touch_source,
    argMax(first_touch_medium, ver)                       AS first_touch_medium,
    argMax(first_touch_content, ver)                      AS first_touch_content,
    argMax(first_touch_utm_id, ver)                       AS first_touch_utm_id,
    argMax(first_touch_source_platform, ver)              AS first_touch_source_platform,
    argMax(first_touch_creative_format, ver)              AS first_touch_creative_format,
    argMax(first_touch_marketing_tactic, ver)             AS first_touch_marketing_tactic,
    argMax(country_code, ver)                             AS country_code,
    argMax(region, ver)                                  AS region,
    argMax(is_pwa, ver)                                  AS is_pwa,
    argMax(has_iban, ver)                                AS has_iban,
    argMax(has_gnft, ver)                                AS has_gnft,
    argMax(is_backer, ver)                               AS is_backer,
    argMax(joined_via_referral, ver)                     AS joined_via_referral,
    max(ver)                                             AS synced_at
FROM parsed
GROUP BY distinct_id
