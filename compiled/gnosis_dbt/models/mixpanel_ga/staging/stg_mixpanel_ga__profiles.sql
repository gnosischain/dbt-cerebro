

-- One row per Mixpanel People/profile (latest snapshot), privacy-safe.
-- The upstream source is a daily full snapshot; each column is picked from the
-- newest row per distinct_id via argMax(col, synced_at). JSON is parsed per row
-- in the `parsed` CTE BEFORE the aggregation so no aggregate is nested inside
-- another (ClickHouse ILLEGAL_AGGREGATION otherwise).
--
-- `pay` is the profile's Gnosis Pay Safe address (People property set by the
-- app via people.set), pseudonymized with the SAME macro used on the on-chain
-- gp_safe, so pay_safe_pseudonym == pseudonymize_address(gp_safe) for the same
-- Safe and the two join without any raw address leaving the model.
-- pay_safe_pseudonym = 0 means the profile has no Gnosis Pay Safe set.
-- initial_utm_* are Mixpanel's set-once first-touch values (present on profiles
-- even though absent from the event stream).

WITH parsed AS (
    SELECT
        distinct_id,
        synced_at AS ver,
        if(
            startsWith(lower(JSONExtractString(properties, 'pay')), '0x'),
            
    sipHash64(concat(unhex('00'), lower(JSONExtractString(properties, 'pay'))))
,
            0
        )                                                     AS pay_safe_pseudonym,
        JSONExtractString(properties, 'initial_utm_campaign') AS initial_utm_campaign,
        JSONExtractString(properties, 'initial_utm_source')   AS initial_utm_source,
        JSONExtractString(properties, 'initial_utm_medium')   AS initial_utm_medium
    FROM `mixpanel_ga`.`mixpanel_raw_profiles`
)

SELECT
    
    sipHash64(concat(unhex('00'), lower(distinct_id)))
    AS ga_user_id_hash,
    argMax(pay_safe_pseudonym, ver)              AS pay_safe_pseudonym,
    argMax(initial_utm_campaign, ver)            AS initial_utm_campaign,
    argMax(initial_utm_source, ver)              AS initial_utm_source,
    argMax(initial_utm_medium, ver)              AS initial_utm_medium,
    max(ver)                                     AS synced_at
FROM parsed
GROUP BY distinct_id