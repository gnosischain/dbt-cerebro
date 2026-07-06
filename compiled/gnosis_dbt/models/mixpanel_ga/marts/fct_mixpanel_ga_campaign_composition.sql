

-- Campaign COMPOSITION (state-based, not weekly) — "who did each first-touch
-- campaign bring in." One row per utm_campaign x source x medium, with the
-- current product-adoption profile of that campaign's users and how many went on
-- to fund a Gnosis Pay card (via the profile `pay` bridge). Complements the
-- weekly funnel (fct_mixpanel_ga_campaign_funnel_weekly): the funnel gives timed
-- conversions, this gives current cohort make-up. Aggregate-only (no pseudonyms).
--
-- Profile flags are current-state, so these are "is currently" rates, not timed.
-- K-ANONYMITY: campaigns with < 5 users are bucketed into '_small_campaigns'.
-- Campaign views are only meaningful for the Mixpanel era; profiles exist 2025-10+.

WITH prof AS (
    SELECT
        user_id_hash,
        pay_safe_pseudonym,
        coalesce(first_touch_campaign, 'unknown') AS utm_campaign_raw,
        coalesce(first_touch_source,   'unknown') AS utm_source,
        coalesce(first_touch_medium,   'unknown') AS utm_medium,
        is_pwa, has_iban, has_gnft, is_backer, joined_via_referral
    FROM `dbt`.`int_mixpanel_ga_user_profile`
),

funded_safes AS (
    SELECT DISTINCT 
    sipHash64(concat(unhex('00'), lower(gp_safe)))
 AS pay_safe_pseudonym
    FROM `dbt`.`int_execution_gpay_conversions`
    WHERE conversion_kind = 'gpay_funded'
      AND identity_role   = 'initial_owner'
),

campaign_sizes AS (
    SELECT utm_campaign_raw, count() AS n
    FROM prof
    GROUP BY utm_campaign_raw
),

joined AS (
    SELECT
        if(cs.n < 5, '_small_campaigns', p.utm_campaign_raw) AS utm_campaign,
        p.utm_source,
        p.utm_medium,
        p.is_pwa, p.has_iban, p.has_gnft, p.is_backer, p.joined_via_referral,
        if(p.pay_safe_pseudonym != 0 AND fs.pay_safe_pseudonym != 0, 1, 0) AS is_funded_card
    FROM prof p
    LEFT JOIN campaign_sizes cs ON cs.utm_campaign_raw = p.utm_campaign_raw
    LEFT JOIN funded_safes  fs ON fs.pay_safe_pseudonym = p.pay_safe_pseudonym
)

SELECT
    utm_campaign,
    utm_source,
    utm_medium,
    count()                    AS users,
    sum(is_funded_card)        AS funded_card_users,
    sum(is_pwa)                AS pwa_users,
    sum(has_iban)              AS iban_users,
    sum(has_gnft)              AS gnft_users,
    sum(is_backer)             AS backer_users,
    sum(joined_via_referral)   AS referral_users
FROM joined
GROUP BY utm_campaign, utm_source, utm_medium