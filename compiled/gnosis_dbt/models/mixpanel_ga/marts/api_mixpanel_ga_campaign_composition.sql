

-- Aggregate-only (k-anonymized, no pseudonyms). cerebro-api exposure is
-- blanket-excluded for all models/mixpanel_ga/ via dbt_project.yml.
-- Current-state make-up of each first-touch campaign's users + how many funded a
-- Gnosis Pay card. Clip campaign views to the Mixpanel era (profiles 2025-10+).

SELECT
    utm_campaign,
    utm_source,
    utm_medium,
    users,
    funded_card_users,
    pwa_users,
    iban_users,
    gnft_users,
    backer_users,
    referral_users
FROM `dbt`.`fct_mixpanel_ga_campaign_composition`
ORDER BY users DESC, utm_campaign