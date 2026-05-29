

-- INTERNAL ONLY — one row per Gnosis App account per first-conversion type,
-- with the account's Mixpanel UTM attribution attached.
--
-- Unlike the GP side (which barely overlaps Mixpanel), Gnosis App conversions
-- match the Mixpanel identified-user set 70-83% — because both describe the
-- same app.gnosis.io population, keyed on the same on-chain identity bridge.
--
-- conversion_kind ∈ {topup, swap_filled, token_offer_claim, marketplace_buy}.
-- 'topup' is a GP card funding initiated from inside the app — the closest
-- UTM-attributable "first funded" signal.
--
-- UTM attached via user_pseudonym == user_id_hash. Carries user_pseudonym →
-- never exposed to cerebro-api or MCP.

WITH first_conv AS (
    SELECT
        user_pseudonym,
        conversion_kind,
        min(conversion_ts) AS first_ts
    FROM `dbt`.`int_execution_gnosis_app_conversions`
    GROUP BY user_pseudonym, conversion_kind
)

SELECT
    f.conversion_kind                             AS conversion_kind,
    toDate(f.first_ts)                            AS first_date,
    f.user_pseudonym                              AS user_pseudonym,
    coalesce(a.first_touch_campaign, 'unknown')   AS first_touch_campaign,
    coalesce(a.last_touch_campaign,  'unknown')   AS last_touch_campaign
FROM first_conv f
LEFT JOIN `dbt`.`int_mixpanel_ga_user_acquisition` a
    ON a.user_id_hash = f.user_pseudonym