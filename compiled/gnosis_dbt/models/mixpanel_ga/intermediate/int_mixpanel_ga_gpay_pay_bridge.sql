

-- INTERNAL ONLY. Authoritative Safe -> Gnosis App account bridge, sourced from
-- the Mixpanel profile `pay` property. This is the app's own record of which GA
-- account controls a given Gnosis Pay Safe, so it recovers app users that the
-- on-chain Delay-module bridge (int_execution_gnosis_app_gt_card_owner) misses.
--
-- One GA account per Safe: prefer the profile that carries a real first-touch
-- campaign, then the freshest snapshot. The profile's own initial_utm_* are
-- carried through so downstream can use them as a campaign fallback.
-- Keyed on pay_safe_pseudonym == pseudonymize_address(gp_safe).

SELECT
    pay_safe_pseudonym,
    argMax(ga_user_id_hash,      (initial_utm_campaign != '', synced_at)) AS ga_user_id_hash,
    argMax(initial_utm_campaign, (initial_utm_campaign != '', synced_at)) AS profile_initial_utm_campaign,
    argMax(initial_utm_source,   (initial_utm_campaign != '', synced_at)) AS profile_initial_utm_source,
    argMax(initial_utm_medium,   (initial_utm_campaign != '', synced_at)) AS profile_initial_utm_medium
FROM `dbt`.`stg_mixpanel_ga__profiles`
WHERE pay_safe_pseudonym != 0
GROUP BY pay_safe_pseudonym