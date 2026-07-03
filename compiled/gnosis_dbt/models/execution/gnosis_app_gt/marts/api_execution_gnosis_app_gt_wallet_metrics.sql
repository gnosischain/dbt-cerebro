

-- Public per-wallet metric endpoint (pseudonym-only). Curated lifecycle /
-- engagement / trust / segment surface over the public rollup.
SELECT
    user_pseudonym,
    app_generation,
    engagement_tier,
    is_registered_active,
    is_active_30d,
    is_active_90d,
    tenure_days,
    days_since_last_action,
    n_actions,
    app_action_breadth,
    n_swaps_gnosis_app,
    n_swaps_metri,
    n_metri_transfer,
    n_personal_mint,
    n_cashback,
    n_investment,
    n_invitees,
    trusts_given,
    trusts_received,
    trusts_mutual,
    referral_crc_earned,
    earned_from_invites_crc,
    is_gp_card_user,
    is_investor,
    is_power_user,
    circles_version,
    today() AS as_of_date
FROM `dbt`.`fct_execution_gnosis_app_gt_wallet_metrics_public`