

-- Aggregate-only projection of fct_mixpanel_ga_gpay_acquisition_weekly.
-- Campaign-level counts only — no user_pseudonym / user_id_hash — so it is
-- safe for the MCP / semantic layer. cerebro-api exposure is blanket-excluded
-- for all models/mixpanel_ga/ via dbt_project.yml (no api:* tag here).

SELECT
    week,
    event_type,
    attribution_model,
    utm_campaign,
    utm_source,
    utm_medium,
    new_accounts,
    cumulative_accounts
FROM `dbt`.`fct_mixpanel_ga_gpay_acquisition_weekly`
ORDER BY week, event_type, attribution_model, utm_campaign, utm_source, utm_medium