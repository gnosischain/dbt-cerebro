

-- Aggregate-only view (campaign-level counts; no user_id_hash) over the
-- weekly client-side conversions by UTM. cerebro-api exposure is
-- blanket-excluded for all models/mixpanel_ga/ via dbt_project.yml
-- (no api:* tag here) — consumed internally / via MCP-safe aggregates.

SELECT
    week,
    metric,
    attribution_model,
    utm_campaign,
    utm_source,
    utm_medium,
    new_users,
    cumulative_users
FROM `dbt`.`fct_mixpanel_ga_client_conversions_weekly`
ORDER BY week, metric, attribution_model, utm_campaign, utm_source, utm_medium