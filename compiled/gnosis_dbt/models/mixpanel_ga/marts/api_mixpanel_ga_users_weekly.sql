

-- Aggregate-only (weekly unique counts, no pseudonyms) — unlike its per-user
-- namesake api_mixpanel_ga_users_daily, this IS safe for MCP / semantic-layer
-- exposure. cerebro-api exposure is blanket-excluded for all
-- models/mixpanel_ga/ via dbt_project.yml. WAU and visitor counts are weekly
-- distincts — do NOT sum across weeks.

SELECT
    week,
    weekly_visitors,
    wau,
    welcome_visitors,
    new_users,
    new_identified_users,
    total_events
FROM `dbt`.`fct_mixpanel_ga_users_weekly`
ORDER BY week