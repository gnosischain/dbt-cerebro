

-- Weekly progression of Gnosis Pay first-funded accounts and first card
-- transactions, broken down by Mixpanel UTM campaign.
--
-- Tidy/long shape: one model serves both metrics (event_type) and both
-- attribution windows (attribution_model = first_touch | last_touch). The
-- growth team filters attribution_model at query time.
--
-- Aggregate-only (campaign-level counts; no user_pseudonym / hash), so this
-- table backs the MCP-exposed api_ view. cerebro-api exclusion is inherited
-- from the models.gnosis_dbt.mixpanel_ga block in dbt_project.yml.

WITH unpivoted AS (
    SELECT
        event_type,
        first_date,
        'first_touch'        AS attribution_model,
        first_touch_campaign AS utm_campaign
    FROM `dbt`.`int_mixpanel_ga_gpay_first_events`

    UNION ALL

    SELECT
        event_type,
        first_date,
        'last_touch'        AS attribution_model,
        last_touch_campaign AS utm_campaign
    FROM `dbt`.`int_mixpanel_ga_gpay_first_events`
),

weekly AS (
    SELECT
        toStartOfWeek(first_date, 1) AS week,
        event_type,
        attribution_model,
        utm_campaign,
        count()                      AS new_accounts
    FROM unpivoted
    WHERE toStartOfWeek(first_date, 1) < toStartOfWeek(today(), 1)
    GROUP BY week, event_type, attribution_model, utm_campaign
)

SELECT
    week,
    event_type,
    attribution_model,
    utm_campaign,
    new_accounts,
    sum(new_accounts) OVER (
        PARTITION BY event_type, attribution_model, utm_campaign
        ORDER BY week
    ) AS cumulative_accounts
FROM weekly
ORDER BY week, event_type, attribution_model, utm_campaign