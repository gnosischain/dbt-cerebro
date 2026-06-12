

-- Weekly progression of client-side (Mixpanel-event) conversions by UTM —
-- card_ordered / crc_minted / circles_created etc. per
-- seeds/mixpanel_conversion_events.csv. Complements the on-chain steps
-- (funded / first_payment / starts_referring in
-- fct_mixpanel_ga_gpay_acquisition_weekly + …gnosis_app…) so Growth's full
-- funnel by UTM lives in one place.
--
-- Tidy/long shape: attribution_model = first_touch | last_touch.
-- Aggregate-only (campaign-level counts; no user_id_hash), backing the
-- MCP-exposed api_ view. cerebro-api exclusion inherited from the
-- models.gnosis_dbt.mixpanel_ga block in dbt_project.yml.

WITH unpivoted AS (
    SELECT
        metric,
        first_date,
        'first_touch'        AS attribution_model,
        first_touch_campaign AS utm_campaign,
        first_touch_source   AS utm_source,
        first_touch_medium   AS utm_medium
    FROM `dbt`.`int_mixpanel_ga_client_first_events`

    UNION ALL

    SELECT
        metric,
        first_date,
        'last_touch'        AS attribution_model,
        last_touch_campaign AS utm_campaign,
        last_touch_source   AS utm_source,
        last_touch_medium   AS utm_medium
    FROM `dbt`.`int_mixpanel_ga_client_first_events`
),

weekly AS (
    SELECT
        toStartOfWeek(first_date, 1) AS week,
        metric,
        attribution_model,
        utm_campaign,
        utm_source,
        utm_medium,
        count()                      AS new_users
    FROM unpivoted
    WHERE toStartOfWeek(first_date, 1) < toStartOfWeek(today(), 1)
    GROUP BY week, metric, attribution_model, utm_campaign, utm_source, utm_medium
)

SELECT
    week,
    metric,
    attribution_model,
    utm_campaign,
    utm_source,
    utm_medium,
    new_users,
    sum(new_users) OVER (
        PARTITION BY metric, attribution_model, utm_campaign, utm_source, utm_medium
        ORDER BY week
    ) AS cumulative_users
FROM weekly
ORDER BY week, metric, attribution_model, utm_campaign, utm_source, utm_medium