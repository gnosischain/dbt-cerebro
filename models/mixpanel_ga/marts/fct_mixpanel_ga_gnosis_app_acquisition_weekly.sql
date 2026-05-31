{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(week, conversion_kind, attribution_model, utm_campaign)',
    tags=['production', 'mixpanel_ga', 'gnosis_app']
  )
}}

-- Weekly progression of first Gnosis App conversions, broken down by Mixpanel
-- UTM campaign. Unlike the Gnosis Pay side (whose on-chain identity barely
-- overlaps Mixpanel), App conversions match the Mixpanel identified-user set
-- 70-83% — both describe the same app.gnosis.io population on the same
-- on-chain identity bridge — so the UTM attribution here is meaningful.
--
-- conversion_kind ∈ {topup, swap_filled, token_offer_claim, marketplace_buy}.
-- 'topup' is GP card funding initiated from inside the app — the closest
-- UTM-attributable "first funded" signal on this side.
--
-- Tidy/long shape: one model serves both attribution windows
-- (attribution_model = first_touch | last_touch). The growth team filters
-- attribution_model at query time.
--
-- Aggregate-only (campaign-level counts; no user_pseudonym), so this table
-- backs the MCP-exposed api_ view. cerebro-api exclusion is inherited from the
-- models.gnosis_dbt.mixpanel_ga block in dbt_project.yml.

WITH unpivoted AS (
    SELECT
        conversion_kind,
        first_date,
        'first_touch'        AS attribution_model,
        first_touch_campaign AS utm_campaign
    FROM {{ ref('int_mixpanel_ga_gnosis_app_first_events') }}

    UNION ALL

    SELECT
        conversion_kind,
        first_date,
        'last_touch'        AS attribution_model,
        last_touch_campaign AS utm_campaign
    FROM {{ ref('int_mixpanel_ga_gnosis_app_first_events') }}
),

weekly AS (
    SELECT
        toStartOfWeek(first_date, 1) AS week,
        conversion_kind,
        attribution_model,
        utm_campaign,
        count()                      AS new_accounts
    FROM unpivoted
    WHERE toStartOfWeek(first_date, 1) < toStartOfWeek(today(), 1)
    GROUP BY week, conversion_kind, attribution_model, utm_campaign
)

SELECT
    week,
    conversion_kind,
    attribution_model,
    utm_campaign,
    new_accounts,
    sum(new_accounts) OVER (
        PARTITION BY conversion_kind, attribution_model, utm_campaign
        ORDER BY week
    ) AS cumulative_accounts
FROM weekly
ORDER BY week, conversion_kind, attribution_model, utm_campaign
