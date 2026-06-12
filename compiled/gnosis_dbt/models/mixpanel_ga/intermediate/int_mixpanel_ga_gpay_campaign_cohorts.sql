

-- INTERNAL ONLY — campaign cohort assignment for Gnosis Pay accounts: one row
-- per account (initial_owner pseudonym) with its acquisition UTM (first-touch,
-- the canonical cohort attribution) and cohort_week = the week the account was
-- first funded. Feeds the per-campaign retention / engagement / funnel marts,
-- which aggregate the pseudonym away before exposure.
-- Carries user_pseudonym → never exposed to cerebro-api or MCP.

SELECT
    user_pseudonym,
    toStartOfWeek(first_date, 1)  AS cohort_week,
    first_touch_campaign          AS utm_campaign,
    first_touch_source            AS utm_source,
    first_touch_medium            AS utm_medium
FROM `dbt`.`int_mixpanel_ga_gpay_first_events`
WHERE event_type = 'funded'