

-- Week-scoped unique-user rollup for Gnosis App Mixpanel traffic. Weekly
-- uniques CANNOT be derived from the daily marts (summing daily uniqExact
-- counts a user once per active day), so distincts are recomputed at week
-- grain from the event stream. Complete ISO weeks only (current week excluded).
--
--   weekly_visitors        distinct pseudonyms incl. anonymous $device: ids
--   wau                    distinct identified (wallet-linked) users
--   welcome_visitors       distinct users on /welcome (same definition as
--                          fct_mixpanel_ga_funnel_daily)
--   new_users              pseudonyms whose first-ever event fell in the week
--   new_identified_users   users whose first identified event fell in the week
--
-- Anonymous devices and identified users carry DIFFERENT pseudonyms (Mixpanel
-- swaps distinct_id on login), so weekly_visitors slightly overcounts people.
-- Aggregate-only (no user_id_hash), backing the MCP-exposed api_ view.

WITH weekly AS (
    SELECT
        toStartOfWeek(event_date, 1)                        AS week,
        uniqExact(user_id_hash)                             AS weekly_visitors,
        uniqExactIf(user_id_hash, is_identified = 1)        AS wau,
        uniqExactIf(user_id_hash, page_path = '/welcome')   AS welcome_visitors,
        count()                                             AS total_events
    FROM `dbt`.`stg_mixpanel_ga__events`
    WHERE is_production = 1
      AND toStartOfWeek(event_date, 1) < toStartOfWeek(today(), 1)
    GROUP BY week
),

first_seen AS (
    SELECT
        user_id_hash,
        min(date)                       AS first_date,
        minIf(date, is_identified = 1)  AS first_identified_date,
        max(is_identified)              AS ever_identified
    FROM `dbt`.`int_mixpanel_ga_users_daily`
    GROUP BY user_id_hash
),

weekly_new AS (
    SELECT
        toStartOfWeek(first_date, 1) AS week,
        count()                      AS new_users
    FROM first_seen
    GROUP BY week
),

weekly_new_identified AS (
    SELECT
        toStartOfWeek(first_identified_date, 1) AS week,
        count()                                 AS new_identified_users
    FROM first_seen
    WHERE ever_identified = 1
    GROUP BY week
)

SELECT
    w.week                                  AS week,
    w.weekly_visitors                       AS weekly_visitors,
    w.wau                                   AS wau,
    w.welcome_visitors                      AS welcome_visitors,
    n.new_users                             AS new_users,
    ni.new_identified_users                 AS new_identified_users,
    w.total_events                          AS total_events
FROM weekly w
LEFT JOIN weekly_new            n  ON n.week  = w.week
LEFT JOIN weekly_new_identified ni ON ni.week = w.week
ORDER BY week