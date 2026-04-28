{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(date)',
    tags=['production','mixpanel_ga','gnosis_app']
  )
}}

WITH heuristic_events AS (
    SELECT
        address,
        toDate(block_timestamp) AS event_date,
        heuristic_kind
    FROM {{ ref('int_execution_gnosis_app_user_events') }}
),

first_seen AS (
    SELECT address, min(event_date) AS first_seen_date
    FROM heuristic_events
    GROUP BY address
),

daily_new AS (
    SELECT
        first_seen_date AS date,
        count() AS new_users
    FROM first_seen
    GROUP BY first_seen_date
),

daily_by_kind AS (
    SELECT
        event_date AS date,
        heuristic_kind,
        uniqExact(address) AS addresses_hit
    FROM heuristic_events
    GROUP BY date, heuristic_kind
),

daily_kinds_pivot AS (
    SELECT
        date,
        sumIf(addresses_hit, heuristic_kind = 'safe_invitation_module') AS h_safe_invitation,
        sumIf(addresses_hit, heuristic_kind = 'circles_metri_fee')      AS h_metri_fee,
        sumIf(addresses_hit, heuristic_kind = 'circles_register_human') AS h_register_human,
        sumIf(addresses_hit, heuristic_kind = 'circles_invite_human')   AS h_invite_human,
        sumIf(addresses_hit, heuristic_kind = 'circles_trust')          AS h_trust,
        sumIf(addresses_hit, heuristic_kind = 'circles_profile_update') AS h_profile_update
    FROM daily_by_kind
    GROUP BY date
),

mp_matched_first_seen AS (
    SELECT toDate(first_seen_at) AS first_seen_date, count() AS new_matched
    FROM {{ ref('fct_mixpanel_ga_gnosis_app_users') }}
    WHERE matched_mp = 1
    GROUP BY first_seen_date
)

-- Explicit AS aliases on the d.* columns are required. Both daily_new
-- and daily_kinds_pivot expose a `date` column, so ClickHouse keeps the
-- qualified name `d.date` in the output projection and the config's
-- order_by=(date) fails to resolve. Same class of aliasing quirk as
-- fct_mixpanel_ga_gpay_crossdomain_daily.
SELECT
    d.date                             AS date,
    d.new_users                        AS new_users,
    sum(d.new_users)        OVER (ORDER BY d.date) AS cumulative_users,
    sum(coalesce(m.new_matched, 0)) OVER (ORDER BY d.date) AS cumulative_mp_matched,
    coalesce(k.h_safe_invitation, 0)   AS h_safe_invitation,
    coalesce(k.h_metri_fee, 0)         AS h_metri_fee,
    coalesce(k.h_register_human, 0)    AS h_register_human,
    coalesce(k.h_invite_human, 0)      AS h_invite_human,
    coalesce(k.h_trust, 0)             AS h_trust,
    coalesce(k.h_profile_update, 0)    AS h_profile_update
FROM daily_new d
LEFT JOIN daily_kinds_pivot      k ON k.date = d.date
LEFT JOIN mp_matched_first_seen  m ON m.first_seen_date = d.date
ORDER BY d.date
