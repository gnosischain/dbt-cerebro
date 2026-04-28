{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(date)',
    tags=['production', 'mixpanel_ga', 'gpay']
  )
}}

WITH

-- Per-user identity bridge: which gp_safe(s) does each Mixpanel user
-- match, and via which role(s)? Read straight from the per-user fact so
-- we don't recompute the JOIN here.
matched_pairs AS (
    SELECT
        user_id_hash,
        gp_safe,
        matched_roles
    FROM {{ ref('fct_mixpanel_ga_gpay_users') }}
),

-- Same gp_safe → activity flags. Computed once, joined into both rollups.
delay_active_safes_7d AS (
    SELECT DISTINCT gp_safe
    FROM {{ ref('int_execution_gpay_delay_activity_daily') }}
    WHERE date >= today() - 7
      AND tx_added_count > 0
),

allowance_changed_safes_30d AS (
    SELECT DISTINCT
        rm.gp_safe AS gp_safe
    FROM {{ ref('int_execution_gpay_roles_events') }} re
    INNER JOIN {{ ref('int_execution_gpay_safe_modules') }} rm
        ON rm.module_proxy_address = re.roles_module_address
       AND rm.contract_type = 'RolesModule'
    WHERE re.event_name = 'SetAllowance'
      AND re.block_timestamp >= toDateTime(today() - 30)
),

-- One row per Mixpanel user with rolled-up role flags + activity flags.
mp_user_flags AS (
    SELECT
        mp.user_id_hash,
        max(has(mp.matched_roles, 'initial_owner')) AS via_initial_owner,
        max(has(mp.matched_roles, 'delegate'))      AS via_delegate,
        max(has(mp.matched_roles, 'safe_self'))     AS via_safe_self,
        max(if(da.gp_safe IS NOT NULL, 1, 0))       AS has_delay_activity_7d,
        max(if(ac.gp_safe IS NOT NULL, 1, 0))       AS has_allowance_change_30d
    FROM matched_pairs mp
    LEFT JOIN delay_active_safes_7d        da ON da.gp_safe = mp.gp_safe
    LEFT JOIN allowance_changed_safes_30d  ac ON ac.gp_safe = mp.gp_safe
    GROUP BY mp.user_id_hash
),

mp_daily AS (
    SELECT
        event_date AS date,
        uniqExact(user_id_hash) AS mp_dau,
        -- Per-day matched-user counts, sliced by role bucket and activity
        uniqExactIf(user_id_hash, user_id_hash IN (SELECT user_id_hash FROM mp_user_flags))
            AS matched_users_any,
        uniqExactIf(user_id_hash, user_id_hash IN (SELECT user_id_hash FROM mp_user_flags WHERE via_initial_owner = 1))
            AS matched_by_initial_owner_users,
        uniqExactIf(user_id_hash, user_id_hash IN (SELECT user_id_hash FROM mp_user_flags WHERE via_delegate = 1))
            AS matched_by_delegate_users,
        uniqExactIf(user_id_hash, user_id_hash IN (SELECT user_id_hash FROM mp_user_flags WHERE via_safe_self = 1))
            AS matched_by_safe_self_users,
        uniqExactIf(user_id_hash, user_id_hash IN (SELECT user_id_hash FROM mp_user_flags WHERE has_delay_activity_7d = 1))
            AS users_with_delay_activity_7d,
        uniqExactIf(user_id_hash, user_id_hash IN (SELECT user_id_hash FROM mp_user_flags WHERE has_allowance_change_30d = 1))
            AS users_with_allowance_changes_30d
    FROM {{ ref('stg_mixpanel_ga__events') }}
    WHERE is_production = 1
    GROUP BY date
),

gpay_daily AS (
    SELECT date, active_users AS onchain_active_users
    FROM {{ ref('fct_execution_gpay_activity_daily') }}
)

-- Explicit AS aliases on the mp.* columns are required. Both mp_daily
-- and gpay_daily expose a `date` column, so ClickHouse keeps the
-- qualified name `mp.date` in the output projection, and the config's
-- order_by=(date) then fails to resolve. The non-ambiguous columns get
-- explicit aliases too as a defensive consistency pass.
SELECT
    mp.date                                                       AS date,
    mp.mp_dau                                                     AS mp_dau,
    COALESCE(gp.onchain_active_users, 0)                          AS onchain_active_users,
    mp.matched_users_any                                          AS matched_users,        -- backward-compat alias
    mp.matched_users_any                                          AS matched_users_any,
    mp.matched_by_initial_owner_users                             AS matched_by_initial_owner_users,
    mp.matched_by_delegate_users                                  AS matched_by_delegate_users,
    mp.matched_by_safe_self_users                                 AS matched_by_safe_self_users,
    mp.users_with_delay_activity_7d                               AS users_with_delay_activity_7d,
    mp.users_with_allowance_changes_30d                           AS users_with_allowance_changes_30d,
    round(100.0 * mp.matched_users_any / greatest(mp.mp_dau, 1), 2) AS match_rate_pct
FROM mp_daily mp
LEFT JOIN gpay_daily gp ON mp.date = gp.date
ORDER BY mp.date
