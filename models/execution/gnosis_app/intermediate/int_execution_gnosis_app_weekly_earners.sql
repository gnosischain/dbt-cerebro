{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(week, address)',
    settings={'allow_nullable_key': 1},
    tags=['production','execution','gnosis_app','weau','weekly']
  )
}}

-- Weekly "economic earners" set FOR THE GNOSIS APP: addresses that earned
-- >= 1 unit of a Circles reward in a given week, filtered to users and
-- actions taken in-app. Used as the right side of the WAU∩earners
-- intersection that defines Weekly Economically Active Users.
--
-- Consumes the circles-first ecosystem layer
-- (int_execution_circles_v2_economically_active_avatars_weekly) and scopes it:
--   * inviter_fee   → address is a Gnosis App user AND at least one fee that
--                     week came through a Gnosis App relayer tx (any_in_app_tx)
--   * gcrc_cashback → address is a Gnosis App user (cashback payouts carry no
--                     tx-origin, so scoping is membership-based)
--
-- DEFINITION CHANGE (vs the original global version): earnings via other
-- apps/direct on-chain no longer count here — they remain visible in the
-- circles-first ecosystem layer and its fct/api. ga_users membership comes
-- from int_execution_gnosis_app_users_current.

{% set floor_date = var('gnosis_app_wau_floor_date') %}

WITH ga_users AS (
    SELECT address
    FROM {{ ref('int_execution_gnosis_app_users_current') }}
),

scoped AS (
    SELECT
        e.week,
        e.avatar AS address
    FROM {{ ref('int_execution_circles_v2_economically_active_avatars_weekly') }} e
    INNER JOIN ga_users u
        ON u.address = e.avatar
    WHERE e.week >= toDate('{{ floor_date }}')
      AND (e.earning_kind = 'gcrc_cashback'
           OR (e.earning_kind = 'inviter_fee' AND e.any_in_app_tx = 1))
)

SELECT DISTINCT week, address
FROM scoped
WHERE address != ''
