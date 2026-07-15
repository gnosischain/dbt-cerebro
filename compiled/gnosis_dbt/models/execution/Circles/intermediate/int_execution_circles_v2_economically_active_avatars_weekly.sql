

-- Circles-first definition of ECONOMICALLY ACTIVE avatars, ecosystem-wide.
--
-- An avatar is economically active in a week if it earned >= 1 unit of a
-- Circles reward that week, via either stream:
--   * gcrc_cashback — >= 1 gCRC received from the Circles cashback wallet
--                     (int_execution_circles_v2_gcrc_cashback_recipients_weekly)
--   * inviter_fee   — >= 1 CRC of invitation fees received
--                     (int_execution_circles_v2_inviter_fees aggregated weekly)
--
-- This layer deliberately carries NO app filter: avatars earning through any
-- app (or directly on-chain) count, which is the right lens for the Circles
-- ecosystem (e.g. Circles Garage builders). The Gnosis App WEAU is derived
-- DOWNSTREAM by filtering this layer to in-app users/actions
-- (int_execution_gnosis_app_weekly_earners).
--
-- any_in_app_tx: 1 if this earning is attributable to the Gnosis App, using
-- the SAME rule as the app-side earner layer (int_execution_gnosis_app_weekly_earners),
-- so the downstream avatars_in_app_tx line is CONSISTENT with the Gnosis App WEAU:
--   * gcrc_cashback → the recipient is a current Gnosis App user (membership);
--   * inviter_fee   → the inviter is a Gnosis App user AND >= 1 fee that week
--                     came through an active Gnosis App relayer tx (is_gnosis_app_tx).
-- avatars_in_app_tx (fct) therefore == the WEAU earner population; the Gnosis
-- App WEAU narrows that further to app-active users (WAU ∩ earners). Membership
-- comes from int_execution_gnosis_app_users_current.

WITH ga_users AS (
    SELECT address FROM `dbt`.`int_execution_gnosis_app_users_current`
),

cashback AS (
    SELECT
        week,
        address                 AS avatar,
        'gcrc_cashback'         AS earning_kind,
        amount,
        toUInt8(address IN (SELECT address FROM ga_users)) AS any_in_app_tx
    FROM `dbt`.`int_execution_circles_v2_gcrc_cashback_recipients_weekly`
),

inviter_fees AS (
    SELECT
        toStartOfWeek(block_timestamp, 1)  AS week,
        inviter                            AS avatar,
        'inviter_fee'                      AS earning_kind,
        sum(amount)                        AS amount,
        toUInt8(max(is_gnosis_app_tx) = 1 AND inviter IN (SELECT address FROM ga_users)) AS any_in_app_tx
    FROM `dbt`.`int_execution_circles_v2_inviter_fees`
    WHERE block_timestamp < today()
    GROUP BY week, inviter
    HAVING amount >= 1
)

SELECT week, avatar, earning_kind, amount, any_in_app_tx
FROM (
    SELECT * FROM cashback
    UNION ALL
    SELECT * FROM inviter_fees
)
WHERE avatar != ''
  AND week < toStartOfWeek(today(), 1)