

-- For the selected Circles v2 avatar's PERSONAL CRC token (the
-- ERC-1155 personal token whose token_address equals the avatar
-- address), classify every current holder into one of:
--
--   * Self                  - the avatar itself holds its own CRC
--   * Wrapped (ERC-20)      - held by an ERC20Lift wrapper contract
--                             registered for this avatar
--   * Other Circles avatars - any other registered Circles avatar
--   * Other contracts       - anything else (DEX pools, custodians, ...)
--
-- Output is one row per (avatar, holder_category) so the dashboard
-- can render a per-avatar pie/bar grouped by category. The dashboard's
-- global filter (avatar) selects the token whose distribution we are
-- inspecting; on this row `avatar` IS the token holder we are studying.
--
-- Backs the "Token Distribution" panel on the Circles Avatar tab.

WITH latest AS (
    SELECT max(date) AS d
    FROM `dbt`.`int_execution_circles_v2_balances_daily`
    WHERE date < today()
),
wrappers AS (
    SELECT
        lower(wrapper_address) AS wrapper_address,
        lower(avatar)          AS underlying_avatar
    FROM `dbt`.`int_execution_circles_v2_wrappers`
),
classified AS (
    SELECT
        b.token_address                                                AS avatar,
        b.account                                                      AS account,
        toFloat64(b.balance_raw)            / pow(10, 18)              AS balance,
        toFloat64(b.demurraged_balance_raw) / pow(10, 18)              AS balance_demurraged,
        CASE
            WHEN b.account = b.token_address                THEN 'Self'
            WHEN w.wrapper_address IS NOT NULL              THEN 'Wrapped (ERC-20)'
            WHEN av.avatar IS NOT NULL                      THEN 'Other Circles avatars'
            ELSE                                                 'Other contracts'
        END                                                            AS holder_category
    FROM `dbt`.`int_execution_circles_v2_balances_daily` b
    CROSS JOIN latest
    LEFT JOIN wrappers w
        ON w.wrapper_address  = b.account
       AND w.underlying_avatar = b.token_address
    LEFT JOIN `dbt`.`int_execution_circles_v2_avatars` av
        ON av.avatar = b.account
    WHERE b.date = latest.d
      AND b.balance_raw > pow(10, 15)
)
SELECT
    avatar                            AS avatar,
    holder_category                   AS holder_category,
    uniqExact(account)                AS holder_count,
    sum(balance)                      AS balance,
    sum(balance_demurraged)           AS balance_demurraged
FROM classified
GROUP BY avatar, holder_category